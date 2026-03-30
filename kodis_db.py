import os
import sqlite3
import subprocess
import sys
import threading
import time
import traceback

from flask import jsonify, render_template

from .setup import *  # pylint: disable=wildcard-import,unused-wildcard-import


class PlexImportMixin(object):
    plex_import_lock = threading.Lock()
    plex_import_status = {
        'running': False,
        'finished': False,
        'stop_requested': False,
        'total': 0,
        'processed': 0,
        'imported': 0,
        'percent': 0,
        'message': '',
        'error': '',
        'started_at': 0,
        'ended_at': 0,
    }

    def _metadata_db_connect(self, timeout=30):
        conn = sqlite3.connect(self._metadata_db_path(), timeout=timeout)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
        except Exception:
            pass
        return conn

    def _metadata_db_path(self):
        db_dir = os.path.join(F.config['path_data'], 'db', P.package_name)
        os.makedirs(db_dir, exist_ok=True)
        return os.path.join(db_dir, 'metadata.sqlite')

    def _plex_import_log_path(self):
        return os.path.join(os.path.dirname(self._metadata_db_path()), 'plex_import_debug.log')

    def _read_plex_import_log_tail(self, max_lines=80, max_chars=12000):
        log_path = self._plex_import_log_path()
        if not os.path.exists(log_path):
            return ''
        try:
            with open(log_path, 'r', encoding='utf-8', errors='replace') as fp:
                lines = fp.readlines()
            text = ''.join(lines[-max_lines:])
            if len(text) > max_chars:
                text = text[-max_chars:]
            return text
        except Exception:
            return ''

    def _get_recent_plex_import_items(self, limit_count=80):
        self._ensure_plex_art_table()
        with self._metadata_db_connect(timeout=0.2) as conn:
            rows = conn.execute(
                '''
                SELECT file_path, series_path, updated_at
                FROM plex_art_item
                ORDER BY updated_at DESC, file_path ASC
                LIMIT ?
                ''',
                (int(limit_count or 80),),
            ).fetchall()
        result = []
        for row in rows:
            file_path = row[0] or ''
            series_path = row[1] or ''
            updated_at = int(row[2] or 0)
            result.append({
                'file_path': file_path,
                'series_path': series_path,
                'updated_at': updated_at,
                'label': file_path or series_path,
            })
        return result

    def _ensure_plex_art_table(self):
        with self._metadata_db_connect() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS plex_art_item (
                    file_path TEXT PRIMARY KEY,
                    series_path TEXT,
                    poster_url TEXT,
                    thumb_url TEXT,
                    updated_at INTEGER NOT NULL DEFAULT 0
                )
                '''
            )
            conn.execute(
                '''
                CREATE INDEX IF NOT EXISTS idx_plex_art_series_path
                ON plex_art_item(series_path)
                '''
            )
            conn.commit()

    def _ensure_plex_import_state_table(self):
        with self._metadata_db_connect() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS plex_import_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    running INTEGER NOT NULL DEFAULT 0,
                    finished INTEGER NOT NULL DEFAULT 0,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    total INTEGER NOT NULL DEFAULT 0,
                    processed INTEGER NOT NULL DEFAULT 0,
                    imported INTEGER NOT NULL DEFAULT 0,
                    percent INTEGER NOT NULL DEFAULT 0,
                    message TEXT NOT NULL DEFAULT '',
                    current_path TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    started_at INTEGER NOT NULL DEFAULT 0,
                    ended_at INTEGER NOT NULL DEFAULT 0
                )
                '''
            )
            columns = [row[1] for row in conn.execute("PRAGMA table_info(plex_import_state)").fetchall()]
            if 'current_path' not in columns:
                conn.execute("ALTER TABLE plex_import_state ADD COLUMN current_path TEXT NOT NULL DEFAULT ''")
            conn.execute(
                '''
                INSERT OR IGNORE INTO plex_import_state (
                    id, running, finished, stop_requested, total, processed, imported,
                    percent, message, current_path, error, started_at, ended_at
                ) VALUES (1, 0, 0, 0, 0, 0, 0, 0, '', '', '', 0, 0)
                '''
            )
            conn.commit()

    def _read_plex_import_state(self):
        try:
            with self._metadata_db_connect(timeout=0.2) as conn:
                row = conn.execute(
                    '''
                    SELECT running, finished, stop_requested, total, processed, imported,
                           percent, message, current_path, error, started_at, ended_at
                    FROM plex_import_state
                    WHERE id = 1
                    '''
                ).fetchone()
        except sqlite3.OperationalError:
            row = None
        if row is None:
            return {
                'running': False,
                'finished': False,
                'stop_requested': False,
                'total': 0,
                'processed': 0,
                'imported': 0,
                'percent': 0,
                'message': '',
                'current_path': '',
                'error': '',
                'started_at': 0,
                'ended_at': 0,
            }
        return {
            'running': bool(row[0]),
            'finished': bool(row[1]),
            'stop_requested': bool(row[2]),
            'total': int(row[3] or 0),
            'processed': int(row[4] or 0),
            'imported': int(row[5] or 0),
            'percent': int(row[6] or 0),
            'message': row[7] or '',
            'current_path': row[8] or '',
            'error': row[9] or '',
            'started_at': int(row[10] or 0),
            'ended_at': int(row[11] or 0),
        }

    def _write_plex_import_state(self, **kwargs):
        self._ensure_plex_import_state_table()
        current = self._read_plex_import_state()
        current.update(kwargs)
        with self._metadata_db_connect() as conn:
            conn.execute(
                '''
                UPDATE plex_import_state
                SET running = ?, finished = ?, stop_requested = ?, total = ?, processed = ?,
                    imported = ?, percent = ?, message = ?, current_path = ?, error = ?, started_at = ?, ended_at = ?
                WHERE id = 1
                ''',
                (
                    1 if current.get('running') else 0,
                    1 if current.get('finished') else 0,
                    1 if current.get('stop_requested') else 0,
                    int(current.get('total') or 0),
                    int(current.get('processed') or 0),
                    int(current.get('imported') or 0),
                    int(current.get('percent') or 0),
                    current.get('message') or '',
                    current.get('current_path') or '',
                    current.get('error') or '',
                    int(current.get('started_at') or 0),
                    int(current.get('ended_at') or 0),
                ),
            )
            conn.commit()

    def _plex_import_worker_path(self):
        return os.path.join(os.path.dirname(__file__), 'plex_import_worker.py')

    def _normalize_plex_file_to_rel(self, file_path):
        normalized = str(file_path or '').replace('\\', '/').strip()
        if normalized == '':
            return ''
        marker = '/VIDEO/'
        idx = normalized.find(marker)
        if idx >= 0:
            return normalized[idx + len(marker):].strip('/')
        if normalized.startswith('VIDEO/'):
            return normalized[6:].strip('/')
        return ''

    def _plex_import_scope_path(self, relative_path):
        parts = [part for part in str(relative_path or '').replace('\\', '/').split('/') if part]
        if not parts:
            return ''
        if parts[0] == '영화' and len(parts) >= 3 and parts[1] == '최신':
            return '/'.join(parts[:3])
        if parts[0] == '방송중' and len(parts) >= 2:
            return '/'.join(parts[:2])
        if parts[0] == '국내TV' and len(parts) >= 3:
            return '/'.join(parts[:3])
        if len(parts) >= 3:
            return '/'.join(parts[:3])
        if len(parts) >= 2:
            return '/'.join(parts[:2])
        return parts[0]

    def _is_valid_plex_import_scope(self, relative_path):
        scope = self._plex_import_scope_path(relative_path)
        if scope == '':
            return False
        root = os.path.abspath(P.ModelSetting.get('gds_path') or '')
        if root == '':
            return False
        return os.path.isdir(os.path.join(root, scope.replace('/', os.sep)))

    def _is_http_url(self, value):
        text = str(value or '').strip()
        return text.startswith('http://') or text.startswith('https://')

    def _plex_import_count(self, plex_db_path):
        query = '''
        SELECT COUNT(*)
        FROM metadata_items mi
        JOIN media_items mdi ON mdi.metadata_item_id = mi.id
        JOIN media_parts mp ON mp.media_item_id = mdi.id
        LEFT JOIN metadata_items parent ON parent.id = mi.parent_id
        WHERE mp.file IS NOT NULL
          AND TRIM(mp.file) != ''
          AND (
            LOWER(COALESCE(mi.user_thumb_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(mi.user_thumb_url, '')) LIKE 'https://%%'
            OR LOWER(COALESCE(mi.user_art_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(mi.user_art_url, '')) LIKE 'https://%%'
            OR LOWER(COALESCE(parent.user_thumb_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(parent.user_thumb_url, '')) LIKE 'https://%%'
            OR LOWER(COALESCE(parent.user_art_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(parent.user_art_url, '')) LIKE 'https://%%'
          )
        '''
        with sqlite3.connect(plex_db_path) as conn:
            return int(conn.execute(query).fetchone()[0] or 0)

    def _iter_plex_import_rows(self, plex_db_path):
        query = '''
        SELECT
            mp.file AS file_path,
            mi.metadata_type,
            COALESCE(mi.user_thumb_url, '') AS item_thumb_url,
            COALESCE(mi.user_art_url, '') AS item_art_url,
            COALESCE(parent.user_thumb_url, '') AS parent_thumb_url,
            COALESCE(parent.user_art_url, '') AS parent_art_url
        FROM metadata_items mi
        JOIN media_items mdi ON mdi.metadata_item_id = mi.id
        JOIN media_parts mp ON mp.media_item_id = mdi.id
        LEFT JOIN metadata_items parent ON parent.id = mi.parent_id
        WHERE mp.file IS NOT NULL
          AND TRIM(mp.file) != ''
          AND (
            LOWER(COALESCE(mi.user_thumb_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(mi.user_thumb_url, '')) LIKE 'https://%%'
            OR LOWER(COALESCE(mi.user_art_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(mi.user_art_url, '')) LIKE 'https://%%'
            OR LOWER(COALESCE(parent.user_thumb_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(parent.user_thumb_url, '')) LIKE 'https://%%'
            OR LOWER(COALESCE(parent.user_art_url, '')) LIKE 'http://%%'
            OR LOWER(COALESCE(parent.user_art_url, '')) LIKE 'https://%%'
          )
        ORDER BY mp.id
        '''
        conn = sqlite3.connect(plex_db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            for row in cur.execute(query):
                yield row
        finally:
            conn.close()

    def _get_plex_import_status(self):
        return self._read_plex_import_state()

    def _plex_art_lookup(self, relative_path, is_dir, kind):
        rel = str(relative_path or '').replace('\\', '/').strip().strip('/')
        if not rel:
            return ''
        lookup_rel = rel[6:].strip('/') if rel.startswith('VIDEO/') else rel
        self._ensure_plex_art_table()
        with self._metadata_db_connect() as conn:
            if is_dir:
                row = conn.execute(
                    '''
                    SELECT poster_url, thumb_url
                    FROM plex_art_item
                    WHERE series_path = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    ''',
                    (lookup_rel,),
                ).fetchone()
            else:
                row = conn.execute(
                    '''
                    SELECT poster_url, thumb_url
                    FROM plex_art_item
                    WHERE file_path = ?
                    LIMIT 1
                    ''',
                    (lookup_rel,),
                ).fetchone()
                if row is None:
                    row = conn.execute(
                        '''
                        SELECT poster_url, thumb_url
                        FROM plex_art_item
                        WHERE series_path = ?
                        ORDER BY updated_at DESC
                        LIMIT 1
                        ''',
                        (os.path.dirname(lookup_rel).replace('\\', '/'),),
                    ).fetchone()
        if row is None:
            P.logger.info('Plex art miss path=%s lookup=%s is_dir=%s kind=%s', rel, lookup_rel, is_dir, kind)
            return ''
        poster_url = row[0] or ''
        thumb_url = row[1] or ''
        P.logger.info(
            'Plex art hit path=%s lookup=%s is_dir=%s kind=%s poster=%s thumb=%s',
            rel,
            lookup_rel,
            is_dir,
            kind,
            poster_url,
            thumb_url,
        )
        if kind == 'thumb':
            return thumb_url or poster_url
        return poster_url or thumb_url

    def _start_plex_import(self, plex_db_path=''):
        plex_db_path = (plex_db_path or P.ModelSetting.get('plex_db_path') or '').strip()
        if plex_db_path == '':
            return {'ret': 'warning', 'msg': 'Plex DB path is empty'}
        if not os.path.exists(plex_db_path):
            return {'ret': 'warning', 'msg': 'Plex DB path does not exist'}
        P.ModelSetting.set('plex_db_path', plex_db_path)
        state = self._read_plex_import_state()
        if state.get('running'):
            return {'ret': 'warning', 'msg': 'Import already running'}
        self._write_plex_import_state(
            running=True,
            finished=False,
            stop_requested=False,
            total=0,
            processed=0,
            imported=0,
            percent=0,
            message='Preparing import...',
            current_path='',
            error='',
            started_at=int(time.time()),
            ended_at=0,
        )
        with open(self._plex_import_log_path(), 'w', encoding='utf-8') as fp:
            fp.write('[{}] Preparing import\n'.format(time.strftime('%Y-%m-%d %H:%M:%S')))
        subprocess.Popen(
            [
                sys.executable,
                self._plex_import_worker_path(),
                self._metadata_db_path(),
                plex_db_path,
                os.path.abspath(P.ModelSetting.get('gds_path') or ''),
                self._plex_import_log_path(),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        return {'ret': 'success', 'msg': 'Plex import started'}

    def _stop_plex_import(self):
        state = self._read_plex_import_state()
        if not state.get('running'):
            return {'ret': 'warning', 'msg': 'Import is not running'}
        self._write_plex_import_state(stop_requested=True, message='Stopping import...')
        return {'ret': 'success', 'msg': 'Stop requested'}


class ModuleDb(PlexImportMixin, PluginModuleBase):
    def __init__(self, P):
        super(ModuleDb, self).__init__(P, name='db')

    def process_menu(self, page, req):
        arg = P.ModelSetting.to_dict()
        arg['package_name'] = P.package_name
        return render_template(f'{P.package_name}_db.html', arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        try:
            if command == 'start_plex_import':
                return jsonify(self._start_plex_import(arg1))
            if command == 'stop_plex_import':
                return jsonify(self._stop_plex_import())
            if command == 'plex_import_status':
                return jsonify({'ret': 'success', 'data': self._get_plex_import_status()})
            if command == 'plex_import_recent_items':
                return jsonify({'ret': 'success', 'data': self._get_recent_plex_import_items()})
            return jsonify({'ret': 'warning', 'msg': f'Unknown command: {command}'})
        except Exception as e:
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'danger', 'msg': str(e)})
