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
        'skipped': 0,
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

    def _db_tool_item_log_path(self, kind='added'):
        suffix = 'deleted' if str(kind or '').lower() == 'deleted' else 'added'
        return os.path.join(os.path.dirname(self._metadata_db_path()), f'plex_{suffix}_items.log')

    def _append_db_tool_log(self, message):
        log_path = self._plex_import_log_path()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        try:
            with open(log_path, 'a', encoding='utf-8') as fp:
                fp.write('[{}] {}\n'.format(timestamp, message))
        except Exception:
            pass

    def _read_plex_import_log_tail(self, max_lines=80, max_chars=12000):
        log_path = self._plex_import_log_path()
        return self._read_text_log_tail(log_path, max_lines=max_lines, max_chars=max_chars)

    def _read_text_log_tail(self, log_path, max_lines=80, max_chars=12000):
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
                SELECT rowid, file_path, series_path, updated_at
                FROM plex_art_item
                ORDER BY updated_at DESC, rowid DESC
                LIMIT ?
                ''',
                (int(limit_count or 80),),
            ).fetchall()
        result = []
        for row in rows:
            file_path = row[1] or ''
            series_path = row[2] or ''
            updated_at = int(row[3] or 0)
            label = series_path or file_path
            result.append({
                'file_path': file_path,
                'series_path': series_path,
                'updated_at': updated_at,
                'label': label,
            })
        return result

    def _ensure_cleanup_deleted_table(self):
        with self._metadata_db_connect() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS plex_cleanup_deleted (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT NOT NULL,
                    deleted_at INTEGER NOT NULL DEFAULT 0
                )
                '''
            )
            conn.execute(
                '''
                CREATE INDEX IF NOT EXISTS idx_plex_cleanup_deleted_deleted_at
                ON plex_cleanup_deleted(deleted_at)
                '''
            )
            conn.commit()

    def _get_recent_cleanup_deleted_items(self, limit_count=80):
        self._ensure_cleanup_deleted_table()
        with self._metadata_db_connect(timeout=0.2) as conn:
            rows = conn.execute(
                '''
                SELECT path, deleted_at
                FROM plex_cleanup_deleted
                ORDER BY deleted_at DESC, id DESC
                LIMIT ?
                ''',
                (int(limit_count or 80),),
            ).fetchall()
        result = []
        for row in rows:
            path = row[0] or ''
            deleted_at = int(row[1] or 0)
            result.append({
                'path': path,
                'deleted_at': deleted_at,
                'label': path,
            })
        return result

    def _get_db_tool_log_tail(self):
        return self._read_plex_import_log_tail()

    def _get_db_tool_item_log_tail(self, kind='added'):
        return self._read_text_log_tail(self._db_tool_item_log_path(kind))

    def _cleanup_scheduler_job_id(self):
        return f'{P.package_name}_db_cleanup'

    def _cleanup_schedule_expression(self):
        value = (P.ModelSetting.get('db_cleanup_schedule') or '').strip()
        if value == '':
            value = (P.ModelSetting.get('db_cleanup_cron') or '').strip()
        if value == '':
            value = (P.ModelSetting.get('db_cleanup_interval') or '').strip()
        return value

    def _sync_cleanup_scheduler(self, enabled_override=None, schedule_override=None):
        if enabled_override is not None:
            enabled_text = str(enabled_override).strip().lower()
            enabled_value = 'True' if enabled_text in ('true', '1', 'on', 'yes') else 'False'
            P.ModelSetting.set('db_cleanup_schedule_enabled', enabled_value)
        if schedule_override is not None:
            P.ModelSetting.set('db_cleanup_schedule', str(schedule_override or '').strip())
        job_id = self._cleanup_scheduler_job_id()
        enabled = str(P.ModelSetting.get('db_cleanup_schedule_enabled') or 'False').lower() == 'true'
        schedule = self._cleanup_schedule_expression()
        if F.scheduler.is_include(job_id):
            F.scheduler.remove_job(job_id)
        if not enabled or schedule == '':
            return {'ret': 'success', 'msg': ''}
        job = Job(P.package_name, job_id, schedule, self._scheduled_cleanup_job, 'ff_kodis DB cleanup')
        F.scheduler.add_job_instance(job)
        return {'ret': 'success', 'msg': ''}

    def _scheduled_cleanup_job(self):
        result = self._start_plex_cleanup()
        self._append_db_tool_log('scheduled_cleanup_job dispatched ret={} msg={}'.format(result.get('ret'), result.get('msg')))
        if result.get('ret') != 'success':
            return
        self._wait_for_cleanup_completion()

    def _wait_for_cleanup_completion(self, startup_timeout=15, poll_interval=1.0):
        started = False
        deadline = time.time() + max(float(startup_timeout or 15), 1.0)
        while time.time() < deadline:
            state = self._read_plex_import_state()
            if state.get('running'):
                started = True
                break
            if state.get('finished'):
                self._append_db_tool_log(
                    'scheduled_cleanup_job finished before running state message={} error={}'.format(
                        state.get('message', ''),
                        state.get('error', ''),
                    )
                )
                return
            time.sleep(poll_interval)
        if not started:
            self._append_db_tool_log('scheduled_cleanup_job wait timeout before running state')
            return
        while True:
            state = self._read_plex_import_state()
            if not state.get('running'):
                self._append_db_tool_log(
                    'scheduled_cleanup_job completed message={} error={}'.format(
                        state.get('message', ''),
                        state.get('error', ''),
                    )
                )
                return
            time.sleep(poll_interval)

    def _normalize_cleanup_paths(self, cleanup_path):
        result = []
        for chunk in str(cleanup_path or '').replace('\r', '\n').split('\n'):
            normalized = chunk.strip()
            if normalized:
                result.append(normalized)
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
                    skipped INTEGER NOT NULL DEFAULT 0,
                    percent INTEGER NOT NULL DEFAULT 0,
                    message TEXT NOT NULL DEFAULT '',
                    current_path TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    worker_pid INTEGER NOT NULL DEFAULT 0,
                    started_at INTEGER NOT NULL DEFAULT 0,
                    ended_at INTEGER NOT NULL DEFAULT 0
                )
                '''
            )
            columns = [row[1] for row in conn.execute("PRAGMA table_info(plex_import_state)").fetchall()]
            if 'current_path' not in columns:
                conn.execute("ALTER TABLE plex_import_state ADD COLUMN current_path TEXT NOT NULL DEFAULT ''")
            if 'worker_pid' not in columns:
                conn.execute("ALTER TABLE plex_import_state ADD COLUMN worker_pid INTEGER NOT NULL DEFAULT 0")
            if 'skipped' not in columns:
                conn.execute("ALTER TABLE plex_import_state ADD COLUMN skipped INTEGER NOT NULL DEFAULT 0")
            conn.execute(
                '''
                INSERT OR IGNORE INTO plex_import_state (
                    id, running, finished, stop_requested, total, processed, imported, skipped,
                    percent, message, current_path, error, worker_pid, started_at, ended_at
                ) VALUES (1, 0, 0, 0, 0, 0, 0, 0, 0, '', '', '', 0, 0, 0)
                '''
            )
            conn.commit()

    def _read_plex_import_state_raw(self):
        try:
            with self._metadata_db_connect(timeout=0.2) as conn:
                row = conn.execute(
                    '''
                    SELECT running, finished, stop_requested, total, processed, imported, skipped,
                           percent, message, current_path, error, worker_pid, started_at, ended_at
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
                'skipped': 0,
                'percent': 0,
                'message': '',
                'current_path': '',
                'error': '',
                'worker_pid': 0,
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
            'skipped': int(row[6] or 0),
            'percent': int(row[7] or 0),
            'message': row[8] or '',
            'current_path': row[9] or '',
            'error': row[10] or '',
            'worker_pid': int(row[11] or 0),
            'started_at': int(row[12] or 0),
            'ended_at': int(row[13] or 0),
        }

    def _is_pid_alive(self, pid):
        try:
            pid = int(pid or 0)
        except Exception:
            return False
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
        except Exception:
            return False

    def _clear_stale_plex_import_state(self):
        state = self._read_plex_import_state_raw()
        if not state.get('running'):
            return state
        worker_pid = int(state.get('worker_pid') or 0)
        if worker_pid > 0 and self._is_pid_alive(worker_pid):
            return state
        self._append_db_tool_log(
            'stale_db_task_reset message={} current_path={} worker_pid={}'.format(
                state.get('message') or '',
                state.get('current_path') or '',
                worker_pid,
            )
        )
        self._write_plex_import_state(
            running=False,
            finished=True,
            stop_requested=False,
            message='Task state reset after restart',
            error='',
            worker_pid=0,
            ended_at=int(time.time()),
        )
        return self._read_plex_import_state_raw()

    def _read_plex_import_state(self):
        return self._clear_stale_plex_import_state()

    def _write_plex_import_state(self, **kwargs):
        self._ensure_plex_import_state_table()
        current = self._read_plex_import_state_raw()
        current.update(kwargs)
        with self._metadata_db_connect() as conn:
            conn.execute(
                '''
                UPDATE plex_import_state
                SET running = ?, finished = ?, stop_requested = ?, total = ?, processed = ?,
                    imported = ?, skipped = ?, percent = ?, message = ?, current_path = ?, error = ?, worker_pid = ?, started_at = ?, ended_at = ?
                WHERE id = 1
                ''',
                (
                    1 if current.get('running') else 0,
                    1 if current.get('finished') else 0,
                    1 if current.get('stop_requested') else 0,
                    int(current.get('total') or 0),
                    int(current.get('processed') or 0),
                    int(current.get('imported') or 0),
                    int(current.get('skipped') or 0),
                    int(current.get('percent') or 0),
                    current.get('message') or '',
                    current.get('current_path') or '',
                    current.get('error') or '',
                    int(current.get('worker_pid') or 0),
                    int(current.get('started_at') or 0),
                    int(current.get('ended_at') or 0),
                ),
            )
            conn.commit()

    def _db_worker_path(self):
        return os.path.join(os.path.dirname(__file__), 'kodis_db_worker.py')

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
        WHERE mp.file IS NOT NULL
          AND TRIM(mp.file) != ''
          AND (
            mp.file LIKE '%/VIDEO/%'
            OR mp.file LIKE 'VIDEO/%'
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
            mp.file LIKE '%/VIDEO/%'
            OR mp.file LIKE 'VIDEO/%'
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
            self._append_db_tool_log('start_plex_import rejected: empty plex_db_path')
            return {'ret': 'warning', 'msg': 'Plex DB path is empty'}
        if not os.path.exists(plex_db_path):
            self._append_db_tool_log('start_plex_import rejected: path does not exist path={}'.format(plex_db_path))
            return {'ret': 'warning', 'msg': 'Plex DB path does not exist'}
        P.ModelSetting.set('plex_db_path', plex_db_path)
        state = self._read_plex_import_state()
        if state.get('running'):
            self._append_db_tool_log(
                'start_plex_import rejected: task already running message={} current_path={}'.format(
                    state.get('message') or '',
                    state.get('current_path') or '',
                )
            )
            return {'ret': 'warning', 'msg': 'Import already running'}
        self._write_plex_import_state(
            running=True,
            finished=False,
            stop_requested=False,
            total=0,
            processed=0,
            imported=0,
            skipped=0,
            percent=0,
            message='Preparing import...',
            current_path='',
            error='',
            started_at=int(time.time()),
            ended_at=0,
        )
        with open(self._plex_import_log_path(), 'w', encoding='utf-8') as fp:
            fp.write('[{}] Preparing import\n'.format(time.strftime('%Y-%m-%d %H:%M:%S')))
        with open(self._db_tool_item_log_path('added'), 'w', encoding='utf-8') as fp:
            fp.write('')
        self._append_db_tool_log('start_plex_import accepted: plex_db_path={}'.format(plex_db_path))
        proc = subprocess.Popen(
            [
                sys.executable,
                self._db_worker_path(),
                self._metadata_db_path(),
                plex_db_path,
                os.path.abspath(P.ModelSetting.get('gds_path') or ''),
                self._plex_import_log_path(),
                self._db_tool_item_log_path('added'),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        self._write_plex_import_state(worker_pid=proc.pid)
        self._append_db_tool_log('start_plex_import worker spawned')
        return {'ret': 'success', 'msg': 'Plex import started'}

    def _start_kodis_db_import(self, kodis_db_url=''):
        kodis_db_url = (kodis_db_url or P.ModelSetting.get('kodis_db_url') or '').strip()
        if kodis_db_url == '':
            self._append_db_tool_log('start_kodis_db_import rejected: empty kodis_db_url')
            return {'ret': 'warning', 'msg': 'Kodis DB URL is empty'}
        P.ModelSetting.set('kodis_db_url', kodis_db_url)
        state = self._read_plex_import_state()
        if state.get('running'):
            self._append_db_tool_log(
                'start_kodis_db_import rejected: task already running message={} current_path={}'.format(
                    state.get('message') or '',
                    state.get('current_path') or '',
                )
            )
            return {'ret': 'warning', 'msg': 'Another DB task is already running'}
        self._write_plex_import_state(
            running=True,
            finished=False,
            stop_requested=False,
            total=0,
            processed=0,
            imported=0,
            skipped=0,
            percent=0,
            message='Preparing DB import...',
            current_path='',
            error='',
            started_at=int(time.time()),
            ended_at=0,
        )
        with open(self._plex_import_log_path(), 'w', encoding='utf-8') as fp:
            fp.write('[{}] Preparing DB import\n'.format(time.strftime('%Y-%m-%d %H:%M:%S')))
        self._append_db_tool_log('start_kodis_db_import accepted: source_url={}'.format(kodis_db_url))
        proc = subprocess.Popen(
            [
                sys.executable,
                self._db_worker_path(),
                self._metadata_db_path(),
                '',
                '',
                self._plex_import_log_path(),
                '',
                'db_import',
                kodis_db_url,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        self._write_plex_import_state(worker_pid=proc.pid)
        self._append_db_tool_log('start_kodis_db_import worker spawned')
        return {'ret': 'success', 'msg': 'Kodis DB import started'}

    def _start_plex_cleanup(self, cleanup_path=''):
        gds_root = os.path.abspath(P.ModelSetting.get('gds_path') or '')
        cleanup_path = (cleanup_path or P.ModelSetting.get('db_cleanup_path') or '').strip()
        cleanup_paths = self._normalize_cleanup_paths(cleanup_path)
        if gds_root == '':
            self._append_db_tool_log('start_plex_cleanup rejected: empty gds_path')
            return {'ret': 'warning', 'msg': 'GDS path is empty'}
        if not os.path.isdir(gds_root):
            self._append_db_tool_log('start_plex_cleanup rejected: invalid gds_path path={}'.format(gds_root))
            return {'ret': 'warning', 'msg': 'GDS path does not exist'}
        state = self._read_plex_import_state()
        if state.get('running'):
            self._append_db_tool_log(
                'start_plex_cleanup rejected: task already running message={} current_path={}'.format(
                    state.get('message') or '',
                    state.get('current_path') or '',
                )
            )
            return {'ret': 'warning', 'msg': 'Another DB task is already running'}
        self._write_plex_import_state(
            running=True,
            finished=False,
            stop_requested=False,
            total=0,
            processed=0,
            imported=0,
            skipped=0,
            percent=0,
            message='Preparing cleanup...',
            current_path='',
            error='',
            started_at=int(time.time()),
            ended_at=0,
        )
        with open(self._plex_import_log_path(), 'w', encoding='utf-8') as fp:
            fp.write('[{}] Preparing cleanup\n'.format(time.strftime('%Y-%m-%d %H:%M:%S')))
        with open(self._db_tool_item_log_path('deleted'), 'w', encoding='utf-8') as fp:
            fp.write('')
        self._append_db_tool_log('start_plex_cleanup accepted: gds_root={} cleanup_path={}'.format(gds_root, ' | '.join(cleanup_paths) if cleanup_paths else '/'))
        proc = subprocess.Popen(
            [
                sys.executable,
                self._db_worker_path(),
                self._metadata_db_path(),
                '',
                gds_root,
                self._plex_import_log_path(),
                self._db_tool_item_log_path('deleted'),
                'cleanup',
                '\n'.join(cleanup_paths),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        self._write_plex_import_state(worker_pid=proc.pid)
        self._append_db_tool_log('start_plex_cleanup worker spawned')
        return {'ret': 'success', 'msg': 'Plex cleanup started'}

    def _start_db_vacuum(self):
        state = self._read_plex_import_state()
        if state.get('running'):
            self._append_db_tool_log(
                'start_db_vacuum rejected: task already running message={} current_path={}'.format(
                    state.get('message') or '',
                    state.get('current_path') or '',
                )
            )
            return {'ret': 'warning', 'msg': 'Another DB task is already running'}
        self._write_plex_import_state(
            running=True,
            finished=False,
            stop_requested=False,
            total=0,
            processed=0,
            imported=0,
            skipped=0,
            percent=0,
            message='Preparing DB optimization...',
            current_path='',
            error='',
            started_at=int(time.time()),
            ended_at=0,
        )
        with open(self._plex_import_log_path(), 'w', encoding='utf-8') as fp:
            fp.write('[{}] Preparing DB optimization\n'.format(time.strftime('%Y-%m-%d %H:%M:%S')))
        self._append_db_tool_log('start_db_vacuum accepted')
        proc = subprocess.Popen(
            [
                sys.executable,
                self._db_worker_path(),
                self._metadata_db_path(),
                '',
                '',
                self._plex_import_log_path(),
                '',
                'vacuum',
                '',
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        self._write_plex_import_state(worker_pid=proc.pid)
        self._append_db_tool_log('start_db_vacuum worker spawned')
        return {'ret': 'success', 'msg': 'DB optimization started'}

    def _stop_plex_import(self):
        state = self._read_plex_import_state()
        if not state.get('running'):
            self._append_db_tool_log('stop_plex_import ignored: no running task')
            return {'ret': 'warning', 'msg': 'Import is not running'}
        self._write_plex_import_state(stop_requested=True, message='Stopping import...')
        self._append_db_tool_log(
            'stop_plex_import requested: message={} current_path={}'.format(
                state.get('message') or '',
                state.get('current_path') or '',
            )
        )
        return {'ret': 'success', 'msg': 'Stop requested'}

    def _start_index_scan(self, scan_root_path=''):
        scan_root_path = (scan_root_path or P.ModelSetting.get('index_scan_path') or '').strip()
        exclude_text = (P.ModelSetting.get('index_scan_exclude') or '').strip()
        if scan_root_path == '':
            self._append_db_tool_log('start_index_scan rejected: empty scan_root_path')
            return {'ret': 'warning', 'msg': 'Index scan path is empty'}
        if not os.path.exists(scan_root_path):
            self._append_db_tool_log('start_index_scan rejected: path does not exist path={}'.format(scan_root_path))
            return {'ret': 'warning', 'msg': 'Index scan path does not exist'}
        if not os.path.isdir(scan_root_path):
            self._append_db_tool_log('start_index_scan rejected: not a directory path={}'.format(scan_root_path))
            return {'ret': 'warning', 'msg': 'Index scan path is not a directory'}
        P.ModelSetting.set('index_scan_path', scan_root_path)
        state = self._read_plex_import_state()
        if state.get('running'):
            self._append_db_tool_log(
                'start_index_scan rejected: task already running message={} current_path={}'.format(
                    state.get('message') or '',
                    state.get('current_path') or '',
                )
            )
            return {'ret': 'warning', 'msg': 'Another DB task is already running'}
        self._write_plex_import_state(
            running=True,
            finished=False,
            stop_requested=False,
            total=0,
            processed=0,
            imported=0,
            skipped=0,
            percent=0,
            message='Preparing index scan...',
            current_path='',
            error='',
            started_at=int(time.time()),
            ended_at=0,
        )
        with open(self._plex_import_log_path(), 'w', encoding='utf-8') as fp:
            fp.write('[{}] Preparing index scan\n'.format(time.strftime('%Y-%m-%d %H:%M:%S')))
        with open(self._db_tool_item_log_path('added'), 'w', encoding='utf-8') as fp:
            fp.write('')
        self._append_db_tool_log('start_index_scan accepted: scan_root_path={}'.format(os.path.abspath(scan_root_path)))
        if exclude_text:
            self._append_db_tool_log('start_index_scan exclude_rules={}'.format(exclude_text.replace('\r', ' ').replace('\n', ' | ')))
        proc = subprocess.Popen(
            [
                sys.executable,
                self._db_worker_path(),
                self._metadata_db_path(),
                os.path.abspath(scan_root_path),
                os.path.abspath(P.ModelSetting.get('gds_path') or ''),
                self._plex_import_log_path(),
                self._db_tool_item_log_path('added'),
                'scan',
                exclude_text,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        self._write_plex_import_state(worker_pid=proc.pid)
        self._append_db_tool_log('start_index_scan worker spawned')
        return {'ret': 'success', 'msg': 'Index scan started'}


class ModuleDb(PlexImportMixin, PluginModuleBase):
    db_default = {
        'index_scan_path': '',
        'index_scan_exclude': '',
        'plex_db_path': '',
        'kodis_db_url': '',
        'db_cleanup_path': '',
        'db_cleanup_schedule_enabled': 'False',
        'db_cleanup_schedule': '',
    }

    def __init__(self, P):
        super(ModuleDb, self).__init__(P, name='db')

    def process_menu(self, page, req):
        arg = P.ModelSetting.to_dict()
        arg['package_name'] = P.package_name
        arg['db_cleanup_schedule_active'] = str(F.scheduler.is_include(self._cleanup_scheduler_job_id()))
        return render_template(f'{P.package_name}_db.html', arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        try:
            if command == 'start_plex_import':
                return jsonify(self._start_plex_import(arg1))
            if command == 'start_kodis_db_import':
                return jsonify(self._start_kodis_db_import(arg1))
            if command == 'start_plex_cleanup':
                return jsonify(self._start_plex_cleanup(arg1))
            if command == 'start_db_vacuum':
                return jsonify(self._start_db_vacuum())
            if command == 'sync_cleanup_schedule':
                return jsonify(self._sync_cleanup_scheduler(arg1, arg2))
            if command == 'start_index_scan':
                return jsonify(self._start_index_scan(arg1))
            if command == 'stop_plex_import':
                return jsonify(self._stop_plex_import())
            if command == 'plex_import_status':
                return jsonify({'ret': 'success', 'data': self._get_plex_import_status()})
            if command == 'cleanup_deleted_items':
                return jsonify({'ret': 'success', 'data': self._get_recent_cleanup_deleted_items()})
            if command == 'db_tool_log_tail':
                return jsonify({'ret': 'success', 'data': self._get_db_tool_log_tail()})
            if command == 'db_tool_item_log_tail':
                return jsonify({'ret': 'success', 'data': self._get_db_tool_item_log_tail(arg1)})
            if command == 'plex_import_recent_items':
                return jsonify({'ret': 'success', 'data': self._get_recent_plex_import_items()})
            return jsonify({'ret': 'warning', 'msg': f'Unknown command: {command}'})
        except Exception as e:
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def plugin_load(self):
        try:
            self._ensure_plex_import_state_table()
            self._clear_stale_plex_import_state()
            self._sync_cleanup_scheduler()
        except Exception as e:
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
 
