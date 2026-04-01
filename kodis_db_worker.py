import os
import re
import sqlite3
import sys
import tarfile
import tempfile
import time
import traceback
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from kodis_plexdb import KodisPlexDBHandle


def db_connect(metadata_db_path, timeout=30):
    conn = sqlite3.connect(metadata_db_path, timeout=timeout)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
    except Exception:
        pass
    return conn


def diagnostic_log(log_path, message):
    if not log_path:
        return
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        with open(log_path, 'a', encoding='utf-8') as fp:
            fp.write('[{}] {}\n'.format(timestamp, message))
    except Exception:
        pass


def append_item_log(item_log_path, item_path):
    if not item_log_path:
        return
    text = str(item_path or '').strip()
    if text == '':
        return
    try:
        with open(item_log_path, 'a', encoding='utf-8') as fp:
            fp.write(text + '\n')
    except Exception:
        pass


def looks_like_sqlite_file(file_path):
    try:
        with open(file_path, 'rb') as fp:
            header = fp.read(16)
        return header == b'SQLite format 3\x00'
    except Exception:
        return False


def looks_like_archive_file(file_path):
    lower_path = str(file_path or '').strip().lower()
    return lower_path.endswith('.tgz') or lower_path.endswith('.tar.gz')


def is_av_relative_path(relative_path):
    rel = str(relative_path or '').replace('\\', '/').strip().strip('/')
    if rel == '':
        return False
    return rel.upper() == 'AV' or rel.upper().startswith('AV/')


def normalize_shared_db_url(source_url):
    url = str(source_url or '').strip()
    if url == '':
        return ''
    parsed = urlparse(url)
    if 'drive.google.com' not in (parsed.netloc or '').lower():
        return url
    match = re.search(r'/file/d/([^/]+)/', parsed.path or '')
    if match:
        return 'https://drive.google.com/uc?export=download&id={}'.format(match.group(1))
    query_id = parse_qs(parsed.query or '').get('id', [])
    if query_id:
        return 'https://drive.google.com/uc?export=download&id={}'.format(query_id[0])
    return url


def write_stopped_state(metadata_db_path, message):
    write_state(
        metadata_db_path,
        running=False,
        finished=True,
        message=message,
        worker_pid=0,
        ended_at=int(time.time()),
    )


def copy_local_db_to_temp(source_path, temp_path, metadata_db_path, log_path):
    source_size = int(os.path.getsize(source_path) or 0)
    copied = 0
    diagnostic_log(log_path, 'db_import:copy_local source_path={} size={}'.format(source_path, source_size))
    with open(source_path, 'rb') as src, open(temp_path, 'wb') as dst:
        while True:
            state = read_state(metadata_db_path)
            if state.get('stop_requested'):
                diagnostic_log(log_path, 'db_import:stopped during local copy')
                write_stopped_state(metadata_db_path, 'DB import stopped')
                return False
            chunk = src.read(1024 * 512)
            if not chunk:
                break
            dst.write(chunk)
            copied += len(chunk)
            write_state(
                metadata_db_path,
                total=source_size,
                processed=copied,
                imported=0,
                percent=int(copied * 100 / source_size) if source_size > 0 else 0,
                message='Copying shared DB file...',
                current_path=os.path.basename(source_path),
            )
    return True


def download_db_to_temp(source_url, temp_path, metadata_db_path, log_path):
    normalized_url = normalize_shared_db_url(source_url)
    diagnostic_log(log_path, 'db_import:download url={}'.format(normalized_url))
    request = Request(normalized_url, headers={'User-Agent': 'ff_kodis/1.0'})
    with urlopen(request, timeout=30) as response, open(temp_path, 'wb') as dst:
        total_size = int(response.headers.get('Content-Length') or 0)
        downloaded = 0
        current_name = os.path.basename(urlparse(normalized_url).path or '') or 'metadata.sqlite'
        while True:
            state = read_state(metadata_db_path)
            if state.get('stop_requested'):
                diagnostic_log(log_path, 'db_import:stopped during download')
                write_stopped_state(metadata_db_path, 'DB import stopped')
                return False
            chunk = response.read(1024 * 512)
            if not chunk:
                break
            dst.write(chunk)
            downloaded += len(chunk)
            write_state(
                metadata_db_path,
                total=total_size,
                processed=downloaded,
                imported=0,
                percent=int(downloaded * 100 / total_size) if total_size > 0 else 0,
                message='Downloading shared DB...',
                current_path=current_name,
            )
    return True


def extract_db_from_archive(archive_path, temp_path, metadata_db_path, log_path):
    diagnostic_log(log_path, 'db_import:extract_archive archive_path={}'.format(archive_path))
    candidates = []
    with tarfile.open(archive_path, 'r:*') as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            lower_name = str(member.name or '').lower()
            if lower_name.endswith('.db') or lower_name.endswith('.sqlite') or lower_name.endswith('.sqlite3'):
                priority = 0
                if 'com.plexapp.plugins.library' in lower_name:
                    priority += 100
                if lower_name.endswith('.db'):
                    priority += 10
                candidates.append((priority, member))
        if not candidates:
            raise Exception('No SQLite DB file found in archive')
        candidates.sort(key=lambda item: (-item[0], len(item[1].name or '')))
        member = candidates[0][1]
        diagnostic_log(log_path, 'db_import:extract_member name={}'.format(member.name))
        extracted = tar.extractfile(member)
        if extracted is None:
            raise Exception('Failed to extract DB file from archive')
        total_size = int(member.size or 0)
        copied = 0
        with extracted, open(temp_path, 'wb') as dst:
            while True:
                state = read_state(metadata_db_path)
                if state.get('stop_requested'):
                    diagnostic_log(log_path, 'db_import:stopped during archive extract')
                    write_stopped_state(metadata_db_path, 'DB import stopped')
                    return False
                chunk = extracted.read(1024 * 512)
                if not chunk:
                    break
                dst.write(chunk)
                copied += len(chunk)
                write_state(
                    metadata_db_path,
                    total=total_size,
                    processed=copied,
                    imported=0,
                    percent=int(copied * 100 / total_size) if total_size > 0 else 0,
                    message='Extracting DB from archive...',
                    current_path=os.path.basename(member.name or archive_path),
                )
    return True


def prepare_plex_db_source(plex_db_path, metadata_db_path, log_path):
    source_path = str(plex_db_path or '').strip()
    if source_path == '':
        raise Exception('Plex DB path is empty')
    if not looks_like_archive_file(source_path):
        return source_path, None
    metadata_dir = os.path.dirname(os.path.abspath(metadata_db_path)) or None
    temp_fd, temp_path = tempfile.mkstemp(prefix='ff_kodis_plex_source_', suffix='.sqlite', dir=metadata_dir)
    os.close(temp_fd)
    ok = extract_db_from_archive(source_path, temp_path, metadata_db_path, log_path)
    if not ok:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
        return None, None
    if not looks_like_sqlite_file(temp_path):
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
        raise Exception('Extracted file is not a valid SQLite database')
    diagnostic_log(log_path, 'plex_import:using_extracted_db path={}'.format(temp_path))
    return temp_path, temp_path


def run_db_import(metadata_db_path, source_path, log_path, source_url=''):
    source_path = str(source_path or '').strip()
    source_url = str(source_url or '').strip()
    if source_path == '' and source_url == '':
        raise Exception('DB source path or URL is required')
    diagnostic_log(log_path, 'db_import:start source_path={} source_url={}'.format(source_path, source_url))
    metadata_dir = os.path.dirname(os.path.abspath(metadata_db_path)) or None
    temp_fd, temp_path = tempfile.mkstemp(prefix='ff_kodis_db_', suffix='.sqlite', dir=metadata_dir)
    os.close(temp_fd)
    source_temp_fd, source_temp_path = tempfile.mkstemp(prefix='ff_kodis_source_', suffix='.tmp', dir=metadata_dir)
    os.close(source_temp_fd)
    try:
        write_state(
            metadata_db_path,
            running=True,
            finished=False,
            stop_requested=False,
            total=0,
            processed=0,
            imported=0,
            percent=0,
            message='Preparing DB import...',
            current_path='',
            error='',
            started_at=int(time.time()),
            ended_at=0,
        )
        is_archive_source = looks_like_archive_file(source_url or source_path)
        if source_url != '':
            target_download_path = source_temp_path if is_archive_source else temp_path
            ok = download_db_to_temp(source_url, target_download_path, metadata_db_path, log_path)
        else:
            target_copy_path = source_temp_path if is_archive_source else temp_path
            ok = copy_local_db_to_temp(source_path, target_copy_path, metadata_db_path, log_path)
        if not ok:
            return
        if is_archive_source:
            ok = extract_db_from_archive(source_temp_path, temp_path, metadata_db_path, log_path)
            if not ok:
                return
        if not looks_like_sqlite_file(temp_path):
            raise Exception('Downloaded file is not a valid SQLite database')
        diagnostic_log(log_path, 'db_import:replace metadata_db_path={}'.format(metadata_db_path))
        os.replace(temp_path, metadata_db_path)
        ensure_tables(metadata_db_path)
        ensure_state_row(metadata_db_path)
        write_state(
            metadata_db_path,
            running=False,
            finished=True,
            stop_requested=False,
            total=1,
            processed=1,
            imported=1,
            worker_pid=0,
            percent=100,
            message='DB import complete',
            current_path=os.path.basename(metadata_db_path),
            ended_at=int(time.time()),
        )
        diagnostic_log(log_path, 'db_import:complete')
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
        if os.path.exists(source_temp_path):
            try:
                os.remove(source_temp_path)
            except Exception:
                pass


def run_vacuum(metadata_db_path, log_path):
    diagnostic_log(log_path, 'db_vacuum:start metadata_db_path={}'.format(metadata_db_path))
    write_state(
        metadata_db_path,
        running=True,
        finished=False,
        stop_requested=False,
        total=1,
        processed=0,
        imported=0,
        percent=0,
        message='Optimizing DB (VACUUM)...',
        current_path=os.path.basename(metadata_db_path),
        error='',
        started_at=int(time.time()),
        ended_at=0,
    )
    with db_connect(metadata_db_path) as conn:
        conn.execute('VACUUM')
    write_state(
        metadata_db_path,
        running=False,
        finished=True,
        stop_requested=False,
        total=1,
        processed=1,
        imported=1,
        worker_pid=0,
        percent=100,
        message='DB optimization complete',
        current_path=os.path.basename(metadata_db_path),
        ended_at=int(time.time()),
    )
    diagnostic_log(log_path, 'db_vacuum:complete')


def ensure_tables(metadata_db_path):
    with db_connect(metadata_db_path) as conn:
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


def ensure_state_row(metadata_db_path):
    with db_connect(metadata_db_path) as conn:
        conn.execute(
            '''
            INSERT OR IGNORE INTO plex_import_state (
                id, running, finished, stop_requested, total, processed, imported, skipped,
                percent, message, current_path, error, worker_pid, started_at, ended_at
            ) VALUES (1, 0, 0, 0, 0, 0, 0, 0, 0, '', '', '', 0, 0, 0)
            '''
        )
        conn.commit()


def read_state(metadata_db_path):
    try:
        with db_connect(metadata_db_path, timeout=0.2) as conn:
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
        return {}
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


def write_state(metadata_db_path, conn=None, **kwargs):
    current = read_state(metadata_db_path)
    current.update(kwargs)
    sql = '''
    UPDATE plex_import_state
    SET running = ?, finished = ?, stop_requested = ?, total = ?, processed = ?,
        imported = ?, skipped = ?, percent = ?, message = ?, current_path = ?, error = ?, worker_pid = ?, started_at = ?, ended_at = ?
    WHERE id = 1
    '''
    params = (
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
    )
    if conn is not None:
        conn.execute(sql, params)
        conn.commit()
        return
    with db_connect(metadata_db_path) as write_conn:
        write_conn.execute(sql, params)
        write_conn.commit()


def is_http_url(value):
    text = str(value or '').strip()
    lower_text = text.lower()
    if 'discord' in lower_text:
        return False
    return text.startswith('http://') or text.startswith('https://')


def normalize_plex_file_to_rel(file_path):
    normalized = str(file_path or '').replace('\\', '/').strip()
    if normalized == '':
        return ''
    marker = '/VIDEO/'
    idx = normalized.find(marker)
    if idx >= 0:
        rel = normalized[idx + len(marker):].strip('/')
        return '' if is_av_relative_path(rel) else rel
    if normalized.startswith('VIDEO/'):
        rel = normalized[6:].strip('/')
        return '' if is_av_relative_path(rel) else rel
    return ''


def extract_title_year_series_path(relative_path):
    rel = str(relative_path or '').replace('\\', '/').strip().strip('/')
    if rel == '':
        return ''
    parts = [part for part in rel.split('/') if part]
    if len(parts) <= 1:
        return ''
    dir_parts = parts[:-1]
    for idx in range(len(dir_parts) - 1, -1, -1):
        if looks_like_title_year(dir_parts[idx]):
            return '/'.join(dir_parts[:idx + 1])
    return ''


def is_video_library_path(file_path):
    normalized = str(file_path or '').replace('\\', '/').strip().lower()
    if not ('/video/' in normalized or normalized.startswith('video/')):
        return False
    rel = normalize_plex_file_to_rel(file_path)
    return rel != ''


def plex_import_scope_path(relative_path):
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


def is_valid_import_scope(relative_path, gds_root):
    scope = plex_import_scope_path(relative_path)
    if scope == '':
        return False
    return True


def resolve_library_dir(relative_path, gds_root, log_path=''):
    rel = str(relative_path or '').replace('\\', '/').strip().strip('/')
    if rel == '' or gds_root == '':
        return ''
    root = os.path.abspath(gds_root)
    candidates = [
        os.path.join(root, rel.replace('/', os.sep)),
        os.path.join(root, 'VIDEO', rel.replace('/', os.sep)),
    ]
    for candidate in candidates:
        if exists_dir_by_parent_listing(candidate, log_path):
            return candidate
    return ''


def exists_dir_by_parent_listing(candidate, log_path=''):
    target = os.path.abspath(candidate)
    parent = os.path.dirname(target)
    name = os.path.basename(target)
    if parent == '' or name == '':
        diagnostic_log(log_path, 'cleanup:check skipped target={}'.format(target))
        return False
    try:
        with os.scandir(parent) as entries:
            for entry in entries:
                if entry.name != name:
                    continue
                try:
                    exists = entry.is_dir()
                except Exception:
                    exists = True
                diagnostic_log(log_path, 'cleanup:check parent={} name={} exists={}'.format(parent, name, str(exists).lower()))
                return exists
    except Exception:
        diagnostic_log(log_path, 'cleanup:check parent={} name={} exists=false error=scandir_failed'.format(parent, name))
        return False
    diagnostic_log(log_path, 'cleanup:check parent={} name={} exists=false'.format(parent, name))
    return False


def count_rows(plex_db_path, log_path=''):
    diagnostic_log(log_path, 'count_rows:start')
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
    rows = KodisPlexDBHandle.select(query, plex_db_path)
    diagnostic_log(log_path, 'count_rows:done rows={}'.format(len(rows or [])))
    return int(rows[0]['COUNT(*)'] or 0) if rows else 0


def fetch_metadata_map(conn, metadata_ids, log_path=''):
    if not metadata_ids:
        return {}
    placeholders = ','.join(['?'] * len(metadata_ids))
    query = f'''
    SELECT id, parent_id, COALESCE(user_thumb_url, '') AS user_thumb_url, COALESCE(user_art_url, '') AS user_art_url
    FROM metadata_items
    WHERE id IN ({placeholders})
    '''
    rows = KodisPlexDBHandle.select_arg(query, tuple(metadata_ids), conn)
    result = {}
    for row in rows:
        result[int(row['id'])] = {
            'parent_id': int(row['parent_id'] or 0),
            'user_thumb_url': row['user_thumb_url'] or '',
            'user_art_url': row['user_art_url'] or '',
        }
    return result


def fetch_media_item_map(plex_db_path, media_item_ids, log_path=''):
    if not media_item_ids:
        return {}
    placeholders = ','.join(['?'] * len(media_item_ids))
    query = f'''
    SELECT id, metadata_item_id
    FROM media_items
    WHERE id IN ({placeholders})
    '''
    rows = KodisPlexDBHandle.select_arg(query, tuple(media_item_ids), plex_db_path)
    result = {}
    for row in rows:
        result[int(row['id'])] = int(row['metadata_item_id'] or 0)
    return result


def iter_rows(plex_db_path, log_path=''):
    part_query = '''
    SELECT mp.id AS part_id, mp.file AS file_path, mp.media_item_id
    FROM media_parts mp
    WHERE mp.id > ?
      AND (
        mp.file LIKE '%/VIDEO/%'
        OR mp.file LIKE 'VIDEO/%'
      )
    ORDER BY mp.id
    LIMIT ?
    '''
    batch_size = 3000
    last_id = 0
    while True:
        rows = KodisPlexDBHandle.select_arg(part_query, (last_id, batch_size), plex_db_path)
        if not rows:
            break
        diagnostic_log(log_path, 'iter_rows:batch rows={} last_id={}'.format(len(rows or []), last_id))
        media_item_ids = sorted({int(row['media_item_id'] or 0) for row in rows if row.get('media_item_id') is not None})
        media_item_map = fetch_media_item_map(plex_db_path, media_item_ids, log_path)
        metadata_ids = sorted({metadata_id for metadata_id in media_item_map.values() if metadata_id})
        metadata_map = fetch_metadata_map(plex_db_path, metadata_ids, log_path)
        parent_ids = sorted({meta['parent_id'] for meta in metadata_map.values() if meta.get('parent_id')})
        parent_map = fetch_metadata_map(plex_db_path, parent_ids, log_path)
        for row in rows:
            part_id = int(row['part_id'] or 0)
            file_path = row['file_path'] or ''
            if str(file_path).strip() == '':
                last_id = part_id
                continue
            if not is_video_library_path(file_path):
                last_id = part_id
                continue
            normalized_rel = normalize_plex_file_to_rel(file_path)
            if normalized_rel == '':
                last_id = part_id
                continue
            media_item_id = int(row['media_item_id'] or 0)
            metadata_id = int(media_item_map.get(media_item_id) or 0)
            last_id = part_id
            meta = metadata_map.get(metadata_id, {})
            parent = parent_map.get(meta.get('parent_id') or 0, {})
            item_thumb_url = meta.get('user_thumb_url', '')
            item_art_url = meta.get('user_art_url', '')
            parent_thumb_url = parent.get('user_thumb_url', '')
            parent_art_url = parent.get('user_art_url', '')
            yield {
                'part_id': part_id,
                'file_path': file_path,
                'item_thumb_url': item_thumb_url,
                'item_art_url': item_art_url,
                'parent_thumb_url': parent_thumb_url,
                'parent_art_url': parent_art_url,
            }


def count_cleanup_rows(metadata_db_path):
    with db_connect(metadata_db_path) as conn:
        row = conn.execute('SELECT COUNT(*) FROM plex_art_item').fetchone()
    return int(row[0] or 0) if row else 0


def iter_cleanup_rows(metadata_db_path):
    with db_connect(metadata_db_path) as conn:
        rows = conn.execute(
            '''
            SELECT file_path, series_path
            FROM plex_art_item
            ORDER BY updated_at DESC, rowid DESC
            '''
        ).fetchall()
    for row in rows:
        yield {
            'file_path': row[0] or '',
            'series_path': row[1] or '',
        }


def normalize_cleanup_scope(cleanup_path, gds_root):
    raw = str(cleanup_path or '').replace('\\', '/').strip()
    if raw == '':
        return ''
    root = os.path.abspath(gds_root or '')
    target = os.path.abspath(raw)
    if root:
        try:
            if os.path.commonpath([root, target]) == root:
                rel = os.path.relpath(target, root).replace('\\', '/').strip('/')
                if rel.upper().startswith('VIDEO/'):
                    rel = rel[6:].strip('/')
                return rel
        except Exception:
            pass
    rel = raw.strip('/').replace('\\', '/')
    if rel.upper().startswith('VIDEO/'):
        rel = rel[6:].strip('/')
    return rel


def normalize_cleanup_scopes(cleanup_path, gds_root):
    scopes = []
    for chunk in str(cleanup_path or '').replace('\r', '\n').split('\n'):
        normalized = normalize_cleanup_scope(chunk, gds_root)
        if normalized:
            scopes.append(normalized)
    return scopes


def looks_like_title_year(name):
    text = str(name or '').strip()
    return bool(re.search(r'\(\d{4}\)', text) or re.search(r'\{tmdb-\d+\}', text, re.IGNORECASE))


def normalize_dir_to_rel(directory_path, gds_root):
    target = os.path.abspath(directory_path)
    root = os.path.abspath(gds_root or '')
    if not root:
        return ''
    try:
        if os.path.commonpath([root, target]) == root:
            rel = os.path.relpath(target, root).replace('\\', '/').strip('/')
            if rel.upper().startswith('VIDEO/'):
                rel = rel[6:].strip('/')
            if rel.upper() == 'VIDEO':
                rel = ''
            return rel
    except Exception:
        return ''
    return ''


def split_exclude_rules(raw_text):
    rules = []
    for chunk in re.split(r'[\r\n,;]+', str(raw_text or '')):
        normalized = chunk.strip().replace('\\', '/').strip('/')
        if normalized:
            rules.append(normalized)
    return rules


def normalize_for_match(path_text):
    return str(path_text or '').replace('\\', '/').strip().strip('/').lower()


def filter_exclude_rules_for_scan_root(exclude_rules, scan_root_path, gds_root):
    normalized_scan_root_path = normalize_for_match(scan_root_path)
    normalized_scan_root_rel = normalize_for_match(normalize_dir_to_rel(scan_root_path, gds_root))
    filtered = []
    for rule in exclude_rules or []:
        normalized_rule = normalize_for_match(rule)
        if not normalized_rule:
            continue
        if normalized_scan_root_path and (
            normalized_scan_root_path == normalized_rule or
            normalized_scan_root_path.startswith(normalized_rule + '/')
        ):
            continue
        if normalized_scan_root_rel and (
            normalized_scan_root_rel == normalized_rule or
            normalized_scan_root_rel.startswith(normalized_rule + '/')
        ):
            continue
        filtered.append(rule)
    return filtered


def should_exclude_path(path_text, rel_path, name, exclude_rules):
    normalized_path = normalize_for_match(path_text)
    normalized_rel = normalize_for_match(rel_path)
    normalized_name = normalize_for_match(name)
    for rule in exclude_rules:
        normalized_rule = normalize_for_match(rule)
        if not normalized_rule:
            continue
        if '/' in normalized_rule:
            if normalized_rel == normalized_rule or normalized_rel.startswith(normalized_rule + '/'):
                return rule
            if normalized_path.endswith('/' + normalized_rule) or ('/' + normalized_rule + '/') in ('/' + normalized_path + '/'):
                return rule
        else:
            if normalized_name == normalized_rule:
                return rule
    return ''


def collect_scan_candidates(scan_root_path, gds_root, metadata_db_path='', log_path='', exclude_rules=None):
    exclude_rules = exclude_rules or []
    result = []
    visited = 0
    for current_root, dirnames, _filenames in os.walk(scan_root_path, topdown=True):
        visited += 1
        if visited == 1 or visited % 100 == 0:
            write_state(
                metadata_db_path,
                running=True,
                finished=False,
                stop_requested=False,
                total=0,
                processed=0,
                imported=0,
                percent=0,
                message='Discovering candidate folders...',
                current_path=current_root,
                error='',
            )
            diagnostic_log(log_path, 'index_scan:discover current_root={}'.format(current_root))
        state = read_state(metadata_db_path)
        if state.get('stop_requested'):
            break
        kept_dirnames = []
        for dirname in dirnames:
            fullpath = os.path.join(current_root, dirname)
            rel = normalize_dir_to_rel(fullpath, gds_root)
            if is_av_relative_path(rel):
                diagnostic_log(log_path, 'index_scan:excluded builtin=AV path={}'.format(fullpath))
                continue
            excluded_by = should_exclude_path(fullpath, rel, dirname, exclude_rules)
            if excluded_by:
                diagnostic_log(log_path, 'index_scan:excluded rule={} path={}'.format(excluded_by, fullpath))
                continue
            if looks_like_title_year(dirname):
                if rel:
                    result.append((fullpath, rel))
                    diagnostic_log(log_path, 'index_scan:candidate path={}'.format(fullpath))
                continue
            kept_dirnames.append(dirname)
        dirnames[:] = kept_dirnames
    return result


def exists_series_entry(conn, rel):
    row = conn.execute(
        '''
        SELECT 1
        FROM plex_art_item
        WHERE file_path = ? OR series_path = ?
        LIMIT 1
        ''',
        (rel, rel),
    ).fetchone()
    return row is not None


def exists_file_entry(conn, rel):
    row = conn.execute(
        '''
        SELECT 1
        FROM plex_art_item
        WHERE file_path = ?
        LIMIT 1
        ''',
        (rel,),
    ).fetchone()
    return row is not None


def load_existing_paths(conn):
    existing_file_paths = set()
    existing_series_paths = set()
    rows = conn.execute(
        '''
        SELECT file_path, series_path
        FROM plex_art_item
        '''
    ).fetchall()
    for file_path, series_path in rows:
        if file_path:
            existing_file_paths.add(file_path)
        if series_path:
            existing_series_paths.add(series_path)
    return existing_file_paths, existing_series_paths


def resolve_cleanup_scope_targets(cleanup_path, gds_root):
    targets = []
    root = os.path.abspath(gds_root or '')
    for chunk in str(cleanup_path or '').replace('\r', '\n').split('\n'):
        raw = str(chunk or '').strip()
        if raw == '':
            continue
        target = os.path.abspath(raw)
        if root:
            try:
                if os.path.commonpath([root, target]) == root:
                    targets.append(target)
                    continue
            except Exception:
                pass
        normalized = normalize_cleanup_scope(raw, gds_root)
        if normalized and root:
            targets.append(os.path.abspath(os.path.join(root, normalized.replace('/', os.sep))))
    return list(dict.fromkeys(targets))


def iter_library_dir_candidates(relative_path, gds_root):
    rel = str(relative_path or '').replace('\\', '/').strip().strip('/')
    if rel == '' or gds_root == '':
        return []
    root = os.path.abspath(gds_root)
    return list(dict.fromkeys([
        os.path.abspath(os.path.join(root, rel.replace('/', os.sep))),
        os.path.abspath(os.path.join(root, 'VIDEO', rel.replace('/', os.sep))),
    ]))


def is_cleanup_row_in_scope(rel, cleanup_scopes, cleanup_scope_targets, gds_root):
    normalized_rel = str(rel or '').replace('\\', '/').strip().strip('/')
    if normalized_rel == '':
        return False
    if cleanup_scope_targets:
        for candidate in iter_library_dir_candidates(normalized_rel, gds_root):
            for scope_target in cleanup_scope_targets:
                try:
                    if os.path.commonpath([scope_target, candidate]) == scope_target:
                        return True
                except Exception:
                    continue
        return False
    if not cleanup_scopes:
        return True
    for cleanup_scope in cleanup_scopes:
        if normalized_rel == cleanup_scope or normalized_rel.startswith(cleanup_scope + '/'):
            return True
    return False


def run_cleanup(metadata_db_path, gds_root, log_path, item_log_path='', cleanup_path=''):
    cleanup_scopes = normalize_cleanup_scopes(cleanup_path, gds_root)
    cleanup_scope_targets = resolve_cleanup_scope_targets(cleanup_path, gds_root)
    diagnostic_log(log_path, 'cleanup:start metadata_db_path={} gds_root={} cleanup_scope={}'.format(metadata_db_path, gds_root, ' | '.join(cleanup_scopes) if cleanup_scopes else '/'))
    cleanup_rows = []
    for row in iter_cleanup_rows(metadata_db_path):
        rel = (row.get('series_path') or row.get('file_path') or '').replace('\\', '/').strip().strip('/')
        if is_cleanup_row_in_scope(rel, cleanup_scopes, cleanup_scope_targets, gds_root):
            cleanup_rows.append({
                'file_path': row.get('file_path') or '',
                'series_path': row.get('series_path') or '',
                'rel': rel,
            })
    total = len(cleanup_rows)
    diagnostic_log(log_path, 'cleanup:count_rows total={}'.format(total))
    write_state(metadata_db_path, total=total, message='Scanning cleanup scope...', percent=0)
    removed = 0
    processed = 0
    with db_connect(metadata_db_path) as meta_conn:
        meta_conn.execute('DELETE FROM plex_cleanup_deleted')
        meta_conn.commit()
        for row in cleanup_rows:
            state = read_state(metadata_db_path)
            if state.get('stop_requested'):
                meta_conn.commit()
                write_state(
                    metadata_db_path,
                    conn=meta_conn,
                    running=False,
                    finished=True,
                    message='Cleanup stopped',
                    worker_pid=0,
                    percent=int(processed * 100 / total) if total > 0 else 0,
                    ended_at=int(time.time()),
                )
                return
            processed += 1
            rel = row.get('rel') or ''
            if rel == '':
                continue
            if resolve_library_dir(rel, gds_root, log_path):
                if processed % 100 == 0 or processed == total:
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=removed,
                        current_path=rel,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Cleanup scanning...',
                    )
                continue
            meta_conn.execute('DELETE FROM plex_art_item WHERE file_path = ?', (row.get('file_path') or '',))
            meta_conn.execute(
                'INSERT INTO plex_cleanup_deleted (path, deleted_at) VALUES (?, ?)',
                (rel, int(time.time())),
            )
            removed += 1
            append_item_log(item_log_path, rel)
            diagnostic_log(log_path, 'cleanup:removed path={}'.format(rel))
            if processed % 25 == 0 or processed == total:
                meta_conn.commit()
                write_state(
                    metadata_db_path,
                    conn=meta_conn,
                    processed=processed,
                    imported=removed,
                    current_path=rel,
                    percent=int(processed * 100 / total) if total > 0 else 0,
                    message='Cleanup removing missing folders...',
                )
        meta_conn.commit()
    diagnostic_log(log_path, 'cleanup:complete processed={} removed={}'.format(processed, removed))
    write_state(
        metadata_db_path,
        running=False,
        finished=True,
        stop_requested=False,
        processed=processed,
        imported=removed,
        worker_pid=0,
        percent=100 if total > 0 else 0,
        message='Cleanup complete',
        ended_at=int(time.time()),
    )


def run_import(metadata_db_path, plex_db_path, gds_root, log_path, item_log_path=''):
    diagnostic_log(log_path, 'worker:start metadata_db_path={} plex_db_path={} gds_root={}'.format(metadata_db_path, plex_db_path, gds_root))
    prepared_db_path = None
    actual_plex_db_path = plex_db_path
    try:
        actual_plex_db_path, prepared_db_path = prepare_plex_db_source(plex_db_path, metadata_db_path, log_path)
        if not actual_plex_db_path:
            return
        total = count_rows(actual_plex_db_path, log_path)
        diagnostic_log(log_path, 'worker:count_rows total={}'.format(total))
        write_state(metadata_db_path, total=total, message='Scanning Plex DB...', percent=0)
        write_state(metadata_db_path, message='Opening Plex row cursor...', current_path='', percent=0)
        imported = 0
        skipped = 0
        processed = 0
        filtered_scope = 0
        filtered_title_year = 0
        folder_only = 0
        imported_series = 0
        skipped_series = 0
        imported_episode = 0
        skipped_episode = 0
        now_ts = int(time.time())
        batch_marker = 0
        seen_series_paths = set()
        cleaned_series_paths = set()
        pruned_series_paths = set()
        last_series_path = ''
        with db_connect(metadata_db_path) as meta_conn:
            existing_file_paths, existing_series_paths = load_existing_paths(meta_conn)
            state_check_interval = 1000
            progress_commit_interval = 1000
            for row in iter_rows(actual_plex_db_path, log_path):
                current_part_id = int(row['part_id'] or 0)
                if current_part_id - batch_marker >= 1000:
                    batch_marker = current_part_id
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        message='Streaming Plex rows... last_part_id={}'.format(current_part_id),
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        processed=processed,
                        imported=imported,
                        skipped=skipped,
                    )
                if processed == 0:
                    write_state(metadata_db_path, conn=meta_conn, message='Streaming Plex rows...', percent=0)
                if processed > 0 and processed % state_check_interval == 0:
                    state = read_state(metadata_db_path)
                else:
                    state = {}
                if state.get('stop_requested'):
                    meta_conn.commit()
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        running=False,
                        finished=True,
                        message='Import stopped',
                        worker_pid=0,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        ended_at=int(time.time()),
                    )
                    return
                processed += 1
                rel = normalize_plex_file_to_rel(row['file_path'] or '')
                if not rel or not is_valid_import_scope(rel, gds_root):
                    filtered_scope += 1
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=imported,
                        skipped=skipped,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Filtering items...',
                    )
                    continue
                series_path = extract_title_year_series_path(rel)
                if not series_path:
                    filtered_title_year += 1
                    diagnostic_log(log_path, 'worker:skip_non_title_year rel={}'.format(rel))
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=imported,
                        skipped=skipped,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Filtering items...',
                    )
                    continue
                if series_path not in cleaned_series_paths:
                    descendant_file_paths = {
                        path for path in existing_file_paths
                        if path != series_path and path.startswith(series_path + '/')
                    }
                    descendant_series_paths = {
                        path for path in existing_series_paths
                        if path != series_path and path.startswith(series_path + '/')
                    }
                    if descendant_file_paths or descendant_series_paths:
                        meta_conn.execute(
                            '''
                            DELETE FROM plex_art_item
                            WHERE (series_path LIKE ? OR file_path LIKE ?)
                              AND series_path != ?
                            ''',
                            (series_path + '/%', series_path + '/%', series_path),
                        )
                        existing_file_paths.difference_update(descendant_file_paths)
                        existing_series_paths.difference_update(descendant_series_paths)
                        pruned_series_paths.add(series_path)
                    cleaned_series_paths.add(series_path)
                item_thumb = row['item_thumb_url'] or ''
                item_art = row['item_art_url'] or ''
                parent_thumb = row['parent_thumb_url'] or ''
                parent_art = row['parent_art_url'] or ''
                episode_thumb_url = item_thumb if is_http_url(item_thumb) else ''
                episode_poster_url = item_art if is_http_url(item_art) else ''
                poster_url = ''
                folder_thumb_url = ''
                for candidate in (parent_thumb, parent_art, item_art, item_thumb):
                    if is_http_url(candidate):
                        folder_thumb_url = candidate
                        break
                for candidate in (item_art, parent_art, parent_thumb, item_thumb):
                    if is_http_url(candidate):
                        poster_url = candidate
                        break
                last_series_path = series_path
                if series_path not in seen_series_paths:
                    seen_series_paths.add(series_path)
                    if series_path in existing_file_paths:
                        skipped += 1
                        skipped_series += 1
                    else:
                        meta_conn.execute(
                            '''
                            INSERT INTO plex_art_item (file_path, series_path, poster_url, thumb_url, updated_at)
                            VALUES (?, ?, ?, ?, ?)
                            ''',
                            (series_path, series_path, poster_url, folder_thumb_url or poster_url, now_ts),
                        )
                        imported += 1
                        imported_series += 1
                        existing_file_paths.add(series_path)
                        existing_series_paths.add(series_path)
                        append_item_log(item_log_path, series_path)

                if not episode_thumb_url and not episode_poster_url:
                    folder_only += 1
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=imported,
                        skipped=skipped,
                        current_path=series_path,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Importing folder metadata...',
                    )
                    continue

                if rel in existing_file_paths:
                    skipped += 1
                    skipped_episode += 1
                    continue

                meta_conn.execute(
                    '''
                    INSERT INTO plex_art_item (file_path, series_path, poster_url, thumb_url, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (rel, series_path, episode_poster_url, episode_thumb_url or episode_poster_url, now_ts),
                )
                imported += 1
                imported_episode += 1
                existing_file_paths.add(rel)
                append_item_log(item_log_path, rel)
                if processed % progress_commit_interval == 0:
                    diagnostic_log(log_path, 'worker:commit processed={} imported={} skipped={}'.format(processed, imported, skipped))
                    meta_conn.commit()
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=imported,
                        skipped=skipped,
                        current_path=last_series_path,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Importing Plex metadata...',
                    )
            meta_conn.commit()
        diagnostic_log(
            log_path,
            'worker:complete processed={} imported={} skipped={} filtered_scope={} filtered_title_year={} folder_only={} imported_series={} skipped_series={} imported_episode={} skipped_episode={}'.format(
                processed,
                imported,
                skipped,
                filtered_scope,
                filtered_title_year,
                folder_only,
                imported_series,
                skipped_series,
                imported_episode,
                skipped_episode,
            )
        )
        if pruned_series_paths:
            diagnostic_log(log_path, 'worker:pruned_descendants count={}'.format(len(pruned_series_paths)))
        write_state(
            metadata_db_path,
            running=False,
            finished=True,
            stop_requested=False,
            processed=processed,
            imported=imported,
            skipped=skipped,
            worker_pid=0,
            percent=100 if total > 0 else 0,
            message='Import complete',
            ended_at=int(time.time()),
        )
    finally:
        if prepared_db_path and os.path.exists(prepared_db_path):
            try:
                os.remove(prepared_db_path)
            except Exception:
                pass


def run_scan(metadata_db_path, scan_root_path, gds_root, log_path, item_log_path='', exclude_text=''):
    exclude_rules = split_exclude_rules(exclude_text)
    exclude_rules = filter_exclude_rules_for_scan_root(exclude_rules, scan_root_path, gds_root)
    diagnostic_log(log_path, 'index_scan:start scan_root={} gds_root={}'.format(scan_root_path, gds_root))
    if exclude_rules:
        diagnostic_log(log_path, 'index_scan:exclude_rules {}'.format(' | '.join(exclude_rules)))
    write_state(
        metadata_db_path,
        running=True,
        finished=False,
        stop_requested=False,
        total=0,
        processed=0,
        imported=0,
        percent=0,
        message='Discovering candidate folders...',
        current_path=scan_root_path,
        error='',
        started_at=int(time.time()),
        ended_at=0,
    )
    candidates = collect_scan_candidates(scan_root_path, gds_root, metadata_db_path, log_path, exclude_rules)
    total = len(candidates)
    diagnostic_log(log_path, 'index_scan:candidates total={}'.format(total))
    write_state(
        metadata_db_path,
        running=True,
        finished=False,
        stop_requested=False,
        total=total,
        processed=0,
        imported=0,
        percent=0,
        message='Scanning index candidates...',
        current_path='',
        error='',
        started_at=int(time.time()),
        ended_at=0,
    )
    imported = 0
    processed = 0
    with db_connect(metadata_db_path) as conn:
        for _fullpath, rel in candidates:
            state = read_state(metadata_db_path)
            if state.get('stop_requested'):
                write_state(
                    metadata_db_path,
                    conn=conn,
                    running=False,
                    finished=True,
                    message='Index scan stopped',
                    worker_pid=0,
                    percent=int(processed * 100 / total) if total > 0 else 0,
                    ended_at=int(time.time()),
                )
                return
            processed += 1
            if not exists_series_entry(conn, rel):
                now_ts = int(time.time())
                conn.execute(
                    '''
                    INSERT INTO plex_art_item (file_path, series_path, poster_url, thumb_url, updated_at)
                    VALUES (?, ?, '', '', ?)
                    ''',
                    (rel, rel, now_ts),
                )
                imported += 1
                append_item_log(item_log_path, rel)
            if processed % 25 == 0 or processed == total:
                write_state(
                    metadata_db_path,
                    conn=conn,
                    processed=processed,
                    imported=imported,
                    current_path=rel,
                    percent=int(processed * 100 / total) if total > 0 else 0,
                    message='Index scanning...',
                )
        write_state(
            metadata_db_path,
            conn=conn,
            running=False,
            finished=True,
            stop_requested=False,
            processed=processed,
            imported=imported,
            worker_pid=0,
            percent=100 if total > 0 else 0,
            message='Index scan complete',
            ended_at=int(time.time()),
        )
    diagnostic_log(log_path, 'index_scan:complete processed={} imported={}'.format(processed, imported))


def main():
    metadata_db_path = sys.argv[1]
    source_path = sys.argv[2]
    gds_root = os.path.abspath(sys.argv[3]) if len(sys.argv) > 3 else ''
    log_path = sys.argv[4] if len(sys.argv) > 4 else ''
    item_log_path = sys.argv[5] if len(sys.argv) > 5 else ''
    mode = sys.argv[6] if len(sys.argv) > 6 else 'import'
    extra_arg = sys.argv[7] if len(sys.argv) > 7 else ''
    ensure_tables(metadata_db_path)
    ensure_state_row(metadata_db_path)
    try:
        write_state(metadata_db_path, worker_pid=os.getpid())
        if mode == 'cleanup':
            run_cleanup(metadata_db_path, gds_root, log_path, item_log_path, extra_arg)
        elif mode == 'db_import':
            run_db_import(metadata_db_path, source_path, log_path, extra_arg)
        elif mode == 'vacuum':
            run_vacuum(metadata_db_path, log_path)
        elif mode == 'scan':
            run_scan(metadata_db_path, os.path.abspath(source_path), gds_root, log_path, item_log_path, extra_arg)
        else:
            run_import(metadata_db_path, source_path, gds_root, log_path, item_log_path)
    except Exception as e:
        diagnostic_log(log_path, 'worker:exception {}'.format(str(e)))
        diagnostic_log(log_path, traceback.format_exc())
        write_state(
            metadata_db_path,
            running=False,
            finished=True,
            error='{}\\n{}'.format(str(e), traceback.format_exc()),
            message='DB worker failed',
            worker_pid=0,
            ended_at=int(time.time()),
        )


if __name__ == '__main__':
    main()
 
