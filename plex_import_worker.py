import os
import sqlite3
import sys
import time
import traceback

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


def ensure_state_row(metadata_db_path):
    with db_connect(metadata_db_path) as conn:
        conn.execute(
            '''
            INSERT OR IGNORE INTO plex_import_state (
                id, running, finished, stop_requested, total, processed, imported,
                percent, message, current_path, error, started_at, ended_at
            ) VALUES (1, 0, 0, 0, 0, 0, 0, 0, '', '', '', 0, 0)
            '''
        )
        conn.commit()


def read_state(metadata_db_path):
    try:
        with db_connect(metadata_db_path, timeout=0.2) as conn:
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
        return {}
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


def write_state(metadata_db_path, conn=None, **kwargs):
    current = read_state(metadata_db_path)
    current.update(kwargs)
    sql = '''
    UPDATE plex_import_state
    SET running = ?, finished = ?, stop_requested = ?, total = ?, processed = ?,
        imported = ?, percent = ?, message = ?, current_path = ?, error = ?, started_at = ?, ended_at = ?
    WHERE id = 1
    '''
    params = (
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
        return normalized[idx + len(marker):].strip('/')
    if normalized.startswith('VIDEO/'):
        return normalized[6:].strip('/')
    return ''


def is_video_library_path(file_path):
    normalized = str(file_path or '').replace('\\', '/').strip().lower()
    return '/video/' in normalized or normalized.startswith('video/')


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
    if gds_root == '':
        return False
    root = os.path.abspath(gds_root)
    candidates = [
        os.path.join(root, scope.replace('/', os.sep)),
        os.path.join(root, 'VIDEO', scope.replace('/', os.sep)),
    ]
    for candidate in candidates:
        if os.path.isdir(candidate):
            return True
    return False


def count_rows(plex_db_path, log_path=''):
    diagnostic_log(log_path, 'count_rows:start')
    query = '''
    SELECT COUNT(*)
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
    rows = KodisPlexDBHandle.select(query, plex_db_path)
    diagnostic_log(log_path, 'count_rows:done rows={}'.format(len(rows or [])))
    return int(rows[0]['COUNT(*)'] or 0) if rows else 0


def fetch_metadata_map(conn, metadata_ids, log_path=''):
    if not metadata_ids:
        return {}
    diagnostic_log(log_path, 'fetch_metadata_map:start ids={}'.format(len(metadata_ids)))
    placeholders = ','.join(['?'] * len(metadata_ids))
    query = f'''
    SELECT id, parent_id, COALESCE(user_thumb_url, '') AS user_thumb_url, COALESCE(user_art_url, '') AS user_art_url
    FROM metadata_items
    WHERE id IN ({placeholders})
    '''
    rows = KodisPlexDBHandle.select_arg(query, tuple(metadata_ids), conn)
    diagnostic_log(log_path, 'fetch_metadata_map:done rows={}'.format(len(rows or [])))
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
    diagnostic_log(log_path, 'fetch_media_item_map:start ids={}'.format(len(media_item_ids)))
    placeholders = ','.join(['?'] * len(media_item_ids))
    query = f'''
    SELECT id, metadata_item_id
    FROM media_items
    WHERE id IN ({placeholders})
    '''
    rows = KodisPlexDBHandle.select_arg(query, tuple(media_item_ids), plex_db_path)
    diagnostic_log(log_path, 'fetch_media_item_map:done rows={}'.format(len(rows or [])))
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
    batch_size = 1000
    last_id = 0
    while True:
        diagnostic_log(log_path, 'iter_rows:batch_query:start last_id={} batch_size={}'.format(last_id, batch_size))
        rows = KodisPlexDBHandle.select_arg(part_query, (last_id, batch_size), plex_db_path)
        diagnostic_log(log_path, 'iter_rows:batch_query:done row_count={}'.format(len(rows or [])))
        if not rows:
            break
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
            if not (
                is_http_url(item_thumb_url)
                or is_http_url(item_art_url)
                or is_http_url(parent_thumb_url)
                or is_http_url(parent_art_url)
            ):
                continue
            yield {
                'part_id': part_id,
                'file_path': file_path,
                'item_thumb_url': item_thumb_url,
                'item_art_url': item_art_url,
                'parent_thumb_url': parent_thumb_url,
                'parent_art_url': parent_art_url,
            }


def main():
    metadata_db_path = sys.argv[1]
    plex_db_path = sys.argv[2]
    gds_root = os.path.abspath(sys.argv[3]) if len(sys.argv) > 3 else ''
    log_path = sys.argv[4] if len(sys.argv) > 4 else ''
    ensure_tables(metadata_db_path)
    ensure_state_row(metadata_db_path)
    try:
        diagnostic_log(log_path, 'worker:start metadata_db_path={} plex_db_path={} gds_root={}'.format(metadata_db_path, plex_db_path, gds_root))
        total = count_rows(plex_db_path, log_path)
        diagnostic_log(log_path, 'worker:count_rows total={}'.format(total))
        write_state(metadata_db_path, total=total, message='Scanning Plex DB...', percent=0)
        write_state(metadata_db_path, message='Opening Plex row cursor...', current_path='', percent=0)
        imported = 0
        processed = 0
        now_ts = int(time.time())
        batch_marker = 0
        with db_connect(metadata_db_path) as meta_conn:
            diagnostic_log(log_path, 'worker:delete_old_rows:start')
            meta_conn.execute('DELETE FROM plex_art_item')
            diagnostic_log(log_path, 'worker:delete_old_rows:done')
            for row in iter_rows(plex_db_path, log_path):
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
                    )
                if processed == 0:
                    write_state(metadata_db_path, conn=meta_conn, message='Streaming Plex rows...', percent=0)
                state = read_state(metadata_db_path)
                if state.get('stop_requested'):
                    meta_conn.commit()
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        running=False,
                        finished=True,
                        message='Import stopped',
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        ended_at=int(time.time()),
                    )
                    return
                processed += 1
                rel = normalize_plex_file_to_rel(row['file_path'] or '')
                if not rel or not is_valid_import_scope(rel, gds_root):
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=imported,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Filtering items...',
                    )
                    continue
                item_thumb = row['item_thumb_url'] or ''
                item_art = row['item_art_url'] or ''
                parent_thumb = row['parent_thumb_url'] or ''
                parent_art = row['parent_art_url'] or ''
                thumb_url = item_thumb if is_http_url(item_thumb) else ''
                poster_url = ''
                for candidate in (item_art, parent_art, parent_thumb, item_thumb):
                    if is_http_url(candidate):
                        poster_url = candidate
                        break
                if not thumb_url and not poster_url:
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=imported,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Filtering items...',
                    )
                    continue
                series_path = os.path.dirname(rel).replace('\\', '/')
                meta_conn.execute(
                    '''
                    INSERT INTO plex_art_item (file_path, series_path, poster_url, thumb_url, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(file_path) DO UPDATE SET
                        series_path = excluded.series_path,
                        poster_url = excluded.poster_url,
                        thumb_url = excluded.thumb_url,
                        updated_at = excluded.updated_at
                    ''',
                    (rel, series_path, poster_url, thumb_url, now_ts),
                )
                imported += 1
                if processed % 500 == 0:
                    diagnostic_log(log_path, 'worker:commit processed={} imported={}'.format(processed, imported))
                    meta_conn.commit()
                    write_state(
                        metadata_db_path,
                        conn=meta_conn,
                        processed=processed,
                        imported=imported,
                        current_path=rel,
                        percent=int(processed * 100 / total) if total > 0 else 0,
                        message='Importing Plex metadata...',
                    )
            meta_conn.commit()
        diagnostic_log(log_path, 'worker:complete processed={} imported={}'.format(processed, imported))
        write_state(
            metadata_db_path,
            running=False,
            finished=True,
            stop_requested=False,
            processed=processed,
            imported=imported,
            percent=100 if total > 0 else 0,
            message='Import complete',
            ended_at=int(time.time()),
        )
    except Exception as e:
        diagnostic_log(log_path, 'worker:exception {}'.format(str(e)))
        diagnostic_log(log_path, traceback.format_exc())
        write_state(
            metadata_db_path,
            running=False,
            finished=True,
            error='{}\\n{}'.format(str(e), traceback.format_exc()),
            message='Import failed',
            ended_at=int(time.time()),
        )


if __name__ == '__main__':
    main()
