import hashlib
import json
import mimetypes
import os
import re
import secrets
import sqlite3
import subprocess
import threading
import time
import traceback
from queue import Queue
from datetime import datetime
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from flask import Response, abort, jsonify, redirect, render_template, request, send_file

from .setup import *  # pylint: disable=wildcard-import,unused-wildcard-import
from .kodis_db import PlexImportMixin
from .kodis_metadata import KodisMetadataMixin

class KodisPlayMixin(KodisMetadataMixin, PlexImportMixin, object):
    recent_virtual_path = '__recent__'
    hls_jobs = {}
    hls_lock = threading.Lock()
    auth_sessions = {}
    auth_lock = threading.Lock()
    encoder_probe_cache = {}
    vod_jobs = {}
    vod_lock = threading.Lock()
    metadata_lookup_lock = threading.Lock()
    metadata_lookup_inflight = set()
    metadata_lookup_semaphore = threading.BoundedSemaphore(2)
    auto_meta_worker_lock = threading.Lock()
    auto_meta_worker_started = False
    auto_meta_base_url_lock = threading.Lock()
    auto_meta_base_url = ''
    auto_meta_poll_interval = 60
    segment_duration = 2
    vod_prefetch_count = 4
    vod_initial_segments = 3
    auth_session_ttl = 60 * 60 * 12
    resolution_map = {
        '1080p': {'width': 1920, 'height': 1080, 'bitrate': '6M', 'maxrate': '7M', 'bufsize': '12M'},
        '720p': {'width': 1280, 'height': 720, 'bitrate': '4M', 'maxrate': '5M', 'bufsize': '8M'},
        '480p': {'width': 854, 'height': 480, 'bitrate': '2M', 'maxrate': '2500k', 'bufsize': '4M'},
    }
    quality_preset_map = {
        'fast': {'cpu': 'veryfast', 'nvenc': 'fast'},
        'medium': {'cpu': 'medium', 'nvenc': 'medium'},
        'high': {'cpu': 'slow', 'nvenc': 'slow'},
    }
    transparent_gif = (
        b'GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!'
        b'\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00'
        b'\x00\x02\x02D\x01\x00;'
    )

    db_default = {
        'ffmpeg_path': 'ffmpeg',
        'gds_path': '',
        'access_password': '',
        'plex_db_path': '',
        'transcode_codec': 'h264',
        'transcode_h264_encoder': '',
        'transcode_h265_encoder': '',
        'transcode_vaapi_device': '',
    }

    def __init__(self, *args, **kwargs):
        try:
            super(KodisPlayMixin, self).__init__(*args, **kwargs)
        except Exception:
            pass
        self.series_folder_probe_lock = threading.Lock()
        self.series_folder_probe_cache = {}

    def _diagnostic_log_path(self):
        log_dir = os.path.join(F.config['path_data'], 'log')
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, f'{P.package_name}_diag.log')

    def _diagnostic_log(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'[{timestamp}] {message}\n'
        try:
            with open(self._diagnostic_log_path(), 'a', encoding='utf-8') as fp:
                fp.write(line)
        except Exception:
            pass

    def _run_with_timeout(self, label, func, timeout_seconds=5):
        result_queue = Queue(maxsize=1)

        def worker():
            try:
                result_queue.put(('ok', func()))
            except Exception as e:
                result_queue.put(('error', e))

        thread = threading.Thread(target=worker, name=f'{P.package_name}_{label}', daemon=True)
        thread.start()
        thread.join(timeout_seconds)
        if thread.is_alive():
            self._diagnostic_log(f'timeout label={label} timeout={timeout_seconds}')
            raise Exception(f'{label} timeout after {timeout_seconds}s')
        status, payload = result_queue.get()
        if status == 'error':
            raise payload
        return payload

    def _remember_base_url(self, base):
        normalized = str(base or '').strip().rstrip('/')
        if not normalized:
            return ''
        with self.auto_meta_base_url_lock:
            self.auto_meta_base_url = normalized
        return normalized

    def _remember_base_url_from_req(self, req):
        try:
            return self._remember_base_url(req.url_root.rstrip('/'))
        except Exception:
            return ''

    def _auto_meta_now_string(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _ensure_auto_meta_state_table(self):
        with sqlite3.connect(self._metadata_db_path()) as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS auto_meta_state (
                    id INTEGER PRIMARY KEY,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    start_created_time TEXT NOT NULL DEFAULT '',
                    last_fp_item_id INTEGER NOT NULL DEFAULT 0,
                    last_processed_at INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT ''
                )
                '''
            )
            columns = [row[1] for row in conn.execute("PRAGMA table_info(auto_meta_state)").fetchall()]
            if 'last_processed_at' not in columns:
                conn.execute("ALTER TABLE auto_meta_state ADD COLUMN last_processed_at INTEGER NOT NULL DEFAULT 0")
            if 'last_error' not in columns:
                conn.execute("ALTER TABLE auto_meta_state ADD COLUMN last_error TEXT NOT NULL DEFAULT ''")
            conn.execute(
                '''
                INSERT OR IGNORE INTO auto_meta_state (
                    id, enabled, start_created_time, last_fp_item_id, last_processed_at, last_error
                ) VALUES (1, 1, '', 0, 0, '')
                '''
            )
            row = conn.execute(
                '''
                SELECT start_created_time
                FROM auto_meta_state
                WHERE id = 1
                '''
            ).fetchone()
            if row is None or not (row[0] or '').strip():
                conn.execute(
                    '''
                    UPDATE auto_meta_state
                    SET start_created_time = ?
                    WHERE id = 1
                    ''',
                    (self._auto_meta_now_string(),),
                )
            conn.commit()

    def _get_auto_meta_state(self):
        self._ensure_auto_meta_state_table()
        with sqlite3.connect(self._metadata_db_path()) as conn:
            row = conn.execute(
                '''
                SELECT enabled, start_created_time, last_fp_item_id, last_processed_at, last_error
                FROM auto_meta_state
                WHERE id = 1
                '''
            ).fetchone()
        return {
            'enabled': bool((row[0] if row else 1)),
            'start_created_time': (row[1] if row else '') or '',
            'last_fp_item_id': int((row[2] if row else 0) or 0),
            'last_processed_at': int((row[3] if row else 0) or 0),
            'last_error': (row[4] if row else '') or '',
        }

    def _update_auto_meta_state(self, **kwargs):
        current = self._get_auto_meta_state()
        current.update(kwargs)
        with sqlite3.connect(self._metadata_db_path()) as conn:
            conn.execute(
                '''
                UPDATE auto_meta_state
                SET enabled = ?, start_created_time = ?, last_fp_item_id = ?, last_processed_at = ?, last_error = ?
                WHERE id = 1
                ''',
                (
                    1 if current.get('enabled', True) else 0,
                    current.get('start_created_time') or self._auto_meta_now_string(),
                    int(current.get('last_fp_item_id') or 0),
                    int(current.get('last_processed_at') or 0),
                    current.get('last_error', '') or '',
                ),
            )
            conn.commit()

    def _start_auto_meta_worker(self):
        self._ensure_auto_meta_state_table()
        cls = type(self)
        with cls.auto_meta_worker_lock:
            if cls.auto_meta_worker_started:
                return
            worker = threading.Thread(
                target=self._auto_meta_worker_loop,
                name='ff_kodis_auto_meta',
                daemon=True,
            )
            worker.start()
            cls.auto_meta_worker_started = True
            P.logger.info('Auto metadata worker started')

    def _auto_meta_worker_loop(self):
        time.sleep(10)
        while True:
            try:
                self._run_auto_meta_cycle()
            except Exception as e:
                message = str(e)
                self._update_auto_meta_state(last_error=message)
                P.logger.warning(f'Auto metadata cycle failed: {message}')
            time.sleep(self.auto_meta_poll_interval)

    def _background_base_url(self):
        with self.auto_meta_base_url_lock:
            cached = (self.auto_meta_base_url or '').strip()
        if cached:
            return cached

        direct_candidates = []
        port_candidates = []
        for key in ('server_url', 'ddns', 'ddns_url', 'server_ddns'):
            try:
                value = F.SystemModelSetting.get(key)
            except Exception:
                value = ''
            if value:
                text = str(value).strip().rstrip('/')
                if text.startswith('http://') or text.startswith('https://'):
                    direct_candidates.append(text)
        for key in ('port', 'server_port', 'web_port'):
            try:
                value = F.SystemModelSetting.get(key)
            except Exception:
                value = ''
            if value:
                port_candidates.append(str(value).strip())
        for key in ('port', 'server_port', 'web_port'):
            try:
                value = F.config.get(key)
            except Exception:
                value = ''
            if value:
                port_candidates.append(str(value).strip())

        candidates = []
        for port in port_candidates:
            if port.isdigit():
                candidates.append(f'http://127.0.0.1:{port}')
                candidates.append(f'http://localhost:{port}')
        candidates.extend(direct_candidates)
        candidates.extend(['http://127.0.0.1:9990', 'http://localhost:9990'])
        for candidate in candidates:
            normalized = self._remember_base_url(candidate)
            if normalized:
                return normalized
        return ''

    def _gds_path_to_relative(self, gds_path):
        normalized = str(gds_path or '').replace('\\', '/').strip()
        prefix = '/ROOT/GDRIVE/'
        if normalized.upper().startswith(prefix.upper()):
            normalized = normalized[len(prefix):]
        normalized = normalized.strip('/')
        if normalized.upper().startswith('VIDEO/'):
            return normalized
        return ''

    def _auto_meta_target_rel(self, gds_path):
        relative_path = self._gds_path_to_relative(gds_path)
        if not relative_path:
            return ''
        try:
            _root, target = self._resolve_target_path(relative_path)
        except Exception:
            target = ''
        if target and os.path.isdir(target):
            return relative_path.strip('/')
        parent = os.path.dirname(relative_path).replace('\\', '/').strip('/')
        return parent

    def _iter_new_fp_items(self, last_fp_item_id, start_created_time, limit_count=50):
        db_path = self._gds_tool_db_path()
        if not os.path.exists(db_path):
            return []
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                '''
                SELECT id, created_time, gds_path
                FROM fp_item
                WHERE id > ?
                  AND created_time >= ?
                  AND gds_path LIKE '/ROOT/GDRIVE/VIDEO/%'
                  AND (scan_mode IS NULL OR UPPER(scan_mode) = 'ADD')
                ORDER BY id ASC
                LIMIT ?
                ''',
                (
                    int(last_fp_item_id or 0),
                    start_created_time or self._auto_meta_now_string(),
                    int(limit_count or 50),
                ),
            ).fetchall()
        return rows

    def _run_auto_meta_cycle(self):
        gds_root = os.path.abspath(P.ModelSetting.get('gds_path') or '')
        if not gds_root or not os.path.isdir(gds_root):
            return
        state = self._get_auto_meta_state()
        if not state.get('enabled', True):
            return
        base = self._background_base_url()
        if not base:
            return

        rows = self._iter_new_fp_items(state.get('last_fp_item_id', 0), state.get('start_created_time', ''))
        if not rows:
            return
        P.logger.info(
            'Auto metadata scan rows=%s start_created_time=%s last_fp_item_id=%s',
            len(rows),
            state.get('start_created_time', ''),
            state.get('last_fp_item_id', 0),
        )

        for fp_item_id, _created_time, gds_path in rows:
            try:
                target_rel = self._auto_meta_target_rel(gds_path)
                if not target_rel:
                    self._update_auto_meta_state(
                        last_fp_item_id=fp_item_id,
                        last_processed_at=int(time.time()),
                        last_error='',
                    )
                    continue
                _root, target = self._resolve_target_path(target_rel)
                if not os.path.isdir(target):
                    self._update_auto_meta_state(
                        last_fp_item_id=fp_item_id,
                        last_processed_at=int(time.time()),
                        last_error='',
                    )
                    continue
                should_lookup = (
                    self._should_attach_series_metadata(target_rel, target)
                    or self._is_probable_series_folder(target)
                )
                if should_lookup and not self._metadata_get_cached(target_rel):
                    P.logger.info('Auto metadata lookup id=%s path=%s', fp_item_id, target_rel)
                    result = self._lookup_metadata_cache_by_base(base, target_rel)
                    if result:
                        P.logger.info('Auto metadata cached id=%s path=%s', fp_item_id, target_rel)
                self._update_auto_meta_state(
                    last_fp_item_id=fp_item_id,
                    last_processed_at=int(time.time()),
                    last_error='',
                )
            except Exception as e:
                self._update_auto_meta_state(
                    last_fp_item_id=fp_item_id,
                    last_processed_at=int(time.time()),
                    last_error=str(e),
                )
                P.logger.warning(f'Auto metadata item failed id={fp_item_id} path={gds_path}: {str(e)}')

    def _process_kodis_command(self, command, req):
        req_path = getattr(req, 'path', '')
        req_args = dict(req.args) if hasattr(req, 'args') else {}
        req_form = dict(req.form) if hasattr(req, 'form') else {}
        self._diagnostic_log(f'process_command command={command} path={req_path} args={req_args} form={req_form}')
        P.logger.warning(
            'kodis_play _process_kodis_command command=%s path=%s args=%s form=%s',
            command,
            req_path,
            req_args,
            req_form,
        )
        if command in ('list', 'list_root'):
            self._diagnostic_log(f'process_command list dispatch command={command} gds_path={P.ModelSetting.get("gds_path")}')
            P.logger.warning('kodis_play handling list command=%s gds_path=%s', command, P.ModelSetting.get('gds_path'))
            return jsonify(self._list_items(req))
        if command == 'hls_status':
            return jsonify(self._hls_status(req))
        self._diagnostic_log(f'process_command unhandled command={command}')
        P.logger.warning('kodis_play unhandled command=%s', command)
        return None

    def _process_kodis_api(self, sub, req):
        req_path = getattr(req, 'path', '')
        req_args = dict(req.args) if hasattr(req, 'args') else {}
        req_form = dict(req.form) if hasattr(req, 'form') else {}
        self._diagnostic_log(f'process_api sub={sub} path={req_path} args={req_args} form={req_form}')
        if sub == 'list':
            self._diagnostic_log(f'process_api list dispatch gds_path={P.ModelSetting.get("gds_path")}')
            return jsonify(self._list_items(req))
        if sub == 'play':
            return self._play(req)
        if sub == 'directplay':
            return self._directplay(req)
        if sub == 'transcode_stream':
            return self._transcode_stream(req)
        if sub == 'meta_art':
            return self._serve_meta_art(req)
        if sub == 'subtitle':
            return self._serve_subtitle(req)
        if sub == 'resume_get':
            return jsonify(self._resume_get(req))
        if sub == 'resume_set':
            return jsonify(self._resume_set(req))
        if sub == 'vod':
            return self._serve_vod(req)
        if sub == 'hls' or sub.startswith('hls/'):
            return self._serve_hls(sub, req)
        abort(404)

    def _resume_db_path(self):
        db_dir = os.path.join(F.config['path_data'], 'db', P.package_name)
        os.makedirs(db_dir, exist_ok=True)
        return os.path.join(db_dir, 'resume.sqlite')

    def _ensure_resume_table(self):
        with sqlite3.connect(self._resume_db_path()) as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS resume_item (
                    client_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    position_seconds REAL NOT NULL DEFAULT 0,
                    duration REAL NOT NULL DEFAULT 0,
                    play_state TEXT NOT NULL DEFAULT '',
                    updated_at INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (client_id, path)
                )
                '''
            )
            columns = [row[1] for row in conn.execute("PRAGMA table_info(resume_item)").fetchall()]
            if 'play_state' not in columns:
                conn.execute("ALTER TABLE resume_item ADD COLUMN play_state TEXT NOT NULL DEFAULT ''")
            conn.commit()

    def _get_resume_state(self, client_id, path_value):
        path_value = self._normalize_resume_path(path_value)
        self._ensure_resume_table()
        with sqlite3.connect(self._resume_db_path()) as conn:
            row = conn.execute(
                '''
                SELECT position_seconds, duration, play_state, updated_at
                FROM resume_item
                WHERE client_id = ? AND path = ?
                ''',
                (client_id, path_value),
            ).fetchone()
        position_seconds = float(row[0]) if row else 0.0
        duration = float(row[1]) if row else 0.0
        play_state = (row[2] or '') if row else ''
        updated_at = int(row[3]) if row else 0
        watched = play_state == 'watched'
        in_progress = (not watched) and position_seconds > 0
        return {
            'client_id': client_id,
            'path': path_value,
            'position_seconds': 0.0 if watched else position_seconds,
            'duration': duration,
            'updated_at': updated_at,
            'watched': watched,
            'in_progress': in_progress,
            'play_state': play_state,
        }

    def _get_resume_state_map(self, client_id, path_values):
        normalized = []
        for path in path_values:
            normalized_path = self._normalize_resume_path(path)
            if normalized_path:
                normalized.append(normalized_path)
        if not normalized:
            return {}
        self._ensure_resume_table()
        placeholders = ','.join(['?'] * len(normalized))
        params = [client_id] + normalized
        query = (
            'SELECT path, position_seconds, duration, play_state, updated_at '
            'FROM resume_item WHERE client_id = ? AND path IN ({})'
        ).format(placeholders)
        result = {}
        with sqlite3.connect(self._resume_db_path()) as conn:
            rows = conn.execute(query, params).fetchall()
        for row in rows:
            path_value = row[0] or ''
            duration = float(row[2] or 0.0)
            play_state = row[3] or ''
            watched = play_state == 'watched'
            position_seconds = 0.0 if watched else float(row[1] or 0.0)
            result[path_value] = {
                'position_seconds': position_seconds,
                'duration': duration,
                'updated_at': int(row[4] or 0),
                'watched': watched,
                'in_progress': (not watched) and position_seconds > 0,
                'play_state': play_state,
            }
        return result

    def _normalize_resume_path(self, path_value):
        normalized = str(path_value or '').replace('\\', '/').strip().strip('/')
        if normalized.upper().startswith('VIDEO/'):
            return normalized[6:].strip('/')
        if normalized.upper() == 'VIDEO':
            return ''
        return normalized

    def _recent_folder_rows(self, client_id, limit_count=20):
        self._ensure_resume_table()
        with sqlite3.connect(self._resume_db_path()) as conn:
            rows = conn.execute(
                '''
                SELECT path, updated_at
                FROM resume_item
                WHERE client_id = ?
                ORDER BY updated_at DESC
                ''',
                (client_id,),
            ).fetchall()
        return rows[: max(int(limit_count or 0), 0)]

    def _recent_folder_paths(self, client_id, limit_count=20):
        result = []
        seen = set()
        for path_value, _updated_at in self._recent_folder_rows(client_id, limit_count * 5):
            normalized = self._normalize_resume_path(path_value)
            if not normalized or normalized == self.recent_virtual_path:
                continue
            folder_path = os.path.dirname(normalized).replace('\\', '/').strip('/')
            if not folder_path or folder_path in seen:
                continue
            seen.add(folder_path)
            result.append(folder_path)
            if len(result) >= limit_count:
                break
        return result

    def _should_hide_entry(self, current_rel, entry):
        if current_rel != '':
            return False
        show_av = str(P.ModelSetting.get('show_av') or 'True').strip().lower() in ('true', '1', 'yes', 'on')
        if show_av:
            return False
        return entry.is_dir() and str(entry.name or '').strip().upper() == 'AV'

    def _build_dir_item(self, req, root, entry, current_rel, metadata_enabled, prefetch_series_paths):
        rel = os.path.relpath(entry.path, root).replace('\\', '/')
        item = {
            'name': entry.name,
            'display_name': entry.name,
            'path': '' if rel == '.' else rel,
            'is_dir': True,
            'type': 'dir',
            'size': 0,
            'mime': None,
            'extension': '',
            'playable': False,
        }
        series_meta = metadata_enabled and self._should_attach_series_metadata(current_rel, entry.path)
        if metadata_enabled:
            P.logger.info(
                'List meta candidate current=%s entry=%s pattern=%s attach=%s',
                current_rel,
                rel,
                self._looks_like_movie_folder_name(entry.name),
                series_meta,
            )
        if series_meta:
            item['poster_url'] = self._make_meta_art_url(req, item['path'], 'poster')
            item['thumb_url'] = item['poster_url']
            prefetch_series_paths.append(item['path'])
        return item

    def _list_recent_items(self, req, root, metadata_enabled, client_id):
        items = []
        prefetch_series_paths = []
        for folder_rel in self._recent_folder_paths(client_id, 20):
            target_path = os.path.join(root, folder_rel.replace('/', os.sep))
            if not os.path.isdir(target_path):
                continue
            entry = type('RecentEntry', (), {
                'name': os.path.basename(target_path.rstrip(os.sep)),
                'path': target_path,
                'is_dir': lambda self=None: True,
            })()
            item = self._build_dir_item(req, root, entry, '', metadata_enabled, prefetch_series_paths)
            items.append(item)
        if prefetch_series_paths:
            deduped = []
            seen = set()
            for series_rel in prefetch_series_paths:
                if series_rel and series_rel not in seen:
                    seen.add(series_rel)
                    deduped.append(series_rel)
            self._schedule_metadata_prefetch(req, deduped)
        return {
            'ret': 'success',
            'data': {
                'current_path': '최근 재생항목',
                'parent_path': '',
                'is_root': False,
                'item_count': len(items),
                'items': items,
            }
        }

    def _resume_get(self, req):
        client_id = (req.args.get('client_id') or req.form.get('client_id') or 'default').strip() or 'default'
        path_value = self._normalize_resume_path(req.args.get('path') or req.form.get('path') or '')
        if not path_value:
            return {'ret': 'success', 'data': {'position_seconds': 0, 'duration': 0, 'client_id': client_id, 'watched': False, 'in_progress': False, 'play_state': ''}}
        data = self._get_resume_state(client_id, path_value)
        return {'ret': 'success', 'data': data}

    def _resume_set(self, req):
        client_id = (req.args.get('client_id') or req.form.get('client_id') or 'default').strip() or 'default'
        path_value = self._normalize_resume_path(req.args.get('path') or req.form.get('path') or '')
        position_seconds = self._parse_start_seconds(req.args.get('position_seconds') or req.form.get('position_seconds') or '0')
        duration = self._parse_start_seconds(req.args.get('duration') or req.form.get('duration') or '0')
        watched_percent = self._parse_start_seconds(req.args.get('watched_percent') or req.form.get('watched_percent') or '95')
        clear_flag = (req.args.get('clear') or req.form.get('clear') or '').lower() in ('1', 'true', 'yes', 'y')

        if not path_value:
            return {'ret': 'warning', 'msg': 'path is empty'}

        self._ensure_resume_table()
        watched_percent = min(max(float(watched_percent or 95.0), 0.0), 100.0)
        watched_ratio = ((position_seconds / duration) * 100.0) if duration > 0 else 0.0
        should_mark_watched = duration > 0 and watched_ratio >= watched_percent
        should_clear = clear_flag or position_seconds <= 0

        with sqlite3.connect(self._resume_db_path()) as conn:
            if should_mark_watched:
                conn.execute(
                    '''
                    INSERT INTO resume_item (client_id, path, position_seconds, duration, play_state, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(client_id, path) DO UPDATE SET
                        position_seconds = excluded.position_seconds,
                        duration = excluded.duration,
                        play_state = excluded.play_state,
                        updated_at = excluded.updated_at
                    ''',
                    (client_id, path_value, 0.0, duration, 'watched', int(time.time())),
                )
            elif should_clear:
                conn.execute(
                    'DELETE FROM resume_item WHERE client_id = ? AND path = ?',
                    (client_id, path_value),
                )
            else:
                conn.execute(
                    '''
                    INSERT INTO resume_item (client_id, path, position_seconds, duration, play_state, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(client_id, path) DO UPDATE SET
                        position_seconds = excluded.position_seconds,
                        duration = excluded.duration,
                        play_state = excluded.play_state,
                        updated_at = excluded.updated_at
                    ''',
                    (client_id, path_value, position_seconds, duration, 'resume', int(time.time())),
                )
            conn.commit()

        return {
            'ret': 'success',
            'data': {
                'client_id': client_id,
                'path': path_value,
                'position_seconds': 0.0 if (should_clear or should_mark_watched) else position_seconds,
                'duration': duration,
                'cleared': should_clear,
                'watched': should_mark_watched,
                'in_progress': (not should_clear) and (not should_mark_watched) and position_seconds > 0,
                'play_state': 'watched' if should_mark_watched else ('' if should_clear else 'resume'),
            }
        }

    def _metadata_db_path(self):
        db_dir = os.path.join(F.config['path_data'], 'db', P.package_name)
        os.makedirs(db_dir, exist_ok=True)
        return os.path.join(db_dir, 'metadata.sqlite')

    def _metadata_series_key(self, series_rel):
        normalized = str(series_rel or '').replace('\\', '/').strip().strip('/')
        if normalized.startswith('VIDEO/'):
            normalized = normalized[6:]
        return normalized

    def _ensure_metadata_table(self):
        with sqlite3.connect(self._metadata_db_path()) as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS metadata_item (
                    series_path TEXT PRIMARY KEY,
                    module_name TEXT,
                    metadata_code TEXT,
                    title TEXT,
                    year TEXT,
                    poster_url TEXT,
                    thumb_url TEXT,
                    fanart_url TEXT,
                    episode_json TEXT,
                    updated_at INTEGER NOT NULL DEFAULT 0
                )
                '''
            )
            conn.commit()

    def _metadata_get_cached(self, series_rel):
        series_key = self._metadata_series_key(series_rel)
        self._ensure_metadata_table()
        with sqlite3.connect(self._metadata_db_path()) as conn:
            row = conn.execute(
                '''
                SELECT module_name, metadata_code, title, year, poster_url, thumb_url, fanart_url, episode_json, updated_at
                FROM metadata_item
                WHERE series_path = ?
                ''',
                (series_key,),
            ).fetchone()
        if row is None:
            return None
        return {
            'series_path': series_key,
            'module_name': row[0] or '',
            'metadata_code': row[1] or '',
            'title': row[2] or '',
            'year': row[3] or '',
            'poster_url': row[4] or '',
            'thumb_url': row[5] or '',
            'fanart_url': row[6] or '',
            'episode_json': row[7] or '',
            'updated_at': int(row[8] or 0),
        }

    def _metadata_save_cached(self, data):
        series_key = self._metadata_series_key(data.get('series_path', ''))
        self._ensure_metadata_table()
        with sqlite3.connect(self._metadata_db_path()) as conn:
            conn.execute(
                '''
                INSERT INTO metadata_item (
                    series_path, module_name, metadata_code, title, year,
                    poster_url, thumb_url, fanart_url, episode_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(series_path) DO UPDATE SET
                    module_name = excluded.module_name,
                    metadata_code = excluded.metadata_code,
                    title = excluded.title,
                    year = excluded.year,
                    poster_url = excluded.poster_url,
                    thumb_url = excluded.thumb_url,
                    fanart_url = excluded.fanart_url,
                    episode_json = excluded.episode_json,
                    updated_at = excluded.updated_at
                ''',
                (
                    series_key,
                    data.get('module_name', ''),
                    data.get('metadata_code', ''),
                    data.get('title', ''),
                    data.get('year', ''),
                    self._normalize_art_value(data.get('poster_url', '')),
                    self._normalize_art_value(data.get('thumb_url', '')),
                    self._normalize_art_value(data.get('fanart_url', '')),
                    data.get('episode_json', ''),
                    int(time.time()),
                ),
            )
            conn.commit()

    def _normalize_art_value(self, value):
        if value is None:
            return ''
        if isinstance(value, str):
            return value
        if isinstance(value, (list, tuple)):
            for item in value:
                normalized = self._normalize_art_value(item)
                if normalized:
                    return normalized
            return ''
        if isinstance(value, dict):
            for key in ('url', 'poster', 'thumb', 'fanart', 'value'):
                normalized = self._normalize_art_value(value.get(key))
                if normalized:
                    return normalized
            return ''
        return str(value)

    def _make_meta_art_url(self, req, relative_path, kind):
        base = req.url_root.rstrip('/')
        query = self._request_auth_query(req)
        query['path'] = relative_path
        query['kind'] = kind
        return f'{base}/{P.package_name}/api/meta_art?{urlencode(query)}'

    def _make_metadata_proxy_url(self, req, image_url):
        base = req.url_root.rstrip('/')
        normalized_url = self._normalize_art_value(image_url)
        return f'{base}/metadata/normal/image_proxy?url={quote(normalized_url)}'

    def _base_url_from_req(self, req):
        return self._remember_base_url(req.url_root.rstrip('/'))

    def _normalize_series_name(self, name):
        title = str(name or '').strip()
        known_exts = ('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m2ts', '.mpg', '.mpeg', '.webm', '.m4v')
        lower_title = title.lower()
        for ext in known_exts:
            if lower_title.endswith(ext):
                title = title[:-len(ext)].strip()
                break
        title = re.sub(r'\[[^\]]+\]\s*$', '', title).strip()
        title = re.sub(r'\{[^}]+\}\s*$', '', title).strip()
        year_match = re.search(r'\((\d{4})\)', title)
        year = year_match.group(1) if year_match else ''
        if year:
            title = re.sub(r'\(\d{4}\)', '', title).strip()
        title = re.sub(r'\s+', ' ', title).strip()
        return title, year

    def _extract_embedded_metadata_code(self, name, module_name):
        text = str(name or '')
        if module_name == 'movie':
            match = re.search(r'\{tmdb-(\d+)\}', text, re.IGNORECASE)
            if match:
                return 'MT' + match.group(1)
        return ''

    def _metadata_module_hints(self, series_rel):
        normalized = '/' + '/'.join([part for part in series_rel.replace('\\', '/').split('/') if part]) + '/'
        movie_tokens = ('/VIDEO/ì˜í™”/', '/ì˜í™”/')
        forced_ftv_tokens = ('/VIDEO/ì™¸êµ­TV/', '/ì™¸êµ­TV/', '/VIDEO/ì¼ë³¸ ì• ë‹ˆë©”ì´ì…˜/', '/ì¼ë³¸ ì• ë‹ˆë©”ì´ì…˜/')
        foreign_tokens = ('/ì™¸êµ­/', '/ëŒ€ë§Œë“œë¼ë§ˆ/', '/ì¤‘ë“œ/', '/ë¯¸ë“œ/', '/ì˜ë“œ/', '/ì¼ë“œ/')
        if any(token in normalized for token in movie_tokens):
            return ('movie', 'ktv', 'ftv')
        if any(token in normalized for token in forced_ftv_tokens):
            return ('ftv', 'ktv')
        if any(token in normalized for token in foreign_tokens):
            return ('ftv', 'ktv')
        return ('ktv', 'ftv')

    def _fetch_json_url(self, url):
        req = Request(url, headers={'Accept': 'application/json', 'User-Agent': f'{P.package_name}/1.0'})
        with urlopen(req, timeout=20) as response:
            payload = response.read().decode('utf-8')
        return json.loads(payload)

    def _metadata_search_code(self, module_name, payload):
        if module_name == 'ktv' and isinstance(payload, dict):
            if 'daum' in payload and isinstance(payload['daum'], dict):
                return payload['daum'].get('code', ''), payload['daum'].get('title', '')
            for key in ('tving', 'wavve', 'watcha'):
                value = payload.get(key)
                if isinstance(value, list) and value:
                    return value[0].get('code', ''), value[0].get('title', '')
                if isinstance(value, dict):
                    return value.get('code', ''), value.get('title', '')
        if module_name == 'movie':
            if isinstance(payload, list) and payload:
                return payload[0].get('code', ''), payload[0].get('title', '')
            if isinstance(payload, dict):
                if isinstance(payload.get('data'), list) and payload['data']:
                    return payload['data'][0].get('code', ''), payload['data'][0].get('title', '')
                if payload.get('code'):
                    return payload.get('code', ''), payload.get('title', '')
        if module_name == 'ftv' and isinstance(payload, list) and payload:
            return payload[0].get('code', ''), payload[0].get('title', '')
        return '', ''

    def _extract_art_fields(self, payload):
        art = payload.get('art') if isinstance(payload, dict) else {}
        art_dict = art if isinstance(art, dict) else {}
        art_list_value = self._normalize_art_value(art if isinstance(art, (list, tuple)) else '')
        poster = self._normalize_art_value(
            art_dict.get('poster') or payload.get('poster') or art_dict.get('thumb') or payload.get('thumb') or art_list_value or ''
        )
        thumb = self._normalize_art_value(
            art_dict.get('thumb') or payload.get('thumb') or art_list_value or poster
        )
        fanart = self._normalize_art_value(
            art_dict.get('fanart') or payload.get('fanart') or art_dict.get('landscape') or payload.get('landscape') or art_list_value or ''
        )
        return poster, thumb, fanart

    def _extract_episode_keys(self, filename):
        patterns = (
            r'(?:[sS](?P<season>[0-9]{1,2}))?[eE](?P<ep>[0-9]{1,4})',
            r'(?P<ep>\d{1,4})[íšŒí™”]',
        )
        keys = []
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match and match.groupdict().get('ep') is not None:
                keys.append(str(int(match.group('ep'))))
                break
        date_patterns = (
            r'[^0-9a-zA-Z](?P<year>[0-9]{2})(?P<month>[0-9]{2})(?P<day>[0-9]{2})[^0-9a-zA-Z]',
            r'(?P<year>[0-9]{4})[^0-9a-zA-Z]+(?P<month>[0-9]{2})[^0-9a-zA-Z]+(?P<day>[0-9]{2})(?:[^0-9]|$)',
        )
        for pattern in date_patterns:
            match = re.search(pattern, filename)
            if not match:
                continue
            year = match.group('year')
            if len(year) == 2:
                year = ('20' if int(year) < 30 else '19') + year
            keys.append('{}-{}-{}'.format(year, match.group('month'), match.group('day')))
            break
        deduped = []
        seen = set()
        for key in keys:
            if key and key not in seen:
                seen.add(key)
                deduped.append(key)
        return deduped

    def _extract_episode_display_parts(self, filename):
        episode_no = ''
        air_date = ''
        match = re.search(r'(?:[sS](?P<season>[0-9]{1,2}))?[eE](?P<ep>[0-9]{1,4})', filename)
        if match and match.groupdict().get('ep') is not None:
            episode_no = str(int(match.group('ep')))
        if not episode_no:
            match = re.search(r'(?P<ep>\d{1,4})[íšŒí™”]', filename)
            if match:
                episode_no = str(int(match.group('ep')))

        date_patterns = (
            r'[^0-9a-zA-Z](?P<year>[0-9]{2})(?P<month>[0-9]{2})(?P<day>[0-9]{2})[^0-9a-zA-Z]',
            r'(?P<year>[0-9]{4})[^0-9a-zA-Z]+(?P<month>[0-9]{2})[^0-9a-zA-Z]+(?P<day>[0-9]{2})(?:[^0-9]|$)',
        )
        for pattern in date_patterns:
            match = re.search(pattern, filename)
            if not match:
                continue
            year = match.group('year')
            if len(year) == 4:
                year = year[2:]
            air_date = '{}-{}-{}'.format(year, match.group('month'), match.group('day'))
            break
        return episode_no, air_date

    def _format_episode_display_name(self, filename):
        episode_no, air_date = self._extract_episode_display_parts(filename)
        if episode_no:
            return '{}íšŒ'.format(episode_no)
        return filename

    def _episode_source_rank(self, filename):
        upper = filename.upper()
        if '-SW' in upper or '.SW.' in upper or upper.endswith('SW.MKV') or upper.endswith('SW.MP4'):
            return 0
        if '-ST' in upper or '.ST.' in upper or upper.endswith('ST.MKV') or upper.endswith('ST.MP4'):
            return 1
        return 2

    def _episode_dedupe_key(self, filename):
        episode_no, air_date = self._extract_episode_display_parts(filename)
        if episode_no:
            return 'ep:' + episode_no
        if air_date:
            return 'date:' + air_date
        stem = os.path.splitext(filename)[0]
        stem = re.sub(r'[-._ ](?:SW|ST)(?=$|[-._ ])', '', stem, flags=re.IGNORECASE)
        stem = re.sub(r'\s+', ' ', stem).strip().lower()
        return 'name:' + stem

    def _format_episode_display_name(self, filename):
        episode_no, air_date = self._extract_episode_display_parts(filename)
        if episode_no:
            return '{}\uD68C'.format(episode_no)
        return filename

    def _extract_episode_thumb_map(self, payload):
        result = {}
        if isinstance(payload, dict):
            if 'episodes' in payload and isinstance(payload['episodes'], (list, dict)):
                payload = payload['episodes']
            elif all(isinstance(v, dict) for v in payload.values()):
                payload = payload
            else:
                payload = list(payload.values())
        if isinstance(payload, dict):
            items = list(payload.items())
        elif isinstance(payload, list):
            items = [(None, item) for item in payload]
        else:
            items = []
        for dict_key, item in items:
            if not isinstance(item, dict):
                continue
            source_item = None
            for key in ('tving', 'wavve', 'daum', 'watcha', 'tmdb', 'tvdb'):
                if isinstance(item.get(key), dict):
                    source_item = item.get(key)
                    break
            if source_item is None:
                source_item = item
            episode_no = (
                source_item.get('episode') or source_item.get('episode_no') or source_item.get('episode_number')
                or source_item.get('no') or source_item.get('index') or dict_key
                or item.get('episode') or item.get('episode_no') or item.get('episode_number')
                or item.get('no') or item.get('index')
            )
            episode_keys = []
            try:
                if episode_no is not None and str(episode_no).strip() != '':
                    episode_keys.append(str(int(episode_no)))
            except Exception:
                pass
            released_at = (
                source_item.get('premiered') or source_item.get('released_at') or source_item.get('air_date')
                or source_item.get('date') or source_item.get('broadcast_date')
                or item.get('premiered') or item.get('released_at') or item.get('air_date')
                or item.get('date') or item.get('broadcast_date')
            )
            if released_at:
                released_text = str(released_at).strip()
                date_match = re.search(r'(?P<year>\d{4})[-./]?(?P<month>\d{2})[-./]?(?P<day>\d{2})', released_text)
                if date_match:
                    episode_keys.append('{}-{}-{}'.format(
                        date_match.group('year'),
                        date_match.group('month'),
                        date_match.group('day'),
                    ))
            if not episode_keys:
                continue
            poster, thumb, fanart = self._extract_art_fields(source_item)
            if not (thumb or poster or fanart):
                poster, thumb, fanart = self._extract_art_fields(item)
            image_url = self._normalize_art_value(thumb or poster or fanart)
            if image_url:
                for episode_key in episode_keys:
                    result[episode_key] = image_url
        return result

    def _lookup_metadata_cache(self, req, series_rel, force_refresh=False):
        cached = self._metadata_get_cached(series_rel)
        if cached and not force_refresh:
            return cached

        base = self._base_url_from_req(req)
        return self._lookup_metadata_cache_by_base(base, series_rel, force_refresh=force_refresh, cached=cached)

    def _lookup_metadata_cache_by_base(self, base, series_rel, force_refresh=False, cached=None):
        if cached is None:
            cached = self._metadata_get_cached(series_rel)
        if cached and not force_refresh:
            return cached

        series_name = os.path.basename(series_rel.rstrip('/\\'))
        title, year = self._normalize_series_name(series_name)
        if not title:
            return cached

        def build_search_queries(module_name):
            if module_name == 'movie':
                queries = []
                if year:
                    queries.append({'call': 'kodi', 'keyword': f'{title} | {year}'})
                    queries.append({'call': 'kodi', 'keyword': title, 'year': year})
                queries.append({'call': 'kodi', 'keyword': title})
                return queries
            query = {'call': 'kodi', 'keyword': title}
            if year:
                query['year'] = year
            return [query]

        for module_name in self._metadata_module_hints(series_rel):
            try:
                metadata_code = ''
                matched_title = ''
                embedded_code = self._extract_embedded_metadata_code(series_name, module_name)
                if embedded_code:
                    metadata_code = embedded_code
                    matched_title = title
                    P.logger.info(
                        'Metadata embedded code module=%s series=%s code=%s',
                        module_name,
                        series_rel,
                        metadata_code,
                    )
                for query in build_search_queries(module_name):
                    if metadata_code:
                        break
                    search_url = f"{base}/metadata/api/{module_name}/search?{urlencode(query)}"
                    search_payload = self._fetch_json_url(search_url)
                    metadata_code, matched_title = self._metadata_search_code(module_name, search_payload)
                    P.logger.info(
                        'Metadata search module=%s series=%s query=%s code=%s matched_title=%s payload_type=%s',
                        module_name,
                        series_rel,
                        query,
                        metadata_code,
                        matched_title,
                        type(search_payload).__name__,
                    )
                    if metadata_code:
                        break
                if not metadata_code:
                    continue

                info_query = {'code': metadata_code}
                if matched_title:
                    info_query['title'] = matched_title
                info_url = f"{base}/metadata/api/{module_name}/info?{urlencode(info_query)}"
                info_payload = self._fetch_json_url(info_url)
                P.logger.info(
                    'Metadata info module=%s series=%s code=%s payload_type=%s',
                    module_name,
                    series_rel,
                    metadata_code,
                    type(info_payload).__name__,
                )
                if not isinstance(info_payload, dict):
                    continue
                poster_url, thumb_url, fanart_url = self._extract_art_fields(info_payload)
                episode_json = ''
                episodes_payload = None
                if isinstance(info_payload.get('extra_info'), dict):
                    episodes_payload = info_payload.get('extra_info', {}).get('episodes')
                if episodes_payload:
                    episode_json = json.dumps(self._extract_episode_thumb_map(episodes_payload), ensure_ascii=False)
                elif module_name == 'ktv' and len(metadata_code) > 1 and metadata_code[1] == 'D':
                    try:
                        episode_url = f"{base}/metadata/api/ktv/episode_info?{urlencode({'code': metadata_code})}"
                        episode_payload = self._fetch_json_url(episode_url)
                        episode_json = json.dumps(self._extract_episode_thumb_map(episode_payload), ensure_ascii=False)
                    except Exception:
                        episode_json = ''

                result = {
                    'series_path': self._metadata_series_key(series_rel),
                    'module_name': module_name,
                    'metadata_code': metadata_code,
                    'title': matched_title or title,
                    'year': year,
                    'poster_url': poster_url or '',
                    'thumb_url': thumb_url or '',
                    'fanart_url': fanart_url or '',
                    'episode_json': episode_json,
                }
                self._metadata_save_cached(result)
                return result
            except Exception as e:
                P.logger.warning(f'Metadata lookup failed for {series_rel} via {module_name}: {str(e)}')
                continue
        return cached

    def _schedule_metadata_prefetch(self, req, series_paths):
        base = self._base_url_from_req(req)
        for series_rel in series_paths[:6]:
            cached = self._metadata_get_cached(series_rel)
            if cached:
                continue
            with self.metadata_lookup_lock:
                if series_rel in self.metadata_lookup_inflight:
                    continue
                self.metadata_lookup_inflight.add(series_rel)
            worker = threading.Thread(
                target=self._metadata_prefetch_worker,
                args=(base, series_rel),
                daemon=True,
            )
            worker.start()

    def _metadata_prefetch_worker(self, base, series_rel):
        try:
            with self.metadata_lookup_semaphore:
                _, target = self._resolve_target_path(series_rel)
                if not os.path.isdir(target) or not self._is_probable_series_folder(target):
                    return
                self._lookup_metadata_cache_by_base(base, series_rel)
        except Exception as e:
            P.logger.warning(f'Metadata prefetch failed for {series_rel}: {str(e)}')
        finally:
            with self.metadata_lookup_lock:
                self.metadata_lookup_inflight.discard(series_rel)

    def _serve_meta_art(self, req):
        relative_path = req.args.get('path', '').strip()
        kind = (req.args.get('kind') or 'poster').strip().lower()
        if not relative_path:
            abort(404)
        root, target = self._resolve_target_path(relative_path)
        series_rel = relative_path if os.path.isdir(target) else os.path.dirname(relative_path).replace('\\', '/')
        plex_image_url = self._plex_art_lookup(relative_path, os.path.isdir(target), kind)
        if plex_image_url:
            return redirect(self._make_metadata_proxy_url(req, plex_image_url))
        if os.path.isdir(target):
            if not self._is_probable_series_folder(target):
                P.logger.info('Meta art empty: not a series folder path=%s target=%s kind=%s', relative_path, target, kind)
                return Response(self.transparent_gif, mimetype='image/gif')
        else:
            series_target = os.path.dirname(target)
            if not self._is_probable_series_folder(series_target):
                P.logger.info('Meta art empty: parent not a series folder path=%s series_target=%s kind=%s', relative_path, series_target, kind)
                return Response(self.transparent_gif, mimetype='image/gif')
        metadata = self._lookup_metadata_cache(req, series_rel)
        if not metadata:
            P.logger.info('Meta art empty: metadata lookup failed path=%s series=%s kind=%s', relative_path, series_rel, kind)
            return Response(self.transparent_gif, mimetype='image/gif')

        image_url = ''
        if kind == 'thumb' and os.path.isfile(target):
            try:
                episode_map = json.loads(metadata.get('episode_json') or '{}')
            except Exception:
                episode_map = {}
            episode_keys = self._extract_episode_keys(os.path.basename(relative_path))
            P.logger.info(
                'Meta thumb request path=%s series=%s keys=%s cached_episode_keys=%s',
                relative_path,
                series_rel,
                episode_keys,
                list(episode_map.keys())[:20],
            )
            for episode_key in episode_keys:
                image_url = episode_map.get(episode_key, '')
                if image_url:
                    P.logger.info(
                        'Meta thumb matched from cache path=%s key=%s url=%s',
                        relative_path,
                        episode_key,
                        image_url,
                    )
                    break
            if not image_url and episode_keys:
                metadata = self._lookup_metadata_cache(req, series_rel, force_refresh=True)
                if metadata:
                    try:
                        episode_map = json.loads(metadata.get('episode_json') or '{}')
                    except Exception:
                        episode_map = {}
                    P.logger.info(
                        'Meta thumb refresh path=%s series=%s keys=%s refreshed_episode_keys=%s',
                        relative_path,
                        series_rel,
                        episode_keys,
                        list(episode_map.keys())[:20],
                    )
                    for episode_key in episode_keys:
                        image_url = episode_map.get(episode_key, '')
                        if image_url:
                            P.logger.info(
                                'Meta thumb matched after refresh path=%s key=%s url=%s',
                                relative_path,
                                episode_key,
                                image_url,
                            )
                            break

        if not image_url:
            image_url = metadata.get('poster_url') or metadata.get('thumb_url') or metadata.get('fanart_url') or ''
            P.logger.info(
                'Meta art fallback path=%s kind=%s fallback_url=%s metadata_code=%s',
                relative_path,
                kind,
                image_url,
                metadata.get('metadata_code', ''),
            )
        if not image_url:
            return Response(self.transparent_gif, mimetype='image/gif')
        return redirect(self._make_metadata_proxy_url(req, image_url))

    def _resolve_target_path(self, relative_path=''):
        root = self._effective_gds_root()
        if not root:
            raise Exception('gds_path is empty')
        if not self._run_with_timeout('gds_root_exists', lambda: os.path.exists(root), timeout_seconds=5):
            raise Exception('gds_path does not exist')
        candidate = os.path.abspath(os.path.join(root, relative_path or ''))
        if os.path.commonpath([root, candidate]) != root:
            raise Exception('invalid path')
        return root, candidate

    def _effective_gds_root(self):
        configured = os.path.abspath(P.ModelSetting.get('gds_path') or '')
        if not configured:
            return configured
        if os.path.basename(os.path.normpath(configured)).upper() == 'VIDEO':
            return configured
        video_root = os.path.join(configured, 'VIDEO')
        if self._run_with_timeout('gds_video_root_exists', lambda: os.path.isdir(video_root), timeout_seconds=5):
            return os.path.abspath(video_root)
        return configured

    def _gds_tool_db_path(self):
        return os.path.join(F.config['path_data'], 'db', 'gds_tool.db')

    def _is_recent_sort_target(self, current_rel):
        normalized = '/'.join([part for part in (current_rel or '').split('/') if part])
        parts = normalized.split('/') if normalized else []
        if len(parts) >= 2 and parts[-2] == '\ubc29\uc1a1\uc911':
            return True
        if len(parts) >= 2 and parts[-2] == '\ubc29\uc1a1\uc911(\uae30\ud0c0)':
            return True
        if parts and parts[-1] == '\ucd5c\uc2e0' and '\uc601\ud654' in parts:
            return True
        if len(parts) >= 3 and parts[-3] == '\uc601\ud654' and parts[-2] == '\ucd5c\uc2e0':
            return True
        return False

    def _is_recent_episode_sort_target(self, current_rel):
        normalized = '/'.join([part for part in (current_rel or '').split('/') if part])
        parts = normalized.split('/') if normalized else []
        if len(parts) >= 3 and parts[-3] == '\ubc29\uc1a1\uc911':
            return True
        if len(parts) >= 2 and parts[-2] == '\ubc29\uc1a1\uc911(\uae30\ud0c0)':
            return True
        if len(parts) >= 2 and parts[-2] == '\ucd5c\uc2e0':
            return True
        return False

    def _is_series_metadata_target(self, current_rel):
        return False

    def _is_episode_metadata_target(self, current_rel):
        return False

    def _should_attach_series_metadata(self, current_rel, target):
        return self._looks_like_movie_folder_name(os.path.basename(target))

    def _should_attach_episode_metadata(self, current_rel, target):
        parent_name = os.path.basename(os.path.dirname(target))
        if parent_name in ('', '.', '..'):
            parent_name = os.path.basename(current_rel or '')
        return self._looks_like_movie_folder_name(parent_name)

    def _is_media_filename(self, name):
        ext = os.path.splitext(name)[1].lower()
        if ext in ('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m2ts', '.mpg', '.mpeg', '.webm', '.m4v'):
            return True
        mime, _ = mimetypes.guess_type(name)
        return bool(mime and mime.startswith('video/'))

    def _has_direct_media_files(self, folder_path):
        try:
            count = 0
            for child in os.scandir(folder_path):
                count += 1
                if not child.is_dir() and self._is_media_filename(child.name):
                    return True
                if count >= 100:
                    break
        except Exception:
            return False
        return False

    def _folder_probe_signature(self, folder_path):
        try:
            stat = os.stat(folder_path)
            return (int(stat.st_mtime), int(stat.st_size))
        except Exception:
            return None

    def _is_probable_series_folder(self, folder_path):
        signature = self._folder_probe_signature(folder_path)
        if signature is not None:
            with self.series_folder_probe_lock:
                cached = self.series_folder_probe_cache.get(folder_path)
                if cached and cached[0] == signature:
                    return cached[1]

        result = False
        if self._has_direct_media_files(folder_path):
            result = True
        else:
            try:
                for child in os.scandir(folder_path):
                    if not child.is_dir():
                        continue
                    lower_name = child.name.lower()
                    if lower_name.startswith('season') or '\uc2dc\uc98c' in child.name:
                        if self._has_direct_media_files(child.path):
                            result = True
                            break
            except Exception:
                result = False

        if signature is not None:
            with self.series_folder_probe_lock:
                self.series_folder_probe_cache[folder_path] = (signature, result)
                if len(self.series_folder_probe_cache) > 2000:
                    self.series_folder_probe_cache.clear()
        return result

    def _has_series_children(self, folder_path):
        try:
            checked = 0
            for child in os.scandir(folder_path):
                if not child.is_dir():
                    continue
                checked += 1
                if self._is_probable_series_folder(child.path):
                    return True
                if checked >= 80:
                    break
        except Exception:
            return False
        return False

    def _looks_like_movie_folder_name(self, name):
        text = str(name or '').strip()
        return bool(re.search(r'\(\d{4}\)', text) or re.search(r'\{tmdb-\d+\}', text, re.IGNORECASE))

    def _is_movie_tree_path(self, rel_path):
        normalized = '/'.join([part for part in (rel_path or '').split('/') if part])
        parts = normalized.split('/') if normalized else []
        return '\uc601\ud654' in parts

    def _has_movie_children(self, folder_path):
        try:
            checked = 0
            for child in os.scandir(folder_path):
                if not child.is_dir():
                    continue
                checked += 1
                if self._looks_like_movie_folder_name(child.name):
                    return True
                if checked >= 120:
                    break
        except Exception:
            return False
        return False
    def _make_gds_virtual_path(self, root, path_value):
        rel = os.path.relpath(path_value, root).replace('\\', '/')
        root_name = os.path.basename(os.path.normpath(root)).replace('\\', '/').strip('/')
        virtual_parts = ['/ROOT/GDRIVE']
        if root_name.upper() == 'VIDEO':
            virtual_parts.append('VIDEO')
        if rel != '.':
            virtual_parts.append(rel.strip('/'))
        return '/'.join(part for part in virtual_parts if part)

    def _get_series_recent_map(self, root, target, entries):
        db_path = self._gds_tool_db_path()
        if not os.path.exists(db_path):
            return {}

        child_items = {}
        for entry in entries:
            child_items[entry.path] = 0

        if not child_items:
            return {}

        target_prefix = self._make_gds_virtual_path(root, target).rstrip('/') + '/'
        try:
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    '''
                    SELECT gds_path, created_time
                    FROM fp_item
                    WHERE gds_path IS NOT NULL
                      AND gds_path != ''
                      AND (scan_mode IS NULL OR UPPER(scan_mode) = 'ADD')
                      AND gds_path LIKE ?
                    ORDER BY created_time DESC
                    ''',
                    (target_prefix + '%',),
                ).fetchall()
        except Exception as e:
            P.logger.warning(f'Failed to read gds_tool.db for recent sort: {str(e)}')
            return {}

        entry_meta = []
        for entry in entries:
            entry_virtual = self._make_gds_virtual_path(root, entry.path)
            entry_meta.append((entry, entry_virtual, entry_virtual.rstrip('/') + ('/' if entry.is_dir() else '')))

        for gds_path, created_time in rows:
            if not gds_path:
                continue
            normalized_path = str(gds_path).replace('\\', '/')
            timestamp = self._parse_series_sort_time(created_time)
            for entry, entry_virtual, entry_prefix in entry_meta:
                if entry.is_dir():
                    if normalized_path.startswith(entry_prefix):
                        if timestamp > child_items[entry.path]:
                            child_items[entry.path] = timestamp
                        break
                else:
                    if normalized_path == entry_virtual:
                        if timestamp > child_items[entry.path]:
                            child_items[entry.path] = timestamp
                        break
        return child_items

    def _parse_series_sort_time(self, value):
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return 0
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(text, fmt).timestamp()
            except Exception:
                continue
        return 0

    def _list_items(self, req):
        relative_path = req.args.get('path', '') if hasattr(req, 'args') else ''
        recent_sort_mode = (req.args.get('recent_sort_mode') or 'latest').strip().lower()
        metadata_enabled = self._req_bool(req, 'metadata_enabled', True)
        client_id = (req.args.get('client_id') or req.form.get('client_id') or 'default').strip() or 'default'
        self._diagnostic_log(
            'list_items start path={} recent_sort_mode={} metadata_enabled={} client_id={} gds_path={}'.format(
                relative_path,
                recent_sort_mode,
                metadata_enabled,
                client_id,
                P.ModelSetting.get('gds_path'),
            )
        )
        if str(relative_path or '').strip().strip('/') == self.recent_virtual_path:
            root, _ = self._resolve_target_path('')
            self._diagnostic_log(f'list_items recent virtual path root={root}')
            return self._list_recent_items(req, root, metadata_enabled, client_id)
        root, target = self._resolve_target_path(relative_path)
        self._diagnostic_log(f'list_items resolved root={root} target={target}')
        if not self._run_with_timeout('list_target_isdir', lambda: os.path.isdir(target), timeout_seconds=5):
            self._diagnostic_log(f'list_items target_not_dir target={target}')
            raise Exception('target path is not a directory')

        current_rel = os.path.relpath(target, root).replace('\\', '/')
        if current_rel == '.':
            current_rel = ''

        items = []
        entries = self._run_with_timeout(
            'list_target_scandir',
            lambda: [entry for entry in os.scandir(target) if not self._should_hide_entry(current_rel, entry)],
            timeout_seconds=8,
        )
        self._diagnostic_log(f'list_items scandir_ok current_rel={current_rel} entry_count={len(entries)}')
        if self._is_recent_episode_sort_target(current_rel):
            file_groups = {}
            deduped_entries = []
            for entry in entries:
                if entry.is_dir():
                    deduped_entries.append(entry)
                    continue
                dedupe_key = self._episode_dedupe_key(entry.name)
                existing = file_groups.get(dedupe_key)
                if existing is None:
                    file_groups[dedupe_key] = entry
                    continue
                current_rank = self._episode_source_rank(entry.name)
                existing_rank = self._episode_source_rank(existing.name)
                if current_rank < existing_rank:
                    file_groups[dedupe_key] = entry
                elif current_rank == existing_rank:
                    try:
                        if entry.stat().st_mtime > existing.stat().st_mtime:
                            file_groups[dedupe_key] = entry
                    except Exception:
                        pass
            entries = deduped_entries + list(file_groups.values())
        series_recent_map = {}
        if recent_sort_mode == 'latest' and self._is_recent_sort_target(current_rel):
            series_recent_map = self._get_series_recent_map(root, target, entries)
        file_recent_sort = recent_sort_mode == 'latest' and self._is_recent_episode_sort_target(current_rel)
        prefetch_series_paths = []
        file_paths = []

        def sort_key(entry):
            if entry.is_dir():
                recent = series_recent_map.get(entry.path, 0)
                return (0, -recent, entry.name.lower())
            if file_recent_sort:
                try:
                    return (1, -entry.stat().st_mtime, entry.name.lower())
                except Exception:
                    pass
            return (1, 0, entry.name.lower())

        for entry in sorted(entries, key=sort_key):
            if entry.is_dir():
                item = self._build_dir_item(req, root, entry, current_rel, metadata_enabled, prefetch_series_paths)
            else:
                rel = os.path.relpath(entry.path, root).replace('\\', '/')
                mime, _ = mimetypes.guess_type(entry.path)
                episode_meta = metadata_enabled and self._should_attach_episode_metadata(current_rel, entry.path)
                item = {
                    'name': entry.name,
                    'display_name': entry.name if not metadata_enabled else self._format_episode_display_name(entry.name),
                    'path': '' if rel == '.' else rel,
                    'is_dir': False,
                    'type': 'file',
                    'size': entry.stat().st_size,
                    'mime': mime,
                    'extension': os.path.splitext(entry.name)[1].lower(),
                    'playable': True,
                }
                if episode_meta:
                    item['thumb_url'] = self._make_meta_art_url(req, item['path'], 'thumb')
                    item['poster_url'] = self._make_meta_art_url(req, os.path.dirname(item['path']).replace('\\', '/'), 'poster')
                    prefetch_series_paths.append(os.path.dirname(item['path']).replace('\\', '/'))
                file_paths.append(item['path'])
            items.append(item)

        if current_rel == '':
            items.insert(0, {
                'name': '최근 재생항목',
                'display_name': '최근 재생항목',
                'path': self.recent_virtual_path,
                'is_dir': True,
                'type': 'dir',
                'size': 0,
                'mime': None,
                'extension': '',
                'playable': False,
            })

        resume_map = self._get_resume_state_map(client_id, file_paths)
        for item in items:
            if item.get('is_dir'):
                continue
            resume_state = resume_map.get(item.get('path', ''), {})
            item['resume_seconds'] = float(resume_state.get('position_seconds', 0.0) or 0.0)
            item['duration_seconds'] = float(resume_state.get('duration', 0.0) or 0.0)
            item['watched'] = bool(resume_state.get('watched', False))
            item['in_progress'] = bool(resume_state.get('in_progress', False))
            item['play_state'] = resume_state.get('play_state', '')

        if prefetch_series_paths:
            deduped = []
            seen = set()
            for series_rel in prefetch_series_paths:
                if series_rel and series_rel not in seen:
                    seen.add(series_rel)
                    deduped.append(series_rel)
            self._schedule_metadata_prefetch(req, deduped)

        return {
            'ret': 'success',
            'data': {
                'current_path': current_rel,
                'parent_path': '' if current_rel == '' else os.path.dirname(current_rel).replace('\\', '/'),
                'is_root': current_rel == '',
                'item_count': len(items),
                'items': items,
            }
        }

    def _ffmpeg_version(self):
        ffmpeg_path = P.ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            return {'ret': 'danger', 'msg': 'ffmpeg_path is empty'}
        proc = subprocess.run([ffmpeg_path, '-version'], capture_output=True, text=True, timeout=10, check=False)
        if proc.returncode != 0:
            return {'ret': 'danger', 'msg': proc.stderr.strip() or 'ffmpeg check failed'}
        return {'ret': 'success', 'title': 'ffmpeg version', 'modal': proc.stdout.strip()}

    def _transcode_capabilities(self):
        ffmpeg_path = P.ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            return {'ret': 'danger', 'msg': 'ffmpeg_path is empty'}

        encoders = subprocess.run([ffmpeg_path, '-hide_banner', '-encoders'], capture_output=True, text=True, timeout=20, check=False)
        hwaccels = subprocess.run([ffmpeg_path, '-hide_banner', '-hwaccels'], capture_output=True, text=True, timeout=20, check=False)
        if encoders.returncode != 0:
            return {'ret': 'danger', 'msg': encoders.stderr.strip() or 'encoder check failed'}

        encoder_text = encoders.stdout
        hwaccel_text = hwaccels.stdout if hwaccels.returncode == 0 else (hwaccels.stderr or '')
        known = [
            ('libx264', 'H.264 CPU'),
            ('libx265', 'H.265 CPU'),
            ('h264_nvenc', 'H.264 NVIDIA NVENC'),
            ('hevc_nvenc', 'H.265 NVIDIA NVENC'),
            ('h264_qsv', 'H.264 Intel QSV'),
            ('hevc_qsv', 'H.265 Intel QSV'),
            ('h264_vaapi', 'H.264 VAAPI'),
            ('hevc_vaapi', 'H.265 VAAPI'),
        ]
        lines = ['[Detected encoders]']
        found = False
        for token, label in known:
            if token in encoder_text:
                lines.append(f'- {label}: available')
                found = True
        if not found:
            lines.append('- No known video transcode encoder detected')
        lines.extend(['', '[hwaccels]', hwaccel_text.strip() or 'No hwaccels output', '', '[matching encoder lines]'])
        excerpt = [line for line in encoder_text.splitlines() if any(token in line for token, _ in known)]
        lines.append('\n'.join(excerpt) if excerpt else 'No matching encoder lines')
        return {'ret': 'success', 'title': 'Transcode capabilities', 'modal': '\n'.join(lines)}

    def _test_transcode_encoder(self):
        codec = (P.ModelSetting.get('transcode_codec') or 'h264').lower()
        ffmpeg_path = P.ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            return {'ret': 'danger', 'msg': 'ffmpeg_path is empty'}

        tested = []
        selected = None
        selected_device = ''
        for encoder in self._candidate_encoders(codec):
            ok, device = self._encoder_is_usable(ffmpeg_path, encoder, force=True, with_device=True)
            suffix = f' ({device})' if device else ''
            tested.append(f'- {encoder}: {"OK" if ok else "FAIL"}{suffix}')
            if ok and selected is None:
                selected = encoder
                selected_device = device or ''

        if selected is None:
            return {
                'ret': 'danger',
                'title': 'Encoder test',
                'modal': '\n'.join(['[codec]', codec, '', '[results]', *tested]),
                'msg': 'ì‚¬ìš© ê°€ëŠ¥í•œ ì¸ì½”ë”ë¥¼ ì°¾ì§€ ëª»í–ˆìŠµë‹ˆë‹¤',
            }

        P.ModelSetting.set(self._saved_encoder_key(codec), selected)
        if selected_device:
            P.ModelSetting.set('transcode_vaapi_device', selected_device)

        return {
            'ret': 'success',
            'title': 'Encoder test',
            'modal': '\n'.join(['[codec]', codec, '', '[saved]', selected, '', '[results]', *tested]),
            'data': {'selected_encoder': selected, 'selected_device': selected_device},
            'msg': 'í…ŒìŠ¤íŠ¸ ê²°ê³¼ê°€ ì €ìž¥ë˜ì—ˆìŠµë‹ˆë‹¤',
        }

    def _find_subtitle_paths(self, video_path, root):
        """ê°™ì€ í´ë”ì— ìžˆëŠ” ëª¨ë“  .srt íŒŒì¼ì„ ì°¾ì•„ relative path ëª©ë¡ìœ¼ë¡œ ë°˜í™˜."""
        folder = os.path.dirname(video_path)
        result = []
        try:
            for entry in sorted(os.scandir(folder), key=lambda x: x.name.lower()):
                if entry.is_file() and '.srt' in entry.name.lower():
                    rel = os.path.relpath(entry.path, root).replace('\\', '/')
                    result.append(rel)
        except Exception:
            pass
        return result

    def _make_subtitle_url(self, req, relative_path):
        base = req.url_root.rstrip('/')
        query = self._request_auth_query(req)
        query['path'] = relative_path
        return f'{base}/{P.package_name}/api/subtitle?{urlencode(query)}'

    def _serve_subtitle(self, req):
        file_path = req.args.get('path', '')
        _, target = self._resolve_target_path(file_path)
        if not os.path.isfile(target):
            abort(404)
        if not target.lower().endswith('.srt') and '.srt' not in target.lower():
            abort(403)
        return send_file(target, mimetype='text/plain; charset=utf-8', conditional=True, as_attachment=False)

    def _play(self, req):
        file_path = req.args.get('path', '')
        root, target = self._resolve_target_path(file_path)
        if not os.path.isfile(target):
            abort(404)

        requested_resolution = (req.args.get('resolution') or 'original').lower()
        quality = (req.args.get('quality') or 'medium').lower()
        start_seconds = self._parse_start_seconds(req.args.get('start_seconds', '0'))
        ffmpeg_path = P.ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            raise Exception('ffmpeg_path is empty')

        source_info = self._probe_video(target, ffmpeg_path)
        mode = self._decide_play_mode(requested_resolution, source_info)

        subtitle_paths = self._find_subtitle_paths(target, root)
        subtitle_urls = [self._make_subtitle_url(req, p) for p in subtitle_paths]

        if mode == 'directplay':
            stream_url = self._make_directplay_url(req, file_path)
            response = jsonify({
                'ret': 'success',
                'data': {
                    'mode': 'directplay',
                    'stream_url': stream_url,
                    'path': file_path,
                    'requested_resolution': requested_resolution,
                    'quality': quality,
                    'start_seconds': start_seconds,
                    'duration': source_info.get('duration', 0),
                    'source': source_info,
                    'subtitle_urls': subtitle_urls,
                }
            })
            response.headers['X-KodiGds-DirectPlay'] = stream_url
            return response

        job = self._ensure_vod_job(target, requested_resolution, quality, start_seconds, ffmpeg_path, source_info)
        playlist_url = self._make_vod_url(req, job['job_id'], 'playlist.m3u8')
        response = jsonify({
            'ret': 'success',
            'data': {
                'mode': 'hls_vod',
                'playlist_url': playlist_url,
                'path': file_path,
                'requested_resolution': requested_resolution,
                'quality': quality,
                'start_seconds': start_seconds,
                'source': source_info,
                'duration': job['source_duration'],
                'transcode_codec': P.ModelSetting.get('transcode_codec'),
                'subtitle_urls': subtitle_urls,
            }
        })
        response.headers['X-KodiGds-VodPlaylist'] = playlist_url
        return response

    def _probe_video(self, target, ffmpeg_path):
        ffprobe_path = self._resolve_ffprobe_path(ffmpeg_path)
        cmd = [
            ffprobe_path, '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height:format=duration',
            '-of', 'json', target
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
        if proc.returncode != 0:
            raise Exception(proc.stderr.strip() or 'ffprobe failed')
        data = json.loads(proc.stdout or '{}')
        streams = data.get('streams') or []
        if not streams:
            raise Exception('video stream not found')
        stream = streams[0]
        duration = 0.0
        try:
            duration = float((data.get('format') or {}).get('duration') or 0)
        except Exception:
            duration = 0.0
        return {
            'width': int(stream.get('width') or 0),
            'height': int(stream.get('height') or 0),
            'duration': duration,
        }

    def _resolve_ffprobe_path(self, ffmpeg_path):
        dirname = os.path.dirname(ffmpeg_path)
        basename = os.path.basename(ffmpeg_path)
        candidate = os.path.join(dirname, basename.replace('ffmpeg', 'ffprobe'))
        return candidate if candidate and os.path.exists(candidate) else 'ffprobe'

    def _decide_play_mode(self, requested_resolution, source_info):
        if requested_resolution == 'original':
            return 'directplay'
        target = self.resolution_map.get(requested_resolution)
        if not target:
            return 'directplay'
        source_height = source_info.get('height') or 0
        if source_height and target['height'] >= source_height:
            return 'directplay'
        return 'transcode'

    def _make_directplay_url(self, req, file_path):
        base = req.url_root.rstrip('/')
        query = self._request_auth_query(req)
        query['path'] = file_path
        return f'{base}/{P.package_name}/api/directplay?{urlencode(query)}'

    def _make_transcode_stream_url(self, req, file_path, requested_resolution, quality, start_seconds):
        base = req.url_root.rstrip('/')
        query = self._request_auth_query(req)
        query['path'] = file_path
        query['resolution'] = requested_resolution
        query['quality'] = quality
        query['start_seconds'] = self._format_seek_seconds(start_seconds)
        return f'{base}/{P.package_name}/api/transcode_stream?{urlencode(query)}'

    def _directplay(self, req):
        file_path = req.args.get('path', '')
        _, target = self._resolve_target_path(file_path)
        if not os.path.isfile(target):
            abort(404)
        mime, _ = mimetypes.guess_type(target)
        return send_file(target, mimetype=mime or 'application/octet-stream', conditional=True, as_attachment=False)

    def _transcode_stream(self, req):
        file_path = req.args.get('path', '')
        _, target = self._resolve_target_path(file_path)
        if not os.path.isfile(target):
            abort(404)

        requested_resolution = (req.args.get('resolution') or 'original').lower()
        quality = (req.args.get('quality') or 'medium').lower()
        start_seconds = self._parse_start_seconds(req.args.get('start_seconds', '0'))
        ffmpeg_path = P.ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            raise Exception('ffmpeg_path is empty')

        source_info = self._probe_video(target, ffmpeg_path)
        codec = (P.ModelSetting.get('transcode_codec') or 'h264').lower()
        encoder = self._select_encoder(codec)
        target_profile = self.resolution_map.get(requested_resolution)
        target_scale = None
        if target_profile and source_info.get('height', 0) > target_profile['height']:
            target_scale = self._make_target_scale(source_info, target_profile)

        cmd = [ffmpeg_path, '-hide_banner', '-loglevel', 'warning', '-y']
        if start_seconds > 0:
            cmd.extend(['-ss', self._format_seek_seconds(start_seconds)])
        cmd.extend(['-i', target])
        cmd.extend(self._build_video_args(codec, encoder, quality, target_scale))
        if target_profile and target_scale:
            cmd.extend(['-b:v', target_profile['bitrate'], '-maxrate', target_profile['maxrate'], '-bufsize', target_profile['bufsize']])
        cmd.extend([
            '-c:a', 'aac',
            '-b:a', '128k',
            '-f', 'matroska',
            'pipe:1',
        ])
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        P.logger.info('Kodi transcode stream start: %s', ' '.join(cmd))
        self._start_stream_log_thread(process, file_path)

        def generate():
            try:
                while True:
                    chunk = process.stdout.read(1024 * 256)
                    if not chunk:
                        break
                    yield chunk
            finally:
                try:
                    if process.poll() is None:
                        process.kill()
                except Exception:
                    pass

        return Response(generate(), mimetype='video/x-matroska', direct_passthrough=True)

    def _ensure_vod_job(self, target, requested_resolution, quality, start_seconds, ffmpeg_path, source_info):
        job_id = secrets.token_hex(16)
        vod_root = os.path.join(F.config['path_data'], 'tmp', P.package_name, 'vod', job_id)
        os.makedirs(vod_root, exist_ok=True)
        source_duration = float(source_info.get('duration') or 0)
        start_seconds = min(max(float(start_seconds or 0), 0.0), max(source_duration - 0.001, 0.0))
        start_index = int(start_seconds // self.segment_duration) if self.segment_duration > 0 else 0
        codec = (P.ModelSetting.get('transcode_codec') or 'h264').lower()
        encoder = self._select_encoder(codec)
        target_profile = self.resolution_map.get(requested_resolution)
        target_scale = None
        if target_profile and source_info.get('height', 0) > target_profile['height']:
            target_scale = self._make_target_scale(source_info, target_profile)
        job = {
            'job_id': job_id,
            'target': target,
            'requested_resolution': requested_resolution,
            'quality': quality,
            'source_duration': source_duration,
            'start_seconds': start_seconds,
            'start_index': start_index,
            'codec': codec,
            'encoder': encoder,
            'target_profile': target_profile,
            'target_scale': target_scale,
            'ffmpeg_path': ffmpeg_path,
            'root': vod_root,
            'pending_segments': {},
            'touched_at': time.time(),
        }
        with self.vod_lock:
            self.vod_jobs[job_id] = job
        return job

    def _make_vod_url(self, req, job_id, filename):
        base = req.url_root.rstrip('/')
        return f'{base}/{P.package_name}/api/vod?job_id={job_id}&file={quote(filename)}'

    def _serve_vod(self, req):
        job_id = req.args.get('job_id', '')
        filename = req.args.get('file', 'playlist.m3u8') or 'playlist.m3u8'
        with self.vod_lock:
            job = self.vod_jobs.get(job_id)
        if not job:
            abort(404)
        job['touched_at'] = time.time()
        if filename == 'playlist.m3u8':
            # segment_0ë§Œ ë™ê¸° ìƒì„± (Kodiê°€ playlist ì§í›„ ì¦‰ì‹œ ìš”ì²­)
            start_index = int(job.get('start_index', 0) or 0)
            self._ensure_vod_segment(job, start_index)
            # ë‚˜ë¨¸ì§€ëŠ” ë°±ê·¸ë¼ìš´ë“œ prefetch
            self._prefetch_vod_segments(job, start_index + 1)
            return Response(self._build_vod_playlist(req, job), mimetype='application/vnd.apple.mpegurl')
        if not filename.startswith('segment_') or not filename.endswith('.ts'):
            abort(404)
        try:
            segment_index = int(filename[len('segment_'):-len('.ts')])
        except Exception:
            abort(404)
        segment_path = self._ensure_vod_segment(job, segment_index)
        self._prefetch_vod_segments(job, segment_index + 1)
        return send_file(segment_path, mimetype='video/mp2t', conditional=True, as_attachment=False)

    def _build_vod_playlist(self, req, job):
        import math
        source_duration = job['source_duration']
        total_segments = int((source_duration + self.segment_duration - 0.000001) // self.segment_duration)
        target_duration = math.ceil(self.segment_duration)
        # MEDIA-SEQUENCEëŠ” í•­ìƒ 0: Kodiê°€ ì „ì²´ íƒ€ìž„ë¼ì¸(0~duration)ì„ ì¸ì‹í•´ì•¼
        # seek barê°€ ì˜¬ë°”ë¥¸ ì´ ê¸¸ì´ë¡œ í‘œì‹œë˜ê³  StartOffsetìœ¼ë¡œ ì´ì–´ë³´ê¸° ìœ„ì¹˜ì— ì •í™•ížˆ ìœ„ì¹˜í•¨.
        # start_indexë¶€í„° ë‚˜ì—´í•˜ë©´ Kodiê°€ ì´ ì‹œê°„ì„ (duration - start_seconds)ë¡œ ê³„ì‚°í•˜ëŠ” ë²„ê·¸ ë°œìƒ.
        lines = [
            '#EXTM3U',
            '#EXT-X-VERSION:3',
            f'#EXT-X-TARGETDURATION:{target_duration}',
            '#EXT-X-MEDIA-SEQUENCE:0',
            '#EXT-X-PLAYLIST-TYPE:VOD',
        ]
        for index in range(0, total_segments):
            seg_start = index * self.segment_duration
            remaining = max(source_duration - seg_start, 0.0)
            seg_duration = min(self.segment_duration, remaining)
            if seg_duration <= 0:
                break
            lines.append(f'#EXTINF:{seg_duration:.3f},')
            lines.append(self._make_vod_url(req, job['job_id'], f'segment_{index}.ts'))
        lines.append('#EXT-X-ENDLIST')
        return '\n'.join(lines) + '\n'

    def _ensure_vod_segment(self, job, segment_index):
        segment_name = f'segment_{segment_index}.ts'
        segment_path = os.path.join(job['root'], segment_name)
        if os.path.exists(segment_path) and os.path.getsize(segment_path) > 0:
            return segment_path

        with self.vod_lock:
            pending = job['pending_segments'].get(segment_index)
            if pending is None:
                pending = {'event': threading.Event(), 'error': None}
                job['pending_segments'][segment_index] = pending
                owner = True
            else:
                owner = False

        if owner:
            try:
                self._generate_vod_segment(job, segment_index, segment_path)
            except Exception as e:
                pending['error'] = e
            finally:
                pending['event'].set()
                with self.vod_lock:
                    job['pending_segments'].pop(segment_index, None)
        else:
            pending['event'].wait()

        if pending.get('error') is not None:
            raise pending['error']
        if not os.path.exists(segment_path):
            raise Exception('segment generation failed')
        return segment_path

    def _prefetch_vod_segments(self, job, start_index):
        max_index = int((job['source_duration'] + self.segment_duration - 0.000001) // self.segment_duration)
        for segment_index in range(start_index, min(start_index + self.vod_prefetch_count, max_index)):
            segment_name = f'segment_{segment_index}.ts'
            segment_path = os.path.join(job['root'], segment_name)
            if os.path.exists(segment_path) and os.path.getsize(segment_path) > 0:
                continue
            with self.vod_lock:
                if segment_index in job['pending_segments']:
                    continue
            thread = threading.Thread(
                target=self._prefetch_vod_segment_worker,
                args=(job, segment_index),
                daemon=True,
            )
            thread.start()

    def _prime_vod_segments(self, job, start_index):
        max_index = int((job['source_duration'] + self.segment_duration - 0.000001) // self.segment_duration)
        for segment_index in range(start_index, min(start_index + self.vod_initial_segments, max_index)):
            try:
                self._ensure_vod_segment(job, segment_index)
            except Exception as e:
                P.logger.warning('VOD prime failed [%s:%s] %s', job['job_id'], segment_index, str(e))
                break

    def _prefetch_vod_segment_worker(self, job, segment_index):
        try:
            self._ensure_vod_segment(job, segment_index)
        except Exception as e:
            P.logger.warning('VOD prefetch failed [%s:%s] %s', job['job_id'], segment_index, str(e))

    def _generate_vod_segment(self, job, segment_index, segment_path):
        # segment_index is absolute (0 = file start), independent of start_seconds.
        start_seconds = segment_index * self.segment_duration
        if start_seconds >= job['source_duration']:
            raise Exception('segment out of range')
        duration = min(self.segment_duration, max(job['source_duration'] - start_seconds, 0.0))
        if duration <= 0:
            raise Exception('segment duration invalid')

        # Two-stage seek: coarse input-side (keyframe snap) + fine output-side.
        keyframe_ss = max(start_seconds - 5.0, 0.0)
        fine_ss = start_seconds - keyframe_ss  # 0-5 s, decoded precisely

        cmd = [job['ffmpeg_path'], '-hide_banner', '-loglevel', 'warning', '-y']
        if keyframe_ss > 0:
            cmd.extend(['-ss', self._format_seek_seconds(keyframe_ss)])
        cmd.extend(['-i', job['target']])
        if fine_ss > 0:
            cmd.extend(['-ss', self._format_seek_seconds(fine_ss)])
        cmd.extend(['-t', self._format_seek_seconds(duration)])
        # output_ts_offset = absolute position in source file.
        # EXT-X-MEDIA-SEQUENCE in the playlist tells Kodi to expect these PTS values,
        # so it can seek freely in both directions within the full timeline.
        cmd.extend(['-output_ts_offset', self._format_seek_seconds(start_seconds)])
        cmd.extend(self._build_video_args(job['codec'], job['encoder'], job['quality'], job['target_scale']))
        if job['target_profile'] and job['target_scale']:
            cmd.extend([
                '-b:v', job['target_profile']['bitrate'],
                '-maxrate', job['target_profile']['maxrate'],
                '-bufsize', job['target_profile']['bufsize'],
            ])
        cmd.extend([
            '-c:a', 'aac',
            '-b:a', '128k',
            '-avoid_negative_ts', 'disabled',
            '-f', 'mpegts',
            segment_path,
        ])
        P.logger.info('Kodi VOD segment start: %s', ' '.join(cmd))
        proc = subprocess.run(cmd, capture_output=True, timeout=120, check=False)
        if proc.returncode != 0 or not os.path.exists(segment_path):
            log_text = (proc.stderr or proc.stdout or b'').decode('utf-8', errors='replace').strip()
            raise Exception(log_text or 'segment transcoding failed')

    def _ensure_hls_job(self, target, requested_resolution, quality, start_seconds, ffmpeg_path, source_info):
        job_id = secrets.token_hex(16)
        hls_root = os.path.join(F.config['path_data'], 'tmp', P.package_name, job_id)
        playlist_path = os.path.join(hls_root, 'index.m3u8')
        with self.hls_lock:
            os.makedirs(hls_root, exist_ok=True)
            self._cleanup_hls_root(hls_root)
            cmd = self._build_hls_command(ffmpeg_path, target, requested_resolution, quality, start_seconds, hls_root, source_info)
            process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            job = {
                'job_id': job_id,
                'target': target,
                'requested_resolution': requested_resolution,
                'quality': quality,
                'start_seconds': start_seconds,
                'root': hls_root,
                'playlist_path': playlist_path,
                'process': process,
                'touched_at': time.time(),
            }
            self.hls_jobs[job_id] = job
            self._start_hls_log_thread(job)
            P.logger.info('Kodi HLS job start: %s', ' '.join(cmd))
            return job

    def _build_hls_command(self, ffmpeg_path, target, requested_resolution, quality, start_seconds, hls_root, source_info):
        playlist_path = os.path.join(hls_root, 'index.m3u8')
        segment_pattern = os.path.join(hls_root, 'segment_%05d.ts')
        codec = (P.ModelSetting.get('transcode_codec') or 'h264').lower()
        encoder = self._select_encoder(codec)
        cmd = [ffmpeg_path, '-hide_banner', '-loglevel', 'warning', '-y']
        if start_seconds > 0:
            cmd.extend(['-ss', self._format_seek_seconds(start_seconds)])
        cmd.extend(['-i', target])
        target_profile = self.resolution_map.get(requested_resolution)
        target_scale = None
        should_scale = bool(target_profile and source_info.get('height', 0) > target_profile['height'])
        if should_scale:
            target_scale = self._make_target_scale(source_info, target_profile)
        cmd.extend(self._build_video_args(codec, encoder, quality, target_scale))
        if target_profile and should_scale:
            cmd.extend(['-b:v', target_profile['bitrate'], '-maxrate', target_profile['maxrate'], '-bufsize', target_profile['bufsize']])
        cmd.extend(['-c:a', 'aac', '-b:a', '128k'])
        cmd.extend([
            '-f', 'hls',
            '-hls_time', '6',
            '-hls_list_size', '20',
            '-hls_playlist_type', 'event',
            '-hls_flags', 'independent_segments',
            '-hls_segment_filename', segment_pattern,
            playlist_path,
        ])
        return cmd

    def _parse_start_seconds(self, value):
        try:
            result = float(value or 0)
        except Exception:
            result = 0.0
        return 0.0 if result < 0 else result

    def _format_seek_seconds(self, value):
        return '{:.3f}'.format(self._parse_start_seconds(value))

    def _build_video_args(self, codec, encoder, quality, target_scale=None):
        preset_pack = self.quality_preset_map.get(quality or 'medium', self.quality_preset_map['medium'])
        scale_filter = None if not target_scale else f'scale={target_scale["width"]}:{target_scale["height"]}'
        if codec == 'h265':
            if encoder == 'hevc_nvenc':
                args = ['-c:v', encoder, '-preset', preset_pack['nvenc'], '-pix_fmt', 'nv12']
                if scale_filter:
                    args.extend(['-vf', scale_filter])
                return args
            if encoder == 'hevc_qsv':
                args = ['-c:v', encoder]
                if scale_filter:
                    args.extend(['-vf', scale_filter])
                return args
            if encoder == 'hevc_vaapi':
                device = P.ModelSetting.get('transcode_vaapi_device')
                args = []
                if device:
                    args.extend(['-vaapi_device', device])
                vf = ''
                if target_scale:
                    vf += f'scale={target_scale["width"]}:{target_scale["height"]},'
                vf += 'format=nv12,hwupload'
                args.extend(['-vf', vf, '-c:v', encoder])
                return args
            args = ['-c:v', 'libx265', '-preset', preset_pack['cpu']]
            if scale_filter:
                args.extend(['-vf', scale_filter])
            return args
        if encoder == 'h264_nvenc':
            args = ['-c:v', encoder, '-preset', preset_pack['nvenc'], '-pix_fmt', 'nv12']
            if scale_filter:
                args.extend(['-vf', scale_filter])
            return args
        if encoder == 'h264_qsv':
            args = ['-c:v', encoder]
            if scale_filter:
                args.extend(['-vf', scale_filter])
            return args
        if encoder == 'h264_vaapi':
            device = P.ModelSetting.get('transcode_vaapi_device')
            args = []
            if device:
                args.extend(['-vaapi_device', device])
            vf = ''
            if target_scale:
                vf += f'scale={target_scale["width"]}:{target_scale["height"]},'
            vf += 'format=nv12,hwupload'
            args.extend(['-vf', vf, '-c:v', encoder])
            return args
        args = ['-c:v', 'libx264', '-preset', preset_pack['cpu']]
        if scale_filter:
            args.extend(['-vf', scale_filter])
        return args

    def _make_target_scale(self, source_info, target_profile):
        source_width = int(source_info.get('width') or 0)
        source_height = int(source_info.get('height') or 0)
        target_height = int(target_profile['height'])
        if source_width <= 0 or source_height <= 0:
            return {'width': int(target_profile['width']), 'height': target_height}

        raw_width = int(round((float(source_width) * target_height) / float(source_height)))
        if raw_width < 2:
            raw_width = 2
        target_width = raw_width if raw_width % 2 == 0 else raw_width + 1
        return {'width': target_width, 'height': target_height}

    def _select_encoder(self, codec):
        ffmpeg_path = P.ModelSetting.get('ffmpeg_path')
        saved = P.ModelSetting.get(self._saved_encoder_key(codec))
        if saved:
            return saved

        encoders = subprocess.run([ffmpeg_path, '-hide_banner', '-encoders'], capture_output=True, text=True, timeout=20, check=False)
        text = encoders.stdout if encoders.returncode == 0 else ''
        for token in self._candidate_encoders(codec):
            if token in text and self._encoder_is_usable(ffmpeg_path, token):
                return token
        return 'libx265' if codec == 'h265' else 'libx264'

    def _candidate_encoders(self, codec):
        if codec == 'h265':
            return ['hevc_nvenc', 'hevc_qsv', 'hevc_vaapi', 'libx265']
        return ['h264_nvenc', 'h264_qsv', 'h264_vaapi', 'libx264']

    def _saved_encoder_key(self, codec):
        return 'transcode_h265_encoder' if codec == 'h265' else 'transcode_h264_encoder'

    def _encoder_is_usable(self, ffmpeg_path, encoder, force=False, with_device=False):
        if force:
            self.encoder_probe_cache.pop(encoder, None)
        cached = self.encoder_probe_cache.get(encoder)
        if cached is not None:
            return (cached, '') if with_device else cached

        if encoder.endswith('_vaapi'):
            for device in self._list_vaapi_devices():
                cmd = self._build_encoder_probe_command(ffmpeg_path, encoder, vaapi_device=device)
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
                if proc.returncode == 0:
                    self.encoder_probe_cache[encoder] = True
                    return (True, device) if with_device else True
                log_text = (proc.stderr or proc.stdout or '').strip()
                P.logger.warning('Encoder probe failed [%s][%s]: %s', encoder, device, log_text)
            self.encoder_probe_cache[encoder] = False
            return (False, '') if with_device else False

        cmd = self._build_encoder_probe_command(ffmpeg_path, encoder)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
        ok = proc.returncode == 0
        self.encoder_probe_cache[encoder] = ok
        if not ok:
            log_text = (proc.stderr or proc.stdout or '').strip()
            P.logger.warning('Encoder probe failed [%s]: %s', encoder, log_text)
        return (ok, '') if with_device else ok

    def _build_encoder_probe_command(self, ffmpeg_path, encoder, vaapi_device=None):
        base = [ffmpeg_path, '-hide_banner', '-loglevel', 'error']

        if encoder in ('h264_vaapi', 'hevc_vaapi'):
            return base + [
                '-vaapi_device', vaapi_device,
                '-f', 'lavfi',
                '-i', 'testsrc2=size=1280x720:rate=30',
                '-vf', 'format=nv12,hwupload',
                '-frames:v', '30',
                '-an',
                '-c:v', encoder,
                '-f', 'null', '-',
            ]

        if encoder in ('h264_qsv', 'hevc_qsv'):
            return base + [
                '-f', 'lavfi',
                '-i', 'testsrc2=size=1280x720:rate=30',
                '-frames:v', '30',
                '-an',
                '-c:v', encoder,
                '-f', 'null', '-',
            ]

        cmd = base + [
            '-f', 'lavfi',
            '-i', 'testsrc2=size=1280x720:rate=30',
            '-frames:v', '30',
            '-an',
            '-c:v', encoder,
        ]
        if encoder in ('h264_nvenc', 'hevc_nvenc'):
            cmd.extend(['-preset', 'medium', '-pix_fmt', 'nv12'])
        cmd.extend(['-f', 'null', '-'])
        return cmd

    def _list_vaapi_devices(self):
        dri_root = '/dev/dri'
        if not os.path.isdir(dri_root):
            return []
        devices = []
        for name in sorted(os.listdir(dri_root)):
            if name.startswith('renderD'):
                fullpath = os.path.join(dri_root, name)
                if os.path.exists(fullpath):
                    devices.append(fullpath)
        return devices

    def _wait_for_playlist(self, job):
        for _ in range(300):
            if os.path.exists(job['playlist_path']) and os.path.getsize(job['playlist_path']) > 0:
                return
            if job['process'].poll() is not None:
                break
            time.sleep(0.2)
        raise Exception('HLS playlist creation failed')

    def _make_hls_url(self, req, job_id, filename):
        base = req.url_root.rstrip('/')
        return f'{base}/{P.package_name}/api/hls?job_id={job_id}&file={quote(filename)}'

    def _serve_hls(self, sub, req):
        if sub == 'hls':
            job_id = req.args.get('job_id', '')
            filename = req.args.get('file', 'index.m3u8') or 'index.m3u8'
        else:
            parts = sub.split('/')
            if len(parts) < 2:
                abort(404)
            _, job_id = parts[0], parts[1]
            filename = 'index.m3u8' if len(parts) < 3 or parts[2] == '' else '/'.join(parts[2:])
        job = self.hls_jobs.get(job_id)
        if not job:
            abort(404)
        target = os.path.abspath(os.path.join(job['root'], filename))
        if os.path.commonpath([job['root'], target]) != job['root']:
            abort(403)
        if not os.path.exists(target):
            abort(404)
        if filename.endswith('.m3u8'):
            with open(target, 'r', encoding='utf-8') as f:
                data = self._rewrite_playlist(f.read(), job)
            return Response(data, mimetype='application/vnd.apple.mpegurl')
        with open(target, 'rb') as f:
            data = f.read()
        mimetype = 'video/mp2t' if filename.endswith('.ts') else 'application/vnd.apple.mpegurl'
        return Response(data, mimetype=mimetype)

    def _hls_status(self, req):
        job_id = req.args.get('job_id', '')
        job = self.hls_jobs.get(job_id)
        if not job:
            return {'ret': 'warning', 'msg': 'job not found'}
        return {
            'ret': 'success',
            'data': {
                'job_id': job_id,
                'playlist_exists': os.path.exists(job['playlist_path']),
                'running': job['process'].poll() is None,
            }
        }

    def _cleanup_hls_root(self, hls_root):
        for entry in os.scandir(hls_root):
            if entry.is_file():
                try:
                    os.remove(entry.path)
                except Exception:
                    pass

    def _rewrite_playlist(self, content, job):
        lines = []
        for line in content.splitlines():
            if line and not line.startswith('#'):
                line = f'{job["base_hls_url"]}?job_id={job["job_id"]}&file={quote(line)}'
            lines.append(line)
        return '\n'.join(lines) + '\n'

    def _start_hls_log_thread(self, job):
        def worker():
            try:
                while True:
                    line = job['process'].stderr.readline()
                    if not line:
                        break
                    P.logger.warning('HLS[%s] %s', job['job_id'], line.decode('utf-8', errors='replace').rstrip())
            except Exception as e:
                P.logger.error(f'Exception:{str(e)}')
                P.logger.error(traceback.format_exc())

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

    def _start_stream_log_thread(self, process, file_path):
        def worker():
            try:
                while True:
                    line = process.stderr.readline()
                    if not line:
                        break
                    P.logger.warning('STREAM[%s] %s', os.path.basename(file_path), line.decode('utf-8', errors='replace').rstrip())
            except Exception as e:
                P.logger.error(f'Exception:{str(e)}')
                P.logger.error(traceback.format_exc())

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()


for _metadata_method_name in (
    '_metadata_db_path',
    '_metadata_series_key',
    '_ensure_metadata_table',
    '_metadata_get_cached',
    '_metadata_save_cached',
    '_normalize_art_value',
    '_make_meta_art_url',
    '_make_metadata_proxy_url',
    '_base_url_from_req',
    '_normalize_series_name',
    '_extract_embedded_metadata_code',
    '_metadata_module_hints',
    '_fetch_json_url',
    '_metadata_search_code',
    '_extract_art_fields',
    '_extract_episode_keys',
    '_extract_episode_thumb_map',
    '_lookup_metadata_cache',
    '_lookup_metadata_cache_by_base',
    '_schedule_metadata_prefetch',
    '_metadata_prefetch_worker',
    '_serve_meta_art',
):
    setattr(KodisPlayMixin, _metadata_method_name, getattr(KodisMetadataMixin, _metadata_method_name))


