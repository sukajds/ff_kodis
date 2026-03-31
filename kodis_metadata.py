import json
import os
import re
import sqlite3
import threading
import time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from flask import Response, abort, redirect

from .setup import *  # pylint: disable=wildcard-import,unused-wildcard-import


class KodisMetadataMixin(object):
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
        return req.url_root.rstrip('/')

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
        movie_tokens = ('/VIDEO/영화/', '/영화/')
        forced_ftv_tokens = ('/VIDEO/외국TV/', '/외국TV/', '/VIDEO/일본 애니메이션/', '/일본 애니메이션/')
        foreign_tokens = ('/외국/', '/대만드라마/', '/중드/', '/미드/', '/영드/', '/일드/')
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
            r'(?P<ep>\d{1,4})[회화]',
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
                    P.logger.info('Metadata embedded code module=%s series=%s code=%s', module_name, series_rel, metadata_code)
                for query in build_search_queries(module_name):
                    if metadata_code:
                        break
                    search_url = f"{base}/metadata/api/{module_name}/search?{urlencode(query)}"
                    search_payload = self._fetch_json_url(search_url)
                    metadata_code, matched_title = self._metadata_search_code(module_name, search_payload)
                    P.logger.info(
                        'Metadata search module=%s series=%s query=%s code=%s matched_title=%s payload_type=%s',
                        module_name, series_rel, query, metadata_code, matched_title, type(search_payload).__name__
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
                    module_name, series_rel, metadata_code, type(info_payload).__name__
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
        _, target = self._resolve_target_path(relative_path)
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
                relative_path, series_rel, episode_keys, list(episode_map.keys())[:20]
            )
            for episode_key in episode_keys:
                image_url = episode_map.get(episode_key, '')
                if image_url:
                    P.logger.info('Meta thumb matched from cache path=%s key=%s url=%s', relative_path, episode_key, image_url)
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
                        relative_path, series_rel, episode_keys, list(episode_map.keys())[:20]
                    )
                    for episode_key in episode_keys:
                        image_url = episode_map.get(episode_key, '')
                        if image_url:
                            P.logger.info('Meta thumb matched after refresh path=%s key=%s url=%s', relative_path, episode_key, image_url)
                            break

        if not image_url:
            image_url = metadata.get('poster_url') or metadata.get('thumb_url') or metadata.get('fanart_url') or ''
            P.logger.info(
                'Meta art fallback path=%s kind=%s fallback_url=%s metadata_code=%s',
                relative_path, kind, image_url, metadata.get('metadata_code', '')
            )
        if not image_url:
            return Response(self.transparent_gif, mimetype='image/gif')
        return redirect(self._make_metadata_proxy_url(req, image_url))
 
