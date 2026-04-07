import json
import secrets
import threading
import time
import traceback

from flask import jsonify

from .setup import *  # pylint: disable=wildcard-import,unused-wildcard-import
from .kodis_play import KodisPlayMixin

name = 'auth'


class KodisAuthMixin(object):
    auth_sessions = {}
    auth_lock = threading.Lock()
    auth_session_ttl = 60 * 60 * 12

    def _fixed_profile_ids(self):
        return ['profile_1', 'profile_2', 'profile_3', 'profile_4', 'profile_5']

    def _default_profile_entry(self, profile_id='profile_1', password='', show_av=None):
        normalized_id = str(profile_id or '').strip() or 'profile_1'
        try:
            index = max(1, int(normalized_id.split('_')[-1]))
        except Exception:
            index = 1
        return {
            'profile_id': normalized_id,
            'name': '프로필{}'.format(index),
            'password': str(password or '').strip(),
            'enabled': True if normalized_id == 'profile_1' else False,
            'show_av': bool(False if show_av is None else show_av),
            'use_custom_root': False,
        }

    def _normalize_profiles(self, profiles):
        source_map = {}
        for index, item in enumerate(profiles or []):
            if not isinstance(item, dict):
                continue
            profile_id = str(item.get('profile_id') or '').strip() or 'profile_{}'.format(index + 1)
            if profile_id in self._fixed_profile_ids():
                source_map[profile_id] = item
                continue
            # Legacy migration: older data used profile_id as the actual Kodi auth id.
            fixed_profile_id = 'profile_{}'.format(index + 1)
            if fixed_profile_id not in self._fixed_profile_ids():
                continue
            migrated = dict(item)
            migrated['profile_id'] = fixed_profile_id
            if not str(migrated.get('password') or '').strip():
                migrated['password'] = profile_id
            source_map[fixed_profile_id] = migrated

        result = []
        for index, profile_id in enumerate(self._fixed_profile_ids(), start=1):
            item = source_map.get(profile_id, {})
            default_entry = self._default_profile_entry(profile_id=profile_id, password='', show_av=False)
            result.append({
                'profile_id': profile_id,
                'name': str(item.get('name') or '').strip() or default_entry['name'],
                'password': str(item.get('password') or '').strip(),
                'enabled': str(item.get('enabled', default_entry['enabled'])).strip().lower() not in ('false', '0', 'no', 'off'),
                'show_av': str(item.get('show_av', default_entry['show_av'])).strip().lower() in ('true', '1', 'yes', 'on'),
                'use_custom_root': str(item.get('use_custom_root', False)).strip().lower() in ('true', '1', 'yes', 'on'),
            })
        if not any(bool(profile.get('enabled', True)) for profile in result):
            result[0]['enabled'] = True
        return result

    def _bootstrap_profiles_setting(self):
        raw = P.ModelSetting.get('profiles_json') or ''
        try:
            loaded = json.loads(raw) if str(raw).strip() else []
        except Exception:
            loaded = []
        normalized = self._normalize_profiles(loaded if isinstance(loaded, list) else [])
        if normalized:
            normalized_raw = json.dumps(normalized, ensure_ascii=False)
            if normalized_raw != str(raw or ''):
                P.ModelSetting.set('profiles_json', normalized_raw)
            P.logger.info('Profile bootstrap skipped existing_profiles=%s', len(normalized))
            return False
        legacy_password = str(P.ModelSetting.get('access_password') or '').strip()
        bootstrap_profiles = self._normalize_profiles([
            self._default_profile_entry(profile_id='profile_1', password=legacy_password, show_av=False)
        ])
        P.ModelSetting.set('profiles_json', json.dumps(bootstrap_profiles, ensure_ascii=False))
        P.logger.info('Profile bootstrap completed created_profile_id=profile_1')
        return True

    def _make_profile_credential_key(self, profile):
        system_apikey = F.SystemModelSetting.get('apikey') or ''
        profile_id = str((profile or {}).get('profile_id') or '').strip()
        password = str((profile or {}).get('password') or '').strip()
        enabled = '1' if bool((profile or {}).get('enabled', True)) else '0'
        return '{}|{}|{}|{}'.format(system_apikey, profile_id, password, enabled)

    def _resolve_request_object(self, req):
        try:
            return req._get_current_object()
        except Exception:
            return req

    def _set_request_profile(self, req, profile):
        req = self._resolve_request_object(req)
        try:
            setattr(req, '_ff_profile', dict(profile or {}))
        except Exception:
            pass

    def _get_request_profile(self, req):
        req = self._resolve_request_object(req)
        profile = getattr(req, '_ff_profile', None)
        return dict(profile or {}) if isinstance(profile, dict) else None

    def _load_profiles(self):
        raw = P.ModelSetting.get('profiles_json') or '[]'
        try:
            loaded = json.loads(raw) if str(raw).strip() else []
        except Exception:
            loaded = []
        normalized = self._normalize_profiles(loaded if isinstance(loaded, list) else [])
        if normalized:
            return normalized
        legacy_password = str(P.ModelSetting.get('access_password') or '').strip()
        return self._normalize_profiles([
            self._default_profile_entry(profile_id='profile_1', password=legacy_password, show_av=False)
        ])

    def _find_profile_by_password(self, password, allow_disabled=False):
        target = str(password or '').strip()
        for profile in self._load_profiles():
            profile_id = str(profile.get('profile_id') or '').strip()
            profile_password = str(profile.get('password') or '').strip()
            if target == '':
                if profile_id != 'profile_1':
                    continue
                if profile_password != '':
                    continue
            elif target != profile_password:
                continue
            if (not allow_disabled) and (not bool(profile.get('enabled', True))):
                return None
            return dict(profile)
        return None

    def _find_profile_by_id(self, profile_id, allow_disabled=False):
        target = str(profile_id or '').strip()
        if target == '':
            return None
        for profile in self._load_profiles():
            if target != str(profile.get('profile_id') or '').strip():
                continue
            if (not allow_disabled) and (not bool(profile.get('enabled', True))):
                return None
            return dict(profile)
        return None

    def _request_value(self, req, *keys):
        req = self._resolve_request_object(req)
        for key in keys:
            if hasattr(req, 'headers'):
                value = req.headers.get(key)
                if value not in (None, ''):
                    return value
            if hasattr(req, 'args'):
                value = req.args.get(key)
                if value not in (None, ''):
                    return value
            if hasattr(req, 'form'):
                value = req.form.get(key)
                if value not in (None, ''):
                    return value
        return ''

    def _require_api_key(self, req, allow_session=True, require_password=True):
        system_apikey = F.SystemModelSetting.get('apikey')
        if not system_apikey:
            from flask import abort
            abort(403)
        request_apikey = self._request_value(req, 'X-FF-ApiKey', 'X-Api-Key', 'apikey')
        if request_apikey != system_apikey:
            from flask import abort
            abort(403)
        if allow_session and self._has_valid_session(req):
            return
        if not require_password:
            return
        self._require_access_password(req)

    def _require_access_password(self, req):
        profiles = self._load_profiles()
        if not profiles:
            return
        request_password = self._request_value(req, 'X-FF-Password', 'password')
        profile = self._find_profile_by_password(request_password, allow_disabled=False)
        if profile is None:
            from flask import abort
            abort(403)
        self._set_request_profile(req, profile)
        return profile

    def _request_auth_query(self, req):
        apikey = self._request_value(req, 'X-FF-ApiKey', 'X-Api-Key', 'apikey')
        password = self._request_value(req, 'X-FF-Password', 'password')
        session_token = self._request_value(req, 'X-FF-Session', 'session_token')
        query = {'apikey': apikey}
        if session_token:
            query['session_token'] = session_token
        elif password:
            query['password'] = password
        return query

    def _cleanup_auth_sessions(self):
        now = time.time()
        with self.auth_lock:
            expired = []
            for token, stored in self.auth_sessions.items():
                if isinstance(stored, dict):
                    expires_at = float(stored.get('expires_at') or 0)
                else:
                    expires_at = float(stored or 0)
                if expires_at <= now:
                    expired.append(token)
            for token in expired:
                self.auth_sessions.pop(token, None)

    def _has_valid_session(self, req):
        token = self._request_value(req, 'X-FF-Session', 'session_token').strip()
        if token == '':
            return False
        self._cleanup_auth_sessions()
        with self.auth_lock:
            stored = self.auth_sessions.get(token)
            if isinstance(stored, dict):
                expires_at = float(stored.get('expires_at') or 0)
                profile_id = str(stored.get('profile_id') or '')
            else:
                expires_at = float(stored or 0)
                profile_id = ''
            if not expires_at or expires_at <= time.time():
                self.auth_sessions.pop(token, None)
                return False
            profile = self._find_profile_by_id(profile_id, allow_disabled=False)
            if profile is None:
                self.auth_sessions.pop(token, None)
                return False
            credential_key = self._make_profile_credential_key(profile)
            stored_credential_key = str(stored.get('credential_key') or '') if isinstance(stored, dict) else ''
            if stored_credential_key != credential_key:
                self.auth_sessions.pop(token, None)
                return False
            self.auth_sessions[token] = {
                'expires_at': time.time() + self.auth_session_ttl,
                'credential_key': credential_key,
                'profile_id': profile_id,
            }
        self._set_request_profile(req, profile)
        return True

    def _auth_issue_session(self, req):
        profile = self._get_request_profile(req) or self._require_access_password(req) or {}
        token = secrets.token_urlsafe(24)
        credential_key = self._make_profile_credential_key(profile)
        with self.auth_lock:
            self.auth_sessions[token] = {
                'expires_at': time.time() + self.auth_session_ttl,
                'credential_key': credential_key,
                'profile_id': str(profile.get('profile_id') or ''),
            }
        return {
            'ret': 'success',
            'data': {
                'session_token': token,
                'expires_in': self.auth_session_ttl,
                'profile_id': str(profile.get('profile_id') or ''),
                'profile_name': str(profile.get('name') or ''),
                'show_av': bool(profile.get('show_av', False)),
                'use_custom_root': bool(profile.get('use_custom_root', False)),
            }
        }


class ModuleAuth(KodisAuthMixin, KodisPlayMixin, PluginModuleBase):
    def __init__(self, P):
        super(ModuleAuth, self).__init__(P, name=name)
        self._bootstrap_profiles_setting()
        self._bootstrap_resume_migration()

    def process_api(self, sub, req):
        req = self._resolve_request_object(req)
        self._remember_base_url_from_req(req)
        try:
            if sub == 'auth':
                self._require_api_key(req, allow_session=False, require_password=True)
                return jsonify(self._auth_issue_session(req))
            self._require_api_key(req, allow_session=True, require_password=True)
            return self._process_kodis_api(sub, req)
        except Exception as e:
            if getattr(e, 'code', None) in (401, 403, 404):
                raise
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'exception', 'msg': str(e)}), 500
