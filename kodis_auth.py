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

    def _session_credential_key(self):
        system_apikey = F.SystemModelSetting.get('apikey') or ''
        configured_password = (P.ModelSetting.get('access_password') or '').strip()
        return '{}|{}'.format(system_apikey, configured_password)

    def _request_value(self, req, *keys):
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
        configured_password = (P.ModelSetting.get('access_password') or '').strip()
        if configured_password == '':
            return
        request_password = self._request_value(req, 'X-FF-Password', 'password')
        if request_password != configured_password:
            from flask import abort
            abort(403)

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
        current_key = self._session_credential_key()
        with self.auth_lock:
            stored = self.auth_sessions.get(token)
            if isinstance(stored, dict):
                expires_at = float(stored.get('expires_at') or 0)
                credential_key = str(stored.get('credential_key') or '')
            else:
                expires_at = float(stored or 0)
                credential_key = ''
            if not expires_at or expires_at <= time.time():
                self.auth_sessions.pop(token, None)
                return False
            if credential_key != current_key:
                self.auth_sessions.pop(token, None)
                return False
            self.auth_sessions[token] = {
                'expires_at': time.time() + self.auth_session_ttl,
                'credential_key': current_key,
            }
        return True

    def _auth_issue_session(self, req):
        token = secrets.token_urlsafe(24)
        credential_key = self._session_credential_key()
        with self.auth_lock:
            self.auth_sessions[token] = {
                'expires_at': time.time() + self.auth_session_ttl,
                'credential_key': credential_key,
            }
        return {
            'ret': 'success',
            'data': {
                'session_token': token,
                'expires_in': self.auth_session_ttl,
            }
        }


class ModuleAuth(KodisAuthMixin, KodisPlayMixin, PluginModuleBase):
    def __init__(self, P):
        super(ModuleAuth, self).__init__(P, name=name)

    def process_api(self, sub, req):
        self._remember_base_url_from_req(req)
        try:
            if sub == 'auth':
                self._require_api_key(req, allow_session=False, require_password=True)
                return jsonify(self._auth_issue_session(req))
            return self._process_kodis_api(sub, req)
        except Exception as e:
            if getattr(e, 'code', None) in (401, 403, 404):
                raise
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'exception', 'msg': str(e)}), 500
 
