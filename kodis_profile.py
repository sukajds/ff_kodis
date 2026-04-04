import json
import traceback

from flask import jsonify, render_template

from .setup import *  # pylint: disable=wildcard-import,unused-wildcard-import
from .kodis_auth import KodisAuthMixin
from .kodis_play import KodisPlayMixin

name = 'profile'


class ModuleProfile(KodisAuthMixin, KodisPlayMixin, PluginModuleBase):
    def __init__(self, P):
        super(ModuleProfile, self).__init__(P, name=name)
        self._bootstrap_profiles_setting()
        self._bootstrap_resume_migration()

    def process_menu(self, page, req):
        self._remember_base_url_from_req(req)
        arg = P.ModelSetting.to_dict()
        arg['profiles'] = self._load_profiles()
        arg['profiles_json'] = json.dumps(arg['profiles'], ensure_ascii=False)
        arg['package_name'] = P.package_name
        return render_template(f'{P.package_name}_{name}.html', arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        self._remember_base_url_from_req(req)
        try:
            if command == 'save_profiles':
                return jsonify(self._save_profiles(arg1))
            if command == 'cleanup_deleted_profiles':
                return jsonify(self._cleanup_deleted_profiles(arg1))
            return jsonify({'ret': 'warning', 'msg': f'Unknown command: {command}'})
        except Exception as e:
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def _save_profiles(self, payload):
        try:
            parsed = json.loads(payload or '[]')
        except Exception:
            parsed = []
        if not isinstance(parsed, list):
            parsed = []

        normalized = self._normalize_profiles(parsed)
        if not normalized:
            normalized = self._normalize_profiles([])

        seen_passwords = set()
        for profile in normalized:
            profile_id = str(profile.get('profile_id') or '').strip()
            password = str(profile.get('password') or '').strip()
            enabled = bool(profile.get('enabled', True))
            if (not enabled) and password == '':
                continue
            if enabled and password == '' and profile_id != 'profile_1':
                return {
                    'ret': 'warning',
                    'msg': '사용 중인 프로필의 프로필 ID를 입력해주세요.',
                }
            if password:
                if password in seen_passwords:
                    return {
                        'ret': 'warning',
                        'msg': '프로필 ID는 중복될 수 없습니다.',
                    }
                seen_passwords.add(password)

        P.ModelSetting.set('profiles_json', json.dumps(normalized, ensure_ascii=False))

        return {
            'ret': 'success',
            'msg': '프로필 설정을 저장했습니다.',
            'data': {
                'profiles': normalized,
                'deleted_profiles': [],
                'deleted_resume_rows': 0,
            }
        }

    def _cleanup_deleted_profiles(self, payload):
        deleted_ids = []
        try:
            parsed = json.loads(payload or '[]')
            if isinstance(parsed, list):
                deleted_ids = [str(item or '').strip() for item in parsed]
        except Exception:
            deleted_ids = []
        deleted_total = 0
        for profile_id in deleted_ids:
            if not profile_id:
                continue
            deleted_total += int(self._delete_resume_profile(profile_id) or 0)
        return {
            'ret': 'success',
            'msg': '삭제된 프로필 정리를 완료했습니다.',
            'data': {'deleted_profiles': len([x for x in deleted_ids if x]), 'deleted_resume_rows': deleted_total},
        }
