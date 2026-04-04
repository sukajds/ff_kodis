import json
import traceback

from flask import jsonify, render_template

from .setup import *  # pylint: disable=wildcard-import,unused-wildcard-import
from .kodis_auth import KodisAuthMixin
from .kodis_play import KodisPlayMixin

name = 'setting'


class ModuleSetting(KodisAuthMixin, KodisPlayMixin, PluginModuleBase):
    db_default = {
        'ffmpeg_path': 'ffmpeg',
        'gds_path': '',
        'access_password': '',
        'profiles_json': '[]',
        'db_tree_schedule_enabled': 'False',
        'db_tree_schedule': '',
        'show_av': 'False',
        'plex_db_path': '',
        'plex_import_since': '',
        'transcode_codec': 'h264',
        'transcode_h264_encoder': '',
        'transcode_h265_encoder': '',
        'transcode_vaapi_device': '',
    }

    def __init__(self, P):
        super(ModuleSetting, self).__init__(P, name=name)
        self._bootstrap_profiles_setting()
        self._bootstrap_resume_migration()
        self._start_auto_meta_worker()

    def process_menu(self, page, req):
        self._remember_base_url_from_req(req)
        arg = P.ModelSetting.to_dict()
        arg['profiles'] = self._load_profiles()
        arg['profiles_json'] = json.dumps(arg['profiles'], ensure_ascii=False)
        arg['db_tree_schedule_active'] = str(F.scheduler.is_include(self._db_tree_scheduler_job_id()))
        arg['package_name'] = P.package_name
        return render_template(f'{P.package_name}_{name}.html', arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        self._remember_base_url_from_req(req)
        try:
            if command in ('list_root', 'list'):
                return jsonify(self._list_items(req))
            if command == 'cleanup_deleted_profiles':
                return jsonify(self._cleanup_deleted_profiles(arg1))
            if command == 'sync_db_tree_schedule':
                return jsonify(self._sync_db_tree_scheduler(arg1, arg2))
            if command == 'build_db_tree_json_now':
                return jsonify(self._build_db_tree_json_now())
            if command == 'ffmpeg_version':
                return jsonify(self._ffmpeg_version())
            if command == 'transcode_capabilities':
                return jsonify(self._transcode_capabilities())
            if command == 'test_transcode_encoder':
                return jsonify(self._test_transcode_encoder())
            response = self._process_kodis_command(command, req)
            if response is not None:
                return response
            return jsonify({'ret': 'warning', 'msg': f'Unknown command: {command}'})
        except Exception as e:
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def process_api(self, sub, req):
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

    def plugin_load(self):
        try:
            self._bootstrap_profiles_setting()
            self._bootstrap_resume_migration()
            self._sync_db_tree_scheduler()
        except Exception as e:
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
 
