import secrets
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
        'show_av': 'False',
        'plex_db_path': '',
        'transcode_codec': 'h264',
        'transcode_h264_encoder': '',
        'transcode_h265_encoder': '',
        'transcode_vaapi_device': '',
    }

    def __init__(self, P):
        super(ModuleSetting, self).__init__(P, name=name)
        self._start_auto_meta_worker()

    def process_menu(self, page, req):
        self._remember_base_url_from_req(req)
        arg = P.ModelSetting.to_dict()
        arg['package_name'] = P.package_name
        return render_template(f'{P.package_name}_{name}.html', arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        self._remember_base_url_from_req(req)
        try:
            req_path = getattr(req, 'path', '')
            req_args = dict(req.args) if hasattr(req, 'args') else {}
            req_form = dict(req.form) if hasattr(req, 'form') else {}
            self._diagnostic_log(f'setting.process_command command={command} path={req_path} args={req_args} form={req_form}')
            P.logger.warning(
                'kodis_setting process_command command=%s arg1=%s arg2=%s arg3=%s path=%s args=%s form=%s',
                command,
                arg1,
                arg2,
                arg3,
                req_path,
                req_args,
                req_form,
            )
            if command in ('list_root', 'list'):
                self._diagnostic_log(f'setting.process_command list command={command} gds_path={P.ModelSetting.get("gds_path")}')
                P.logger.warning('kodis_setting list_root requested gds_path=%s', P.ModelSetting.get('gds_path'))
                return jsonify(self._list_items(req))
            if command == 'generate_password':
                return jsonify(self._generate_password())
            if command == 'ffmpeg_version':
                return jsonify(self._ffmpeg_version())
            if command == 'transcode_capabilities':
                return jsonify(self._transcode_capabilities())
            if command == 'test_transcode_encoder':
                return jsonify(self._test_transcode_encoder())
            response = self._process_kodis_command(command, req)
            if response is not None:
                self._diagnostic_log(f'setting.process_command delegated command={command}')
                P.logger.warning('kodis_setting delegated command=%s to KodisPlayMixin', command)
                return response
            self._diagnostic_log(f'setting.process_command unknown command={command}')
            P.logger.warning('kodis_setting unknown command=%s path=%s args=%s form=%s', command, req_path, req_args, req_form)
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
            return self._process_kodis_api(sub, req)
        except Exception as e:
            if getattr(e, 'code', None) in (401, 403, 404):
                raise
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'exception', 'msg': str(e)}), 500

    def _generate_password(self):
        generated = secrets.token_urlsafe(12)
        P.ModelSetting.set('access_password', generated)
        return {
            'ret': 'success',
            'msg': 'Access password generated',
            'data': {'access_password': generated},
        }
