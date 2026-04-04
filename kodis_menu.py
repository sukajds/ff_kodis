import json

from flask import jsonify, render_template

from .setup import *  # pylint: disable=wildcard-import,unused-wildcard-import

name = 'menu'


class ModuleMenu(PluginModuleBase):
    db_default = {
        'custom_root_enabled': 'False',
        'custom_root_items': '[]',
    }

    def __init__(self, P):
        super(ModuleMenu, self).__init__(P, name=name)

    def process_menu(self, page, req):
        arg = P.ModelSetting.to_dict()
        arg['package_name'] = P.package_name
        arg['custom_root_items_list'] = self._load_items(arg.get('custom_root_items', '[]'))
        arg['fixed_profiles'] = self._load_fixed_profiles()
        return render_template(f'{P.package_name}_{name}.html', arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        try:
            if command == 'save_custom_root_items':
                return jsonify(self._save_custom_root_items(arg1))
            return jsonify({'ret': 'warning', 'msg': f'Unknown command: {command}'})
        except Exception as e:
            P.logger.error(f'Exception:{str(e)}')
            P.logger.error(traceback.format_exc())
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def _load_fixed_profiles(self):
        try:
            data = json.loads(P.ModelSetting.get('profiles_json') or '[]')
        except Exception:
            data = []
        profile_map = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            profile_id = str(item.get('profile_id') or '').strip()
            if not profile_id:
                continue
            profile_map[profile_id] = item
        result = []
        for index in range(1, 6):
            profile_id = f'profile_{index}'
            item = profile_map.get(profile_id, {})
            result.append({
                'profile_id': profile_id,
                'name': str(item.get('name') or '').strip() or f'프로필{index}',
            })
        return result

    def _load_items(self, raw_value):
        try:
            data = json.loads(raw_value or '[]')
        except Exception:
            data = []
        result = []
        for item in data:
            if not isinstance(item, dict):
                continue
            profiles = item.get('profiles')
            if not isinstance(profiles, list) or not profiles:
                profiles = [f'profile_{i}' for i in range(1, 6)]
            result.append({
                'name': str(item.get('name') or '').strip(),
                'path': str(item.get('path') or '').strip(),
                'profiles': [str(x).strip() for x in profiles if str(x).strip()],
            })
        return result

    def _save_custom_root_items(self, payload):
        try:
            parsed = json.loads(payload or '[]')
        except Exception:
            parsed = []
        if not isinstance(parsed, list):
            parsed = []
        normalized = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            path = str(item.get('path') or '').strip()
            if path == '':
                continue
            profiles = item.get('profiles')
            if not isinstance(profiles, list) or not profiles:
                profiles = [f'profile_{i}' for i in range(1, 6)]
            normalized.append({
                'name': str(item.get('name') or '').strip(),
                'path': path,
                'profiles': [str(x).strip() for x in profiles if str(x).strip()],
            })
        P.ModelSetting.set('custom_root_items', json.dumps(normalized, ensure_ascii=False))
        return {'ret': 'success', 'msg': '목록 설정을 저장했습니다.'}
