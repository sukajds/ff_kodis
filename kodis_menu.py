import json

from flask import render_template

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
        return render_template(f'{P.package_name}_{name}.html', arg=arg)

    def _load_items(self, raw_value):
        try:
            data = json.loads(raw_value or '[]')
        except Exception:
            data = []
        result = []
        for item in data:
            if not isinstance(item, dict):
                continue
            result.append({
                'name': str(item.get('name') or '').strip(),
                'path': str(item.get('path') or '').strip(),
            })
        return result
