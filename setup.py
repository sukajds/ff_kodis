setting = {
    'filepath': __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': 'setting',
    'menu': {
        'uri': __package__,
        'name': 'GDS for Kodi',
        'list': [
            {
                'uri': 'setting',
                'name': '설정',
            },
            {
                'uri': 'db',
                'name': 'DB툴',
            },
            {
                'uri': 'manual',
                'name': '메뉴얼',
                'list': [
                    {
                        'uri': 'README.md',
                        'name': 'README',
                    },
                ]
            },
            {
                'uri': 'log',
                'name': '로그',
            },
        ]
    },
    'default_route': 'normal',
}

from plugin import *  # pylint: disable=wildcard-import,unused-wildcard-import

P = create_plugin_instance(setting)
try:
    from .kodis_auth import ModuleAuth
    from .kodis_db import ModuleDb
    from .kodis_setting import ModuleSetting
    P.set_module_list([ModuleAuth, ModuleSetting, ModuleDb])
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())
