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
                'uri': 'profile',
                'name': '프로필',
            },
            {
                'uri': 'menu',
                'name': '목록 편집',
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
                        'uri': 'file/manual.md',
                        'name': 'manual.md',
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
    from .kodis_menu import ModuleMenu
    from .kodis_profile import ModuleProfile
    from .kodis_setting import ModuleSetting
    P.set_module_list([ModuleAuth, ModuleSetting, ModuleProfile, ModuleMenu, ModuleDb])
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())
