"""


Leon MediaCrawler - A social media crawler project

Support platforms: Xiaohongshu, Weibo, Zhihu, Bilibili, Douyin, BaiduTieBa etc.
"""

__version__ = "0.1.4"
__author__ = "Bradleon"
__email__ = "bradleon91@gmail.com"

import sys
import os

# Add current package directory to sys.path to support absolute imports
# This allows 'import config' to work instead of requiring 'from leon_mediacrawler import config'
_package_dir = os.path.dirname(__file__)
if _package_dir not in sys.path:
    sys.path.insert(0, _package_dir)

# Import main modules for easier access
try:
    # Import all modules
    from . import config
    from . import base
    from . import cache
    from . import constant
    from . import model
    from . import utils
    from . import db
    from . import async_db
    from . import var
    from . import tools
    from . import proxy
    from . import store
    from . import cmd_arg
    from . import media_platform
    
    # Register modules in sys.modules for absolute imports to work
    sys.modules['config'] = config
    sys.modules['base'] = base
    sys.modules['cache'] = cache
    sys.modules['constant'] = constant
    sys.modules['model'] = model
    sys.modules['utils'] = utils
    sys.modules['db'] = db
    sys.modules['async_db'] = async_db
    sys.modules['var'] = var
    sys.modules['tools'] = tools
    sys.modules['proxy'] = proxy
    sys.modules['store'] = store
    sys.modules['cmd_arg'] = cmd_arg
    sys.modules['media_platform'] = media_platform
    
except ImportError:
    # Fallback for development mode
    import config
    import base
    import cache
    import cmd_arg
    import constant
    import media_platform
    import model
    import proxy
    import tools
    import store
    import utils
    import db
    import async_db
    import var

__all__ = [
    "config",
    "base", 
    "cache",
    "cmd_arg",
    "constant",
    "media_platform",
    "model",
    "proxy",
    "tools",
    "store",
    "utils",
    "db",
    "async_db", 
    "var",
    "__version__",
    "__author__",
    "__email__"
] 