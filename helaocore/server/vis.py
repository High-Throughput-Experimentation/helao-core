__all__ = ["Vis"]

import os
import sys
from socket import gethostname

import colorama
from helaocore.helper import print_message
from helaocore.helper import helao_dirs

from .api import HelaoBokehAPI

# ANSI color codes converted to the Windows versions
colorama.init(strip=not sys.stdout.isatty())  # strip colors if stdout is redirected
# colorama.init()


class Vis(object):
    """Base class for all HELAO bokeh servers."""

    def __init__(self, bokehapp: HelaoBokehAPI):
        self.server_name = bokehapp.helao_srv
        self.server_cfg = bokehapp.world_cfg["servers"][self.server_name]
        self.world_cfg = bokehapp.world_cfg
        self.hostname = gethostname()
        self.doc = bokehapp.doc
        self.root, self.save_root, self.log_root, self.states_root = \
            helao_dirs(self.world_cfg)
        
        if self.root is None:
            raise ValueError(
                "Warning: root directory was not defined. Logs, PRCs, PRGs, and data will not be written.",
                error=True,
            )
        
        # self.technique_name = None
        # self.aloop = asyncio.get_running_loop()

    def print_message(self, *args, **kwargs):
        print_message(self.server_cfg, self.server_name, *args, **kwargs)
