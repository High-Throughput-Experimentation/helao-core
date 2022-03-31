__all__ = ["config_loader"]

import os
from importlib.util import spec_from_file_location
from importlib.util import module_from_spec
from importlib.machinery import SourceFileLoader

from helaocore.helper.print_message import print_message


def config_loader(confArg, helao_root):
    confPrefix = os.path.basename(confArg).replace(".py", "")
    if confArg.endswith(".py") and os.path.exists(confArg):
        print_message({}, "launcher", f"Loading config from {confArg}", info=True)
        conf_spec = spec_from_file_location("config", confArg)
        conf_mod = module_from_spec(conf_spec)
        conf_spec.loader.exec_module(conf_mod)
        config = conf_mod.config
    elif confArg.endswith(".py") and not os.path.exists(confArg):
        print_message({}, "launcher", f"Config not found at {confArg}", error=True)
        raise FileNotFoundError(
            "Launcher argument ends with .py, expected path not found."
        )
    else:
        print_message(
            {},
            "launcher",
            f"Loading config from helao/config/{confPrefix}.py",
            info=True,
        )
        config = (
            SourceFileLoader(
                "config",
                os.path.join(helao_root, "helao", "config", f"{confPrefix}.py"),
            )
            .load_module()
            .config
        )
    return config
