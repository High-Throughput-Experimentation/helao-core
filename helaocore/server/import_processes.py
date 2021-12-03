__all__ = ["import_processes"]

import os
import sys
from importlib import import_module

from helaocore.helper import print_message


def import_processes(world_config_dict: dict, process_path: str = None, server_name: str = ""):
    """Import process functions into environment."""
    process_lib = {}
    if process_path is None:
        process_path = world_config_dict.get(
            "process_path", os.path.join("helao", "config", "process")
        )
    if not os.path.isdir(process_path):
        print_message(
            world_config_dict,
            server_name,
            f"process path {process_path} was specified but is not a valid directory",
        )
        return process_lib  # False
    sys.path.append(process_path)
    for prclib in world_config_dict["process_libraries"]:
        tempd = import_module(prclib).__dict__
        process_lib.update({func: tempd[func] for func in tempd["PROCESSES"]})
    print_message(
        world_config_dict,
        server_name,
        f"imported {len(world_config_dict['process_libraries'])} processes specified by config.",
    )
    return process_lib
