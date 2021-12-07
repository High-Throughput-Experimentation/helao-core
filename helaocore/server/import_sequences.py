__all__ = ["import_sequences"]

import os
import sys
from importlib import import_module

from helaocore.helper import print_message


def import_sequences(world_config_dict: dict, sequence_path: str = None, server_name: str = ""):
    """Import sequence functions into environment."""
    sequence_lib = {}
    if sequence_path is None:
        sequence_path = world_config_dict.get(
            "sequence_path", os.path.join("helao", "config", "sequence")
        )
    if not os.path.isdir(sequence_path):
        print_message(
            world_config_dict,
            server_name,
            f"sequence path {sequence_path} was specified but is not a valid directory",
        )
        return sequence_lib
    sys.path.append(sequence_path)
    for prglib in world_config_dict["sequence_libraries"]:
        tempd = import_module(prglib).__dict__
        sequence_lib.update({func: tempd[func] for func in tempd["SEQUENCES"]})
    print_message(
        world_config_dict,
        server_name,
        f"imported {len(world_config_dict['sequence_libraries'])} sequences specified by config.",
    )
    return sequence_lib
