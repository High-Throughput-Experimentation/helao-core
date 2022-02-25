__all__ = ["import_sequences"]

import os
import sys
from importlib import import_module

from ..helper.print_message import print_message


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
    seqlibs =  world_config_dict.get("sequence_libraries", [])
    for seqlib in seqlibs:
        tempd = import_module(seqlib).__dict__
        sequence_lib.update({func: tempd[func] for func in tempd.get("SEQUENCES",[])})
    print_message(
        world_config_dict,
        server_name,
        f"imported {len(seqlibs)} sequences specified by config.",
    )
    return sequence_lib
