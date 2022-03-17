__all__ = ["import_experiments"]

import os
import sys
from importlib import import_module

from ..helper.print_message import print_message


def import_experiments(world_config_dict: dict, experiment_path: str = None, server_name: str = ""):
    """Import experiment functions into environment."""
    experiment_lib = {}
    if experiment_path is None:
        experiment_path = world_config_dict.get(
            "experiment_path", os.path.join("helao", "config", "experiment")
        )
    if not os.path.isdir(experiment_path):
        print_message(
            world_config_dict,
            server_name,
            f"experiment path {experiment_path} was specified but is not a valid directory",
        )
        return experiment_lib  # False
    sys.path.append(experiment_path)
    explibs = world_config_dict.get("experiment_libraries", [])
    for explib in explibs:
        print_message(
            world_config_dict,
            server_name,
            f"importing exeriments from {explib}",
        )
        tempd = import_module(explib).__dict__
        for func in tempd.get("EXPERIMENTS",[]):
            if func in tempd:
                experiment_lib.update({func: tempd[func]})
                print_message(
                    world_config_dict,
                    server_name,
                    f"added exp '{func}' to experiment library",
                )
            else:
                print_message(
                    world_config_dict,
                    server_name,
                    f"!!! Could not find experiment function '{func}' in '{explib}'",
                    error = True
                )

    print_message(
        world_config_dict,
        server_name,
        f"imported {len(explibs)} experiments specified by config.",
    )
    return experiment_lib
