__all__ = ["helao_dirs"]

import os

from .print_message import print_message

def helao_dirs(world_cfg: dict):
    def check_dir(path):
        if not os.path.isdir(path):
            print_message({},"DIR",
                f"Warning: directory '{path}' does not exist. Creatig it.",
                warning=True,
            )
            os.makedirs(path)
        
    root = None
    save_root = None
    log_root = None
    states_root = None
    db_root = None
    
    if "root" in world_cfg:
        root = world_cfg["root"]
        save_root = os.path.join(root, "RUNS_ACTIVE")
        log_root = os.path.join(root, "LOGS")
        states_root = os.path.join(root, "STATES")
        db_root = os.path.join(root, "DATABASE")
        print_message({},"DIR",
            f"Found root directory in config: {world_cfg['root']}",
        )
        check_dir(root)
        check_dir(save_root)
        check_dir(log_root)
        check_dir(states_root)
        check_dir(db_root)

    return root, save_root, log_root, states_root, db_root
