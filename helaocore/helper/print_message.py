__all__ = ["print_message"]

import os
from time import strftime

from colorama import Fore, Back, Style

def print_message(server_cfg, server_name, *args, **kwargs):
    precolor = ""
    msg_type = ""
    if "error" in kwargs:
        precolor = f"{Style.BRIGHT}{Fore.WHITE}{Back.RED}"
        msg_type = "error_"
    elif "warning" in kwargs:
        precolor = f"{Fore.BLACK}{Back.YELLOW}"
        msg_type = "warning_"
    elif "info" in kwargs:
        precolor = f"{Fore.BLACK}{Back.GREEN}"
        msg_type = "info_"
    else:
        precolor = f"{Style.RESET_ALL}"


    srv_type = server_cfg.get("group", "")
    style = ""
    if srv_type == "orchestrator":
        style = f"{Style.BRIGHT}{Fore.GREEN}"
    elif srv_type == "action":
        style = f"{Style.BRIGHT}{Fore.YELLOW}"
    elif srv_type == "operator":
        style = f"{Style.BRIGHT}{Fore.CYAN}"
    elif srv_type == "visualizer":
        style = f"{Style.BRIGHT}{Fore.CYAN}"
    else:
        style = ""

    for arg in args:
        print(
            f"{precolor}[{strftime('%H:%M:%S')}_{server_name}]:{Style.RESET_ALL} {style}{arg}{Style.RESET_ALL}"
        )

    output_path = kwargs.get("log_dir", None)
    if output_path is not None:
        output_path = os.path.join(output_path,server_name)
        output_file = os.path.join(output_path, f"{server_name}_log_{strftime('%Y%m%d')}.txt")
        if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)
        with open(output_file, "a+") as f:
            for arg in args:
                f.write(f"[{msg_type}{strftime('%H:%M:%S')}_{server_name}]: {arg}\n")
