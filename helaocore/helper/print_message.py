__all__ = ["print_message"]

from time import strftime

from colorama import Fore, Back, Style


def print_message(server_cfg, server_name, *args, **kwargs):
    precolor = ""
    if "error" in kwargs:
        precolor = f"{Style.BRIGHT}{Fore.WHITE}{Back.RED}"
    elif "warning" in kwargs:
        precolor = f"{Fore.BLACK}{Back.YELLOW}"
    elif "info" in kwargs:
        precolor = f"{Fore.BLACK}{Back.GREEN}"
    else:
        precolor = f"{Style.RESET_ALL}"

    srv_type = server_cfg.get("group", "")
    style = ""
    if srv_type == "orchestrator":
        style = f"{Style.BRIGHT}{Fore.GREEN}"
    elif srv_type == "process":
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
