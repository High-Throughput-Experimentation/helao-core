__all__ = ["yml_finisher"]

import asyncio
import aiohttp
from typing import Union
from helaocore.server.base import Base
from helaocore.server.orch import Orch

async def yml_finisher(yml_path:str, yml_type:str, base:Union[Base, Orch], retry:int=3):

    if "DB" in base.world_cfg["servers"].keys():
        dbp_host = base.world_cfg["servers"]["DB"]["host"]
        dbp_port = base.world_cfg["servers"]["DB"]["port"]
    else:
        base.print_message("DB server not found in config. Cannot finish yml.")
        return False
    
    req_params = {"yml_path": yml_path, "yml_type": "experiment"}
    req_url = f"http://{dbp_host}:{dbp_port}/finish_yml"
    async with aiohttp.ClientSession() as session:
        for i in range(retry):
            async with session.post(req_url, params=req_params) as resp:
                if resp.status == 200:
                    base.print_message(f"Finished experiment: {yml_path}.")
                    return True
                else:
                    base.print_message(f"Retry [{i}/{retry}] finish experiment {yml_path}.")
                    await asyncio.sleep(1)
        base.print_message(f"Could not finish {yml_path} after {retry} tries.")
        return False