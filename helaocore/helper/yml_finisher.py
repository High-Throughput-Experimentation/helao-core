__all__ = ["yml_finisher"]

import asyncio
import aiohttp

from .print_message import print_message


async def yml_finisher(yml_path: str, yml_type: str, base: object = None, retry: int = 3):

    if base is not None:
        print_msg = lambda msg: base.print_message(msg, info=True)
    else:
        print_msg = lambda msg: print_message({}, "yml_finisher", msg, info=True)

    if "DB" in base.world_cfg["servers"].keys():
        dbp_host = base.world_cfg["servers"]["DB"]["host"]
        dbp_port = base.world_cfg["servers"]["DB"]["port"]
    else:
        print_msg("DB server not found in config. Cannot finish yml.")
        return False

    req_params = {"yml_path": yml_path, "yml_type": yml_type}
    req_url = f"http://{dbp_host}:{dbp_port}/finish_yml"
    async with aiohttp.ClientSession() as session:
        for i in range(retry):
            async with session.post(req_url, params=req_params) as resp:
                if resp.status == 200:
                    print_msg(f"Finished experiment: {yml_path}.")
                    return True
                else:
                    print_msg(f"Retry [{i}/{retry}] finish experiment {yml_path}.")
                    await asyncio.sleep(1)
        print_msg(f"Could not finish {yml_path} after {retry} tries.")
        return False
