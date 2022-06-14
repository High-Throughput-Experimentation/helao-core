__all__ = ["yml_finisher"]

import os
import asyncio
import traceback
from typing import Union
from glob import glob

import aiohttp
import aioshutil

from .print_message import print_message
from ..schema import Sequence, Experiment, Action


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

    priority = {'action': 0, 'experiment': 0, 'sequence': 2}

    req_params = {"yml_path": yml_path, "priority": priority[yml_type]}
    req_url = f"http://{dbp_host}:{dbp_port}/finish_yml"
    async with aiohttp.ClientSession() as session:
        for i in range(retry):
            async with session.post(req_url, params=req_params) as resp:
                if resp.status == 200:
                    print_msg(f"Finished {yml_type}: {yml_path}.")
                    return True
                else:
                    print_msg(f"Retry [{i}/{retry}] finish {yml_type} {yml_path}.")
                    await asyncio.sleep(1)
        print_msg(f"Could not finish {yml_path} after {retry} tries.")
        return False


async def move_dir(hobj: Union[Action, Experiment, Sequence], base: object = None, retry_delay: int = 2):
    """Move directory from RUNS_ACTIVE to RUNS_FINISHED. """
    
    if base is not None:
        print_msg = lambda msg: base.print_message(msg, info=True)
    else:
        print_msg = lambda msg: print_message({}, "yml_finisher", msg, info=True)
        
    obj_type = hobj.__class__.__name__
    save_dir = base.helaodirs.save_root.__str__()
    if obj_type == 'action':
        yml_dir = os.path.join(save_dir, hobj.get_action_dir())
    elif obj_type == 'experiment':
        yml_dir = os.path.join(save_dir, hobj.get_experiment_dir())
    elif obj_type == 'sequence':
        yml_dir = os.path.join(save_dir, hobj.get_experiment_dir())
    else:
        yml_dir = None
        print_msg('Invalid object was provided. Can only move Action, Experiment, or Sequence.')
        return {}
    
    new_dir = os.path.join(yml_dir.replace("RUNS_ACTIVE", "RUNS_FINISHED"))
    os.makedirs(new_dir, exist_ok=True)

    copy_success = False
    copy_retries = 0
    while not copy_success and copy_retries <= 30:
        try:
            for p in glob(os.path.join(yml_dir, '*')):
                await aioshutil.copy(p, p.replace("RUNS_ACTIVE", "RUNS_FINISHED"))
            copy_success = True
        except Exception as e:
            tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            print_msg(f"Could not move all files to FINISHED: {repr(e), tb,}, retrying after {retry_delay} seconds", warning=True)
            copy_retries += 1
            await asyncio.sleep(retry_delay)
    if copy_success:
        rm_success = False
        rm_retries = 0
        while not rm_success and rm_retries <= 30:
            try:
                await aioshutil.rmtree(yml_dir.__str__())
                rm_success = True
            except PermissionError:
                print_msg(f"Could not remove directory from ACTIVE, retrying after {retry_delay} seconds", warning=True)
                rm_retries += 1
                await asyncio.sleep(retry_delay)
        if rm_success:
            timestamp = getattr(hobj, f"{obj_type}_timestamp").strftime('%Y%m%d.%H%M%S%f')
            yml_path = os.path.join(new_dir, f"{timestamp}.yml")
            await yml_finisher(yml_path, obj_type, base=base)
    