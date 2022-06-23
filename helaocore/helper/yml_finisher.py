__all__ = ["yml_finisher"]

import os
import asyncio
import traceback
from typing import Union
from glob import glob
from pathlib import Path

import aiohttp
import aioshutil
import aiofiles
from ruamel.yaml import YAML

from .print_message import print_message
from ..schema import Sequence, Experiment, Action


async def yml_finisher(yml_path: str, base: object = None, retry: int = 3):
    yaml = YAML(typ="safe")
    ymld = yaml.load(Path(yml_path))
    yml_type = ymld["file_type"]

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

    priority = {'action': 0, 'experiment': 1, 'sequence': 2}

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


async def move_dir(hobj: Union[Sequence, Experiment, Action], base: object = None, retry_delay: int = 5):
    """Move directory from RUNS_ACTIVE to RUNS_FINISHED. """
 
    if base is not None:
        print_msg = lambda msg: base.print_message(msg, info=True)
    else:
        print_msg = lambda msg: print_message({}, "yml_finisher", msg, info=True)
 
    obj_type = hobj.__class__.__name__.lower()
    save_dir = base.helaodirs.save_root.__str__()
    if obj_type == 'action':
        yml_dir = os.path.join(save_dir, hobj.get_action_dir())
    elif obj_type == 'experiment':
        yml_dir = os.path.join(save_dir, hobj.get_experiment_dir())
    elif obj_type == 'sequence':
        yml_dir = os.path.join(save_dir, hobj.get_sequence_dir())
    else:
        yml_dir = None
        print_msg(f'Invalid object {obj_type} was provided. Can only move Action, Experiment, or Sequence.')
        return {}
 
    new_dir = os.path.join(yml_dir.replace("RUNS_ACTIVE", "RUNS_FINISHED"))
    await aiofiles.os.makedirs(new_dir, exist_ok=True)

    copy_success = False
    copy_retries = 0
    file_list = glob(os.path.join(yml_dir, '*'))
 
    while not copy_success and copy_retries <= 60:
        copy_results = await asyncio.gather(*[aioshutil.copy(p, p.replace("RUNS_ACTIVE", "RUNS_FINISHED")) for p in file_list], return_exceptions=True)
        copied_idx = [i for i,v in enumerate(copy_results)]
        exists_idx = [i for i in copied_idx if os.path.exists(copy_results[i])]
        if len(exists_idx) == len(file_list):
            copy_success = True
        else:
            file_list = [v for i,v in enumerate(file_list) if i not in exists_idx]
            print_msg(f"Could not move {len(file_list)} files to FINISHED, retrying after {retry_delay} seconds")
            copy_retries += 1
            await asyncio.sleep(retry_delay)
    if copy_success:
        rm_success = False
        rm_retries = 0
        while not rm_success and rm_retries <= 30:
            rm_result = await asyncio.gather(aioshutil.rmtree(yml_dir))
            rm_result = rm_result[0]
            if not isinstance(rm_result, Exception):
                rm_success = True
            else:
                print_msg(f"Could not remove directory from ACTIVE, retrying after {retry_delay} seconds")
                rm_retries += 1
                await asyncio.sleep(retry_delay)
        if rm_success:
            timestamp = getattr(hobj, f"{obj_type}_timestamp").strftime('%Y%m%d.%H%M%S%f')
            yml_path = os.path.join(new_dir, f"{timestamp}.yml")
            await yml_finisher(yml_path, base=base)
 