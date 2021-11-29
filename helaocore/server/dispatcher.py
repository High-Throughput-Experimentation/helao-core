__all__ = ["async_process_dispatcher", "async_private_dispatcher"]

import aiohttp
from helaocore.schema import Process


async def async_process_dispatcher(world_config_dict: dict, A: Process):
    """Request non-blocking process_dq which may run concurrently.

    Send process object to process server for processing.

    Args:
        A: an process type object contain process server name, endpoint, parameters

    Returns:
        Response string from http POST request to process server
    """
    actd = world_config_dict["servers"][A.process_server]
    act_addr = actd["host"]
    act_port = actd["port"]
    url = f"http://{act_addr}:{act_port}/{A.process_server}/{A.process_name}"

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            params={},
            json=A.as_dict(),
        ) as resp:
            response = await resp.json()
            return response


async def async_private_dispatcher(
    world_config_dict: dict,
    server: str,
    private_process: str,
    params_dict: dict,
    json_dict: dict,
):
    """Request non-blocking private process which may run concurrently.

    Returns:
        Response string from http POST request to process server
    """

    actd = world_config_dict["servers"][server]
    act_addr = actd["host"]
    act_port = actd["port"]

    url = f"http://{act_addr}:{act_port}/{private_process}"

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            params=params_dict,
            json=json_dict,
        ) as resp:
            response = await resp.json()
            return response
