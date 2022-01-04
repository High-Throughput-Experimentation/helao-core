__all__ = ["async_action_dispatcher", "async_private_dispatcher"]

import aiohttp

from ..schema import Action

async def async_action_dispatcher(world_config_dict: dict, A: Action):
    """Request non-blocking action_dq which may run concurrently.

    Send action object to action server for processing.

    Args:
        A: an action type object contain action server name, endpoint, parameters

    Returns:
        Response string from http POST request to action server
    """
    actd = world_config_dict["servers"][A.action_server_name]
    act_addr = actd["host"]
    act_port = actd["port"]
    url = f"http://{act_addr}:{act_port}/{A.action_server_name}/{A.action_name}"
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
    private_action: str,
    params_dict: dict,
    json_dict: dict,
):
    """Request non-blocking private action which may run concurrently.

    Returns:
        Response string from http POST request to action server
    """

    actd = world_config_dict["servers"][server]
    act_addr = actd["host"]
    act_port = actd["port"]

    url = f"http://{act_addr}:{act_port}/{private_action}"

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            params=params_dict,
            json=json_dict,
        ) as resp:
            response = await resp.json()
            return response
