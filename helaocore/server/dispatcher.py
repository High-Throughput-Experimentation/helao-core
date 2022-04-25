__all__ = ["async_action_dispatcher", "async_private_dispatcher"]

import aiohttp

from ..schema import Action
from ..error import ErrorCodes

from ..helper.print_message import print_message


async def async_action_dispatcher(world_config_dict: dict, A: Action):
    """Request non-blocking action_dq which may run concurrently.

    Send action object to action server for experimenting.

    Args:
        A: an action type object contain action server name,
           endpoint, parameters

    Returns:
        Response string from http POST request to action server
    """
    actd = world_config_dict["servers"][A.action_server.server_name]
    act_addr = actd["host"]
    act_port = actd["port"]
    url = f"http://{act_addr}:{act_port}/{A.action_server.server_name}/{A.action_name}"
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            params={},
            json={"action": A.json_dict()},
        ) as resp:
            error_code = ErrorCodes.none
            if resp.status != 200:
                error_code = ErrorCodes.http
                print_message(
                    actd,
                    A.action_server.server_name,
                    f"{private_action} POST request returned status {resp.status}: '{resp.json()}', error={e}",
                    error=True,
                )
            try:
                response = await resp.json()
            except Exception as e:
                print_message(
                    actd,
                    A.action_server.server_name,
                    f"async_action_dispatcher could not decide response: '{resp}', error={repr(e)}",
                    error=True,
                )
                response = None
            return response, error_code


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
            error_code = ErrorCodes.none
            if resp.status != 200:
                error_code = ErrorCodes.http
                print_message(
                    actd,
                    server,
                    f"{private_action} POST request returned status {resp.status}: '{resp.json()}', error={repr(e)}",
                    error=True,
                )
            try:
                response = await resp.json()
            except Exception as e:
                print_message(
                    actd,
                    server,
                    f"async_private_dispatcher could not decide response: '{resp}', error={repr(e)}",
                    error=True,
                )
                response = None
            return response, error_code
