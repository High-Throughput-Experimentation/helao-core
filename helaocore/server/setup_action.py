__all__ = ["setup_action"]

import json
from socket import gethostname

from fastapi import Request


from ..schema import Action
from ..model.sample import object_to_sample
from ..model.action import ActionModel
from ..model.machine import MachineModel


async def setup_action(request: Request) -> Action:
    servKey, _, action_name = request.url.path.strip("/").partition("/")
    body_bytes = await request.body()
    if body_bytes == b"":
        body_params = {}
    else:
        body_params = await request.json()

    action_dict = dict()
    # action_dict.update(request.query_params)
    if len(request.query_params) == 0:  # cannot check against {}
        # empty: orch
        action_dict.update(body_params)
    else:
        # not empty: swagger
        if "action_params" not in action_dict:
            action_dict.update({"action_params": {}})
        action_dict["action_params"].update(body_params)
        # action_dict["action_params"].update(request.query_params)
        for k, v in request.query_params.items():
            try:
                val = json.loads(v)
            except ValueError:
                val = v
            action_dict["action_params"][k] = val

    action_dict["action_server"] = MachineModel(
                                                server_name = servKey,
                                                machine_name = gethostname()
                                               ).dict()
    action_dict["action_name"] = action_name
    A = Action(
               inputdict=action_dict,
               act=ActionModel(**action_dict)
              )

    if A.action_params is not None:
        if "fast_samples_in" in A.action_params:
            tmp_fast_samples_in = A.action_params.get("fast_samples_in", [])
            del A.action_params["fast_samples_in"]
    
            for sample in tmp_fast_samples_in:
                A.samples_in.append(object_to_sample(sample))

    if A.action_abbr is None:
        A.action_abbr = A.action_name

    # setting some default values if action was not submitted via orch
    if A.technique_name is None:
        A.technique_name = "MANUAL"
        A.orchestrator = MachineModel(
                                      server_name = "MANUAL",
                                      machine_name = gethostname()
                                     )

    return A
