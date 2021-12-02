__all__ = ["setup_action"]

import json
from socket import gethostname

import helaocore.model.sample as hcms
from fastapi import Request
from helaocore.schema import Action


async def setup_action(request: Request):
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

    action_dict["action_server"] = servKey
    action_dict["action_name"] = action_name
    A = Action(action_dict)

    if "fast_samples_in" in A.action_params:
        tmp_fast_samples_in = A.action_params.get("fast_samples_in", [])
        del A.action_params["fast_samples_in"]
        if isinstance(tmp_fast_samples_in,dict):
            A.samples_in = hcms.SampleList(**tmp_fast_samples_in)
        elif isinstance(tmp_fast_samples_in, list):
            A.samples_in = hcms.SampleList(samples=tmp_fast_samples_in)

    if A.action_abbr is None:
        A.action_abbr = A.action_name

    # setting some default values if action was not submitted via orch
    if A.machine_name is None:
        A.machine_name = gethostname()
    if A.technique_name is None:
        A.technique_name = "MANUAL"
        A.orch_name = "MANUAL"
        A.sequence_label = "MANUAL"
    # sample_list cannot be serialized so needs to be updated here
    if A.samples_in == []:
        A.samples_in = hcms.SampleList()
    if A.samples_out == []:
        A.samples_out = hcms.SampleList()

    return A
