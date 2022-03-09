__all__ = ["to_json"]

import json

def to_json(v):
    try:
        val = json.loads(v.replace("'",'"'))
    except ValueError:
        val = v
    return val