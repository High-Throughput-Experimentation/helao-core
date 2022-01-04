__all__ = ["cleanupdict"]

from enum import Enum
from uuid import UUID

def cleanupdict(d):
    clean = {}
    for k, v in d.items():
        if k.startswith("_"):
            continue
        elif isinstance(v, dict):
            nested = cleanupdict(v)
            if len(nested.keys()) > 0:
                clean[k] = nested
        elif v is not None:
            if isinstance(v, Enum):
                clean[k] = v.name
            elif isinstance(v, UUID):
                clean[k] = str(v)
            elif isinstance(v, list):
                if len(v) !=0:
                    clean[k] = _cleanuplist(v)
            elif isinstance(v, str):
                if len(v) !=0:
                    clean[k] = v
            else:
                clean[k] = v
    return clean


def _cleanuplist(input_list):
    clean_list = []
    for list_item in input_list:
        if isinstance(list_item, dict):
            clean_list.append(cleanupdict(list_item))
        elif isinstance(list_item, UUID):
            clean_list.append(str(list_item))
        else:
            clean_list.append(list_item)
    return clean_list
