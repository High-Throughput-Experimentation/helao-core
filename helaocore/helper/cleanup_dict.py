__all__ = ["cleanupdict"]


def cleanupdict(d):
    clean = {}
    for k, v in d.items():
        if isinstance(v, dict):
            nested = cleanupdict(v)
            if len(nested.keys()) > 0:
                clean[k] = nested
        elif v is not None:
            if isinstance(v, list):
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
        else:
            clean_list.append(list_item)
    return clean_list
