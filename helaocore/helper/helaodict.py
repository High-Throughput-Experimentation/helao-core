__all__ = ["HelaoDict"]

from datetime import datetime
from uuid import UUID
import types
from pydantic import BaseModel
from typing import Any
from enum import Enum

simple = (int, str, float, bool, type(None))
complextostr = (UUID, datetime)
iterables = (list, set, tuple)


class HelaoDict():
    def _serialize_dict(self, dict_in: dict):
        clean = dict()
        for k, v in dict_in.items():
            if not isinstance(v,types.FunctionType) \
            and not (isinstance(v,str) and k.startswith("__")):
                clean.update({k: self._serialize_item(val = v)})
        return clean

    
    def _serialize_item(self, val: Any):
        if isinstance(val, simple):
            return val
        elif isinstance(val, complextostr):
            return str(val)
        elif isinstance(val, list):
            return [self._serialize_item(val = item) for item in val]
        elif isinstance(val, tuple):
            return (self._serialize_item(val = item) for item in val)
        elif isinstance(val, set):
            return {self._serialize_item(val = item) for item in val}
        elif isinstance(val, dict):
            return self._serialize_dict(dict_in = val)
        elif isinstance(val, Enum):
            return val.name
        elif isinstance(val, BaseModel):
            return self._serialize_dict(dict_in = val.dict())


    def as_dict(self):
        d = vars(self)
        attr_only = self._serialize_dict(dict_in = d)#dict()
        return attr_only

    

    def fastdict(self):
        json_list_keys = ["process_list"]
        # json_keys = []
        d = vars(self)
        params_dict = {
            k: int(v) if isinstance(v, bool) else v
            for k, v in d.items()
            if not isinstance(v,types.FunctionType)
            and not k.startswith("__")
            and k not in json_list_keys
            and (v is not None)
            # and not isinstance(v, UUID)
            and not isinstance(v,dict)
            and not isinstance(v,list)
            and (v != {})
        }
        
        
        json_dict = {
            k: v
            for k, v in d.items()
            if not isinstance(v,types.FunctionType)
            and not k.startswith("__")
            and k not in json_list_keys
            and (v is not None)
            and not isinstance(v, UUID)
            and isinstance(v,dict)
        }

        # # add UUID keys to json_dict
        # for key,val in d.items():
        #     if isinstance(val, UUID):
        #         json_dict.update({key:str(val)})


        for key in json_list_keys:
            if key in d:
                json_dict.update({key:[val.as_dict() for val in d[key]]})
        #         json_dict.update({key:[]})


        return params_dict, json_dict
