__all__ = ["HelaoDict"]

from datetime import datetime
from uuid import UUID
import types
from pydantic import BaseModel
from typing import Any
from enum import Enum
from pathlib import Path
from copy import deepcopy


class HelaoDict:
    """implements dict and serialization methods for helao"""

    def _serialize_dict(self, dict_in: dict):
        clean = dict()
        for k, v in dict_in.items():
            if not isinstance(v, types.FunctionType) and not (isinstance(v, str) and k.startswith("__")):
                # keys can also be UUID, datetime etc
                clean.update({self._serialize_item(val=k): self._serialize_item(val=v)})
        return clean

    def _serialize_item(self, val: Any):

        if isinstance(val, Enum):
            # need to be first to catch also str enums
            if isinstance(val, str):
                return val.name
            else:
                return val.value
        elif isinstance(val, (int, str, float, bool, type(None))):
            return val
        elif isinstance(val, (Path)):
            return str(val.as_posix())
        elif isinstance(val, (UUID, datetime)):
            return str(val)
        elif isinstance(val, list):
            return [self._serialize_item(val=item) for item in val]
        elif isinstance(val, tuple):
            return (self._serialize_item(val=item) for item in val)
        elif isinstance(val, set):
            return {self._serialize_item(val=item) for item in val}
        elif isinstance(val, dict):
            return self._serialize_dict(dict_in=val)
        elif isinstance(val, BaseModel):
            return self._serialize_dict(dict_in=val.dict())
        elif hasattr(val, "as_dict"):
            return val.as_dict()
        else:
            tmp_str = f"Helao as_dict cannot serialize {val}"
            raise ValueError(tmp_str)

    def json_dict(self):
        return self.as_dict()

    def as_dict(self):
        d = deepcopy(vars(self))
        attr_only = self._serialize_dict(dict_in=d)
        return attr_only

    def fastdict(self):
        """creates dictionaries for FastAPI post request"""
        d = deepcopy(vars(self))

        params_dict = dict()
        json_dict = dict()
        for k, v in d.items():
            if not isinstance(v, types.FunctionType) and not k.startswith("__") and (v is not None):
                if isinstance(v, (dict, list, set, tuple)):
                    json_dict.update({k: self._fastdict_serialize_item(v)})
                elif isinstance(v, BaseModel):
                    json_dict.update({k: self._fastdict_serialize_basemodel(v)})
                elif hasattr(v, "json_dict"):
                    json_dict.update({k: v.json_dict()})
                # elif isinstance(v, UUID):
                #     params_dict.update({k:v})

                else:
                    params_dict.update({k: v})

        return params_dict, json_dict

    def _fastdict_serialize_item(self, val: Any):
        if isinstance(val, dict):
            return {k: self._fastdict_serialize_item(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [self._fastdict_serialize_item(item) for item in val]
        elif isinstance(val, tuple):
            return (self._fastdict_serialize_item(item) for item in val)
        elif isinstance(val, set):
            return {self._fastdict_serialize_item(item) for item in val}
        elif isinstance(val, BaseModel):
            return self._fastdict_serialize_basemodel(val)
        elif hasattr(val, "json_dict"):
            return val.json_dict()
        else:
            return val

    def _fastdict_serialize_basemodel(self, model: BaseModel):
        if hasattr(model, "as_dict"):
            return model.as_dict()
        elif hasattr(model, "dict"):
            return self._serialize_dict(model.dict())
        else:
            tmp_str = f"Helao fastdict cannot serialize {model}"
            raise ValueError(tmp_str)

    def clean_dict(self):
        return self._cleanupdict(self.as_dict())

    def _cleanupdict(self, d):
        clean = {}
        for k, v in d.items():
            if k.startswith("_"):
                continue
            elif isinstance(v, dict):
                nested = self._cleanupdict(v)
                if len(nested.keys()) > 0:
                    clean[k] = nested
            elif v is not None:
                if isinstance(v, Enum):
                    clean[k] = v.name
                elif isinstance(v, UUID):
                    clean[k] = str(v)
                elif isinstance(v, list):
                    if len(v) != 0:
                        clean[k] = self._cleanuplist(v)
                elif isinstance(v, str):
                    if len(v) != 0:
                        clean[k] = v
                else:
                    clean[k] = v
        return clean

    def _cleanuplist(self, input_list):
        clean_list = []
        for list_item in input_list:
            if isinstance(list_item, dict):
                clean_list.append(self._cleanupdict(list_item))
            elif isinstance(list_item, UUID):
                clean_list.append(str(list_item))
            else:
                clean_list.append(list_item)
        return clean_list
