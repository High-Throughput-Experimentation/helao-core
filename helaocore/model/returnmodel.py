""" returnmodel.py
Return models returned in API response. Will be deprecated.

"""
__all__ = [
    "ReturnProcess",
    "ReturnProcessList",
    "ReturnAction",
    "ReturnActionList",
]


from typing import List, Optional, Union

from pydantic import BaseModel


# TODO: deprecate return* models in favor of original models.
class ReturnProcess(BaseModel):
    """Return class for queried Process objects."""

    index: int
    process_uuid: Union[str, None]
    process_label: str
    process_name: str
    process_params: dict
    access: str


class ReturnProcessList(BaseModel):
    """Return class for queried Process list."""

    processes: List[ReturnProcess]


class ReturnAction(BaseModel):
    """Return class for queried action objects."""

    index: int
    action_uuid: Union[str, None]
    server: str
    action_name: str
    action_params: dict
    preempt: int


class ReturnActionList(BaseModel):
    """Return class for queried action list."""

    actions: List[ReturnAction]
