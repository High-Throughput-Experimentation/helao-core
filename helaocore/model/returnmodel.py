""" returnmodel.py
Return models returned in API response. Will be deprecated.

"""
__all__ = [
    "ReturnProcess",
    "ReturnProcessList",
    "ReturnAction",
    "ReturnActionList",
    "ReturnFinishedAction",
    "ReturnRunningAction",
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


class ReturnFinishedAction(BaseModel):
    """Standard return class for actions that finish with response."""

    technique_name: str
    access: str
    orch_name: str
    process_timestamp: str
    process_uuid: str
    process_label: str
    process_name: str
    process_params: dict
    result_dict: dict
    action_server: str
    action_timestamp: str
    action_real_time: Optional[str]
    action_name: str
    action_params: dict
    action_uuid: str
    action_ordering: str
    action_abbr: str
    action_num: str
    start_condition: Union[int, dict]
    save_prc: bool
    save_data: bool
    samples_in: Optional[dict]
    samples_out: Optional[dict]
    output_dir: Optional[str]
    file_dict: Optional[dict]
    column_names: Optional[list]
    header: Optional[str]
    data: Optional[list]


class ReturnRunningAction(BaseModel):
    """Standard return class for actions that finish after response."""

    technique_name: str
    access: str
    orch_name: str
    process_timestamp: str
    process_uuid: str
    process_label: str
    process_name: str
    process_params: dict
    result_dict: dict
    action_server: str
    action_timestamp: str
    action_real_time: Optional[str]
    action_name: str
    action_params: dict
    action_uuid: str
    action_ordering: str
    action_abbr: str
    action_num: str
    start_condition: Union[int, dict]
    save_prc: bool
    save_data: bool
    samples_in: Optional[dict]
    samples_out: Optional[dict]
    output_dir: Optional[str]
