""" returnmodel.py
Return models returned in API response. Will be deprecated.

"""
__all__ = [
    "ReturnSequence",
    "ReturnSequenceList",
    "ReturnProcess",
    "ReturnProcessList",
    "ReturnFinishedProcess",
    "ReturnRunningProcess",
]


from typing import List, Optional, Union

from pydantic import BaseModel


# TODO: deprecate return* models in favor of original models.
class ReturnSequence(BaseModel):
    """Return class for queried Sequence objects."""

    index: int
    sequence_uuid: Union[str, None]
    sequence_label: str
    sequence_name: str
    sequence_params: dict
    access: str


class ReturnSequenceList(BaseModel):
    """Return class for queried Sequence list."""

    sequences: List[ReturnSequence]


class ReturnProcess(BaseModel):
    """Return class for queried process objects."""

    index: int
    process_uuid: Union[str, None]
    server: str
    process_name: str
    process_params: dict
    preempt: int


class ReturnProcessList(BaseModel):
    """Return class for queried process list."""

    processes: List[ReturnProcess]


class ReturnFinishedProcess(BaseModel):
    """Standard return class for processes that finish with response."""

    technique_name: str
    access: str
    orch_name: str
    sequence_timestamp: str
    sequence_uuid: str
    sequence_label: str
    sequence_name: str
    sequence_params: dict
    result_dict: dict
    process_server: str
    process_timestamp: str
    process_real_time: Optional[str]
    process_name: str
    process_params: dict
    process_uuid: str
    process_enum: str
    process_abbr: str
    process_num: str
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


class ReturnRunningProcess(BaseModel):
    """Standard return class for processes that finish after response."""

    technique_name: str
    access: str
    orch_name: str
    sequence_timestamp: str
    sequence_uuid: str
    sequence_label: str
    sequence_name: str
    sequence_params: dict
    result_dict: dict
    process_server: str
    process_timestamp: str
    process_real_time: Optional[str]
    process_name: str
    process_params: dict
    process_uuid: str
    process_enum: str
    process_abbr: str
    process_num: str
    start_condition: Union[int, dict]
    save_prc: bool
    save_data: bool
    samples_in: Optional[dict]
    samples_out: Optional[dict]
    output_dir: Optional[str]
