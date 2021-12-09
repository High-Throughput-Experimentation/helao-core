""" file.y
File models for writing/organizing .prc and .prg metadata contents.

"""
__all__ = ["ActFile", "PrcFile", "SeqFile"]

from typing import Optional, Union, List

from helaocore.server import version
from pydantic import BaseModel


class ActFile(BaseModel):
    hlo_version: Union[str, None] = version.hlo_version
    technique_name: Union[str, None]
    server_name: Union[str, None]
    orchestrator: Union[str, None]
    machine_name: Union[str, None]
    access: Union[str, None]
    output_dir: Union[str, None]
    process_uuid: Union[str, None]
    process_timestamp: Union[str, None]
    action_uuid: Union[str, None]
    action_timestamp: Union[str, None]
    action_order: Optional[int] = 0
    action_retry: Optional[int] = 0
    action_actual_order: Optional[int] = 0
    action_name: Union[str, None]
    action_abbr: Optional[str] = None
    action_params: Union[dict, None] = None
    samples_in: Optional[Union[List[dict], None]] = None
    samples_out: Optional[Union[List[dict], None]] = None
    files: Optional[Union[dict, None]] = None


class PrcFile(BaseModel):
    hlo_version: Union[str, None] = version.hlo_version
    orchestrator: Union[str, None]
    machine_name: Union[str, None]
    access: Union[str, None]
    process_uuid: Union[str, None]
    process_timestamp: Union[str, None]
    process_label: Union[str, None]
    technique_name: Union[str, None]
    process_name: Union[str, None]
    process_params: Union[dict, None] = None


class SeqFile(BaseModel):
    hlo_version: Union[str, None] = version.hlo_version
    sequence_name: Union[str, None]
    sequence_label: Union[str, None]
    sequence_uuid: Union[str, None]
    sequence_timestamp: Union[str, None]
    # process_list: Optional[]
