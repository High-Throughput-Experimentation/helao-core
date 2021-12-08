""" file.y
File models for writing/organizing .prc and .prg metadata contents.

"""
__all__ = ["ActFile", "PrcFile", "SeqFile"]

from typing import Optional, Union

from helaocore.server import version
from pydantic import BaseModel


class ActFile(BaseModel):
    hlo_version: str = version.hlo_version
    technique_name: str
    server_name: str
    orchestrator: str
    machine_name: str
    access: str
    output_dir: str
    process_uuid: str
    process_timestamp: str
    action_uuid: str
    action_timestamp: str
    action_enum: Optional[float] = 0.0
    action_name: str
    action_abbr: Optional[str] = None
    action_params: Union[dict, None] = None
    samples_in: Optional[Union[dict, None]] = None
    samples_out: Optional[Union[dict, None]] = None
    files: Optional[Union[dict, None]] = None


class PrcFile(BaseModel):
    hlo_version: str = version.hlo_version
    orchestrator: str
    machine_name: str
    access: str
    process_uuid: str
    process_timestamp: str
    process_label: str
    technique_name: str
    process_name: str
    process_params: Union[dict, None] = None


class SeqFile(BaseModel):
    hlo_version: str = version.hlo_version
    sequence_name: str
    sequence_label: str
    sequence_uuid: str
    sequence_timestamp: str
    # process_list: Optional[]
