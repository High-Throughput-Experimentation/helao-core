__all__ = ["ActionModel", "ShortActionModel"]

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field


from .sample import SampleUnion
from .fileinfo import FileInfo
from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict


class ShortActionModel(BaseModel, HelaoDict):
    action_uuid: Optional[UUID]
    output_dir: Optional[str]
    action_actual_order: Optional[int] = 0


class ActionModel(ShortActionModel):
    hlo_version: Optional[str] = get_hlo_version()
    technique_name: Optional[str]
    action_server_name: Optional[str]
    orchestrator: Optional[str]
    machine_name: Optional[str]
    access: Optional[str]
    process_uuid: Optional[UUID]
    process_timestamp: Optional[datetime]
    action_timestamp: Optional[datetime]
    action_status: Optional[str]
    action_order: Optional[int] = 0
    action_retry: Optional[int] = 0
    action_split: Optional[int] = 0
    action_name: Optional[str]
    action_abbr: Optional[str]
    action_params: dict = Field(default_factory=dict)
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)
