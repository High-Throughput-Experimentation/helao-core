__all__ = ["ActionModel", "ShortActionModel"]

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field


from .hlostatus import HloStatus
from .sample import SampleUnion
from .file import FileInfo
from .machine import MachineModel
from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict


class ShortActionModel(BaseModel, HelaoDict):
    action_uuid: Optional[UUID]
    output_dir: Optional[str]
    action_actual_order: Optional[int] = 0


class ActionModel(ShortActionModel):
    hlo_version: Optional[str] = get_hlo_version()
    technique_name: Optional[str]
    action_server: MachineModel = MachineModel()
    orchestrator: MachineModel = MachineModel()
    access: Optional[str]
    experiment_uuid: Optional[UUID]
    experiment_timestamp: Optional[datetime]
    action_timestamp: Optional[datetime]
    action_status: List[HloStatus] = Field(default_factory=list)
    action_order: Optional[int] = 0
    action_retry: Optional[int] = 0
    action_split: Optional[int] = 0
    action_name: Optional[str]
    action_abbr: Optional[str]
    action_params: dict = Field(default_factory=dict)
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)
