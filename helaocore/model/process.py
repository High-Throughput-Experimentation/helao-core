__all__ = [
    "ProcessTemplate",
    "ProcessModel",
    "ShortProcessModel",
]

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field

from .sample import SampleUnion
from .action import ShortActionModel
from .file import FileInfo
from .machine import MachineModel
from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict

class ShortProcessModel(BaseModel, HelaoDict):
    process_uuid: Optional[UUID]
    process_name: Optional[str]


class ProcessTemplate(BaseModel, HelaoDict):
    process_name: Optional[str]
    process_params: Optional[dict] = Field(default_factory=dict)


class ProcessModel(ProcessTemplate):
    hlo_version: Optional[str] = get_hlo_version()
    access: Optional[str]
    orchestrator: MachineModel = MachineModel()
    # name of "instrument": sdc, anec, adss etc. defined in world config
    technique_name: Optional[str]
    sequence_uuid: Optional[UUID]
    experiment_uuid: Optional[UUID]
    process_timestamp: Optional[datetime]
    process_group_index: Optional[int]
    process_uuid: Optional[UUID]
    action_list: List[ShortActionModel] = Field(default_factory=list)
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)
