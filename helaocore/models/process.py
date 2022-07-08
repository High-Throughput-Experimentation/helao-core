__all__ = [
    "ProcessTemplate",
    "ProcessModel",
    "ShortProcessModel",
]

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field

from helaocore.models.sample import SampleUnion
from helaocore.models.action import ShortActionModel
from helaocore.models.file import FileInfo
from helaocore.models.machine import MachineModel
from helaocore.version import get_hlo_version
from helaocore.helaodict import HelaoDict


class ShortProcessModel(BaseModel, HelaoDict):
    process_uuid: Optional[UUID]
    process_name: Optional[str]


class ProcessTemplate(BaseModel, HelaoDict):
    process_name: Optional[str]
    process_params: Optional[dict] = Field(default_factory=dict)


class ProcessModel(ProcessTemplate):
    hlo_version: Optional[str] = get_hlo_version()
    orchestrator: MachineModel = MachineModel()
    access: Optional[str] = 'hte'
    dummy: bool = False
    simulation: bool = False
    # name of "instrument": eche, anec, adss etc. defined in world config
    sequence_uuid: Optional[UUID]
    experiment_uuid: Optional[UUID]
    run_type: Optional[str]
    technique_name: Optional[str]
    process_timestamp: Optional[datetime]
    process_group_index: Optional[int]
    process_uuid: Optional[UUID]
    action_list: List[ShortActionModel] = Field(default_factory=list)
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)
