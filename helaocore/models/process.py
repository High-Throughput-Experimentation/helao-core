__all__ = [
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
from helaocore.models.run_use import RunUse


class ShortProcessModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = get_hlo_version()
    process_uuid: Optional[UUID]


class ProcessModel(ShortProcessModel):
    sequence_uuid: Optional[UUID]
    experiment_uuid: Optional[UUID]
    orchestrator: MachineModel = MachineModel()
    access: Optional[str] = "hte"
    dummy: bool = False
    simulation: bool = False
    technique_name: Optional[str]
    run_type: Optional[str]
    run_use: Optional[RunUse] = "data"
    process_timestamp: Optional[datetime]
    process_params: Optional[dict] = {}
    process_group_index: Optional[int]
    action_list: List[ShortActionModel] = Field(default=[])
    samples_in: List[SampleUnion] = Field(default=[])
    samples_out: List[SampleUnion] = Field(default=[])
    files: List[FileInfo] = Field(default=[])
