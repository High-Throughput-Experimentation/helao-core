__all__ = ["ActionModel", "ShortActionModel"]

from datetime import datetime
from typing import List, Optional, Union
from uuid import UUID
from pathlib import Path
from pydantic import BaseModel, Field

from helaocore.models.hlostatus import HloStatus
from helaocore.models.process_contrib import ProcessContrib
from helaocore.models.sample import SampleUnion
from helaocore.models.file import FileInfo
from helaocore.models.machine import MachineModel
from helaocore.version import get_hlo_version
from helaocore.helaodict import HelaoDict
from helaocore.error import ErrorCodes


class ShortActionModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = get_hlo_version()
    action_uuid: Optional[UUID]
    action_output_dir: Optional[Path]
    action_actual_order: Optional[int] = 0
    orch_submit_order: Optional[int] = 0
    action_server: MachineModel = MachineModel()


class ActionModel(ShortActionModel):
    orchestrator: MachineModel = MachineModel()
    access: Optional[str] = 'hte'
    dummy: bool = False
    simulation: bool = False
    run_type: Optional[str]
    experiment_uuid: Optional[UUID]
    experiment_timestamp: Optional[datetime]
    action_timestamp: Optional[datetime]
    action_status: List[HloStatus] = Field(default_factory=list)
    action_order: Optional[int] = 0
    action_retry: Optional[int] = 0
    action_split: Optional[int] = 0
    action_name: Optional[str]
    action_sub_name: Optional[str]
    action_abbr: Optional[str]
    action_params: dict = Field(default_factory=dict)
    action_etc: Optional[float]  # expected time to completion
    parent_action_uuid: Optional[UUID]
    child_action_uuid: Optional[UUID]
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)
    manual_action: bool = False
    technique_name: Optional[Union[str, list]] = None
    process_finish: bool = False
    process_contrib: List[ProcessContrib] = Field(default_factory=list)
    error_code: Optional[ErrorCodes] = ErrorCodes.none

    # process_group_index: Optional[int] = 0 # unnecessary if we rely on process_finish as group terminator
