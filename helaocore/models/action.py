__all__ = ["ActionModel", "ShortActionModel"]

from datetime import datetime
from typing import List, Optional, Union
from uuid import UUID
from pathlib import Path
from pydantic import BaseModel, Field

from helaocore.models.hlostatus import HloStatus
from helaocore.models.process_contrib import ProcessContrib
from helaocore.models.run_use import RunUse
from helaocore.models.sample import SampleUnion
from helaocore.models.file import FileInfo
from helaocore.models.machine import MachineModel
from helaocore.version import get_hlo_version
from helaocore.helaodict import HelaoDict
from helaocore.error import ErrorCodes


class ShortActionModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = Field(default_factory=get_hlo_version)
    action_uuid: Optional[UUID] = None
    action_output_dir: Optional[Path] = None
    action_actual_order: Optional[int] = 0
    orch_submit_order: Optional[int] = 0
    action_server: MachineModel = MachineModel()
    orch_key: Optional[str] = None
    orch_host: Optional[str] = None
    orch_port: Optional[int] = None


class ActionModel(ShortActionModel):
    orchestrator: MachineModel = MachineModel()
    access: Optional[str] = "hte"
    dummy: bool = False
    simulation: bool = False
    run_type: Optional[str] = None
    run_use: Optional[RunUse] = "data"
    experiment_uuid: Optional[UUID] = None
    experiment_timestamp: Optional[datetime] = None
    action_timestamp: Optional[datetime] = None
    action_status: List[HloStatus] = Field(default=[])
    action_order: Optional[int] = 0
    action_retry: Optional[int] = 0
    action_split: Optional[int] = 0
    action_name: Optional[str] = None
    action_sub_name: Optional[str] = None
    action_abbr: Optional[str] = None
    action_params: dict = Field(default={})
    action_output: dict = Field(default={})
    action_etc: Optional[float] = None # expected time to completion
    action_codehash: Optional[str] = None
    parent_action_uuid: Optional[UUID] = None
    child_action_uuid: Optional[UUID] = None
    samples_in: List[SampleUnion] = Field(default=[])
    samples_out: List[SampleUnion] = Field(default=[])
    files: List[FileInfo] = Field(default=[])
    manual_action: bool = False
    nonblocking: bool = False
    exec_id: Optional[str] = None
    technique_name: Optional[Union[str, list]] = None
    process_finish: bool = False
    process_contrib: List[ProcessContrib] = Field(default=[])
    error_code: Optional[ErrorCodes] = ErrorCodes.none
    process_uuid: Optional[UUID] = None
    data_request_id: Optional[UUID] = None
    # process_group_index: Optional[int] = 0 # unnecessary if we rely on process_finish as group terminator

    @property
    def url(self):
        return f"http://{self.action_server.hostname}:{self.action_server.port}/{self.action_server.server_name}/{self.action_name}"
