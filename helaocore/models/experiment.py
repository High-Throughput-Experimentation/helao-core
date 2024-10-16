__all__ = ["ExperimentTemplate", "ExperimentModel", "ShortExperimentModel"]

from datetime import datetime
from typing import List, Optional, Dict
from uuid import UUID
from pathlib import Path

from pydantic import BaseModel, Field

from helaocore.models.hlostatus import HloStatus
from helaocore.models.sample import SampleUnion
from helaocore.models.action import ShortActionModel
from helaocore.models.file import FileInfo
from helaocore.models.machine import MachineModel

from helaocore.version import get_hlo_version
from helaocore.helaodict import HelaoDict


class ShortExperimentModel(BaseModel, HelaoDict):
    experiment_uuid: Optional[UUID] = None
    experiment_name: Optional[str] = None
    experiment_output_dir: Optional[Path] = None
    orch_key: Optional[str] = None
    orch_host: Optional[str] = None
    orch_port: Optional[int] = None
    data_request_id: Optional[UUID] = None


class ExperimentTemplate(BaseModel, HelaoDict):
    experiment_name: Optional[str] = None
    experiment_params: Optional[dict] = {}
    data_request_id: Optional[UUID] = None


class ExperimentModel(ExperimentTemplate):
    hlo_version: Optional[str] = Field(default_factory=get_hlo_version)
    orchestrator: MachineModel = MachineModel()
    access: Optional[str] = 'hte'
    dummy: bool = False
    simulation: bool = False
    # name of "instrument": eche, anec, adss etc. defined in world config
    run_type: Optional[str] = None
    sequence_uuid: Optional[UUID] = None
    experiment_uuid: Optional[UUID] = None
    experiment_timestamp: Optional[datetime] = None
    experiment_status: List[HloStatus] = Field(default=[])
    experiment_output_dir: Optional[Path] = None
    experiment_codehash: Optional[str] = None
    experiment_label: Optional[str] = None
    action_list: List[ShortActionModel] = Field(default=[])
    samples_in: List[SampleUnion] = Field(default=[])
    samples_out: List[SampleUnion] = Field(default=[])
    files: List[FileInfo] = Field(default=[])
    process_list: List[UUID] = Field(default=[])  # populated by DB yml_finisher
    process_order_groups: Dict[int, List[int]] = Field(default={})
    data_request_id: Optional[UUID] = None
    orch_key: Optional[str] = None
    orch_host: Optional[str] = None
    orch_port: Optional[int] = None