__all__ = [
           "ExperimentTemplate",
           "ExperimentModel",
           "ShortExperimentModel"
          ]

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pathlib import Path

from pydantic import BaseModel, Field

from .hlostatus import HloStatus
from .sample import SampleUnion
from .action import ShortActionModel
from .file import FileInfo
from .machine import MachineModel

from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict


class ShortExperimentModel(BaseModel, HelaoDict):
    experiment_uuid: Optional[UUID]
    experiment_name: Optional[str]
    experiment_output_dir: Optional[Path]


class ExperimentTemplate(BaseModel, HelaoDict):
    access: Optional[str]
    # name of "instrument": sdc, anec, adss etc. defined in world config
    technique_name: Optional[str]
    experiment_name: Optional[str]
    experiment_params: Optional[dict] = Field(default_factory=dict)

    def make_experiment(
                     self, 
                     orchestrator: MachineModel,
                     sequence_uuid: UUID, 
                     experiment_uuid: UUID, 
                     experiment_timestamp: datetime, 
                    ):
        return ExperimentModel(
                        **self.dict(),
                        orchestrator = orchestrator,
                        sequence_uuid = sequence_uuid,
                        experiment_uuid = experiment_uuid,
                        experiment_timestamp = experiment_timestamp
        )


class ExperimentModel(ExperimentTemplate):
    hlo_version: Optional[str] = get_hlo_version()
    orchestrator: MachineModel = MachineModel()
    sequence_uuid: Optional[UUID]
    experiment_uuid: Optional[UUID]
    experiment_timestamp: Optional[datetime]
    experiment_status: List[HloStatus] = Field(default_factory=list)
    experiment_output_dir: Optional[Path]
    action_list: List[ShortActionModel] = Field(default_factory=list)
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)

