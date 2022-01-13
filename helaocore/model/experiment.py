__all__ = [
           "ExperimentTemplate",
           "ExperimentModel",
           "ShortExperimentModel"
          ]

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from .sample import SampleUnion
from .action import ShortActionModel
from .fileinfo import FileInfo
from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict


class ShortExperimentModel(BaseModel, HelaoDict):
    experiment_uuid: Optional[UUID]
    experiment_name: Optional[str]
    output_dir: Optional[str]


class ExperimentTemplate(BaseModel, HelaoDict):
    access: Optional[str]
    technique_name: Optional[str]
    experiment_name: Optional[str]
    experiment_params: Optional[dict]

    def make_experiment(
                     self, 
                     orchestrator: str, 
                     machine_name: str, 
                     sequence_uuid: UUID, 
                     experiment_uuid: UUID, 
                     experiment_timestamp: datetime, 
                    ):
        return ExperimentModel(
                        **self.dict(),
                        orchestrator = orchestrator,
                        machine_name = machine_name,
                        sequence_uuid = sequence_uuid,
                        experiment_uuid = experiment_uuid,
                        experiment_timestamp = experiment_timestamp
        )


class ExperimentModel(ExperimentTemplate):
    hlo_version: Optional[str] = get_hlo_version()
    orchestrator: Optional[str]
    machine_name: Optional[str]
    sequence_uuid: Optional[UUID]
    experiment_uuid: Optional[UUID]
    experiment_timestamp: Optional[datetime]
    experiment_status: Optional[str]
    output_dir: Optional[str]
    action_list: List[ShortActionModel] = Field(default_factory=list)
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)

