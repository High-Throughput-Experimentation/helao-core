__all__ = [
           "ExperimentSequenceTemplate",
           "ExperimentSequenceModel"
           ]

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from .hlostatus import HloStatus
from .experiment import ShortExperimentModel

from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict


class ExperimentSequenceTemplate(BaseModel, HelaoDict):
    sequence_name: Optional[str]
    sequence_params: Optional[dict] = Field(default_factory=dict)
    sequence_label: Optional[str]
    experiment_plan_list: List[str] = Field(default_factory=list)


    def make_sequence(
                      self, 
                      sequence_timestamp: datetime,
                      sequence_uuid: UUID
                     ):
        return ExperimentSequenceModel(
            **self.dict(), 
            sequence_timestamp=sequence_timestamp, 
            sequence_uuid=sequence_uuid
        )


class ExperimentSequenceModel(ExperimentSequenceTemplate):
    hlo_version: Optional[str] = get_hlo_version()
    sequence_uuid: Optional[UUID]
    sequence_timestamp: Optional[datetime]
    sequence_status: List[HloStatus] = Field(default_factory=list)
    sequence_output_dir: Optional[str]
    experiment_list: List[ShortExperimentModel] = Field(default_factory=list)
