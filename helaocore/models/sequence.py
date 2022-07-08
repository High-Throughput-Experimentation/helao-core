__all__ = ["SequenceTemplate", "SequenceModel"]

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pathlib import Path

from pydantic import BaseModel, Field

from helaocore.models.hlostatus import HloStatus
from helaocore.models.experiment import ShortExperimentModel, ExperimentTemplate

from helaocore.version import get_hlo_version
from helaocore.helaodict import HelaoDict


class SequenceTemplate(BaseModel, HelaoDict):
    sequence_name: Optional[str]
    sequence_params: Optional[dict] = Field(default_factory=dict)
    sequence_label: Optional[str] = "noLabel"
    experiment_plan_list: List[ExperimentTemplate] = Field(default_factory=list)


class SequenceModel(SequenceTemplate):
    hlo_version: Optional[str] = get_hlo_version()
    access: Optional[str] = 'hte'
    dummy: bool = False
    simulation: bool = False
    sequence_uuid: Optional[UUID]
    sequence_timestamp: Optional[datetime]
    sequence_status: List[HloStatus] = Field(default_factory=list)
    sequence_output_dir: Optional[Path]
    experiment_list: List[ShortExperimentModel] = Field(default_factory=list)
