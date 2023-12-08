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
    sequence_name: Optional[str] = None
    sequence_params: Optional[dict] = {}
    sequence_label: Optional[str] = "noLabel"
    experiment_plan_list: List[ExperimentTemplate] = Field(default=[])


class SequenceModel(SequenceTemplate):
    hlo_version: Optional[str] = get_hlo_version()
    access: Optional[str] = 'hte'
    dummy: bool = False
    simulation: bool = False
    sequence_uuid: Optional[UUID] = None
    sequence_timestamp: Optional[datetime] = None
    sequence_status: List[HloStatus] = Field(default=[])
    sequence_output_dir: Optional[Path] = None
    sequence_codehash: Optional[str] = None
    sequence_comment: Optional[str] = None
    experiment_list: List[ShortExperimentModel] = Field(default=[])
    data_request_id: Optional[UUID] = None
