__all__ = ["AnalysisModel", "ShortAnalysisModel", "AnalysisDataModel", "AnalysisOutputModel"]

from enum import Enum
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


class AnalysisOutputType(str, Enum):
    primary = "primary"
    intermediate = "intermediate"


class ShortAnalysisModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = get_hlo_version()
    analysis_uuid: Optional[UUID]


class AnalysisDataModel(BaseModel, HelaoDict):
    action_uuid: UUID
    run_use: RunUse = "data"
    raw_data_path: str
    sample_label: Optional[SampleUnion]


class AnalysisOutputModel(BaseModel, HelaoDict):
    analysis_output_path: str
    output_type: AnalysisOutputType
    output_name: Optional[str]
    output_keys: List[str]
    output: dict = Field(default={})


class AnalysisModel(ShortAnalysisModel):
    access: Optional[str] = "hte"
    dummy: bool = False
    simulation: bool = False
    analysis_name: str
    analysis_params: dict = Field(default={})
    analysis_code_path: str
    inputs: List[AnalysisDataModel]
    outputs: List[AnalysisOutputModel]
