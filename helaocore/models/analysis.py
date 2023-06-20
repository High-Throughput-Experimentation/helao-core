__all__ = ["AnalysisModel", "ShortAnalysisModel", "AnalysisDataModel", "AnalysisOutputModel"]

from enum import Enum
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field

from helaocore.models.run_use import RunUse
from helaocore.version import get_hlo_version
from helaocore.helaodict import HelaoDict


class AnalysisOutputType(str, Enum):
    primary = "primary"
    auxiliary = "auxiliary"
    intermediate = "intermediate"


class ShortAnalysisModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = get_hlo_version()
    analysis_uuid: Optional[UUID]


class AnalysisDataModel(BaseModel, HelaoDict):
    action_uuid: UUID
    run_use: RunUse = "data"
    raw_data_path: str
    global_sample_label: Optional[str]


class AnalysisOutputModel(BaseModel, HelaoDict):
    analysis_output_path: str
    output_keys: Optional[List[str]]
    output_type: Optional[AnalysisOutputType]
    output_name: Optional[str]
    output: Optional[dict] = Field(default={})


class AnalysisModel(ShortAnalysisModel):
    access: Optional[str] = "hte"
    dummy: bool = False
    simulation: bool = False
    analysis_name: str
    analysis_params: dict = Field(default={})
    analysis_codehash: Optional[str]
    inputs: List[AnalysisDataModel]
    output: AnalysisOutputModel
