__all__ = ["AnalysisModel", "ShortAnalysisModel", "AnalysisDataModel", "AnalysisOutputModel"]

from enum import Enum
from typing import List, Optional, Union, Dict
from uuid import UUID
from pydantic import BaseModel, Field

from helaocore.models.run_use import RunUse
from helaocore.version import get_hlo_version
from helaocore.helaodict import HelaoDict
from helaocore.models.s3locator import S3Locator


class ShortAnalysisModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = get_hlo_version()
    analysis_uuid: Optional[UUID]


class AnalysisDataModel(BaseModel, HelaoDict):
    action_uuid: UUID
    run_use: RunUse = "data"
    raw_data_path: str
    global_sample_label: Optional[str]
    composition: Optional[dict]


class AnalysisOutputModel(BaseModel, HelaoDict):
    analysis_output_path: S3Locator
    content_type: str
    output_keys: Optional[List[str]]
    output_name: Optional[str]
    output: Optional[Dict[str, Union[float, str, bool, int, None]]]


class AnalysisModel(ShortAnalysisModel):
    access: Optional[str] = "hte"
    dummy: bool = False
    simulation: bool = False
    analysis_name: str
    analysis_params: dict
    analysis_codehash: Optional[str]
    process_uuid: Optional[UUID]
    process_params: Optional[dict]
    inputs: List[AnalysisDataModel]
    outputs: List[AnalysisOutputModel]
