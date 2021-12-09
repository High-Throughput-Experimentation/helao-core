__all__ = ["ActionModel"]

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel
from helaocore.model.sample import SampleUnion

from helaocore.server import version

class ActionModel(BaseModel):
    hlo_version: Optional[str] = version.hlo_version
    technique_name: Optional[str]
    server_name: Optional[str]
    orchestrator: Optional[str]
    machine_name: Optional[str]
    access: Optional[str]
    output_dir: Optional[str]
    process_uuid: Optional[str]
    process_timestamp: Optional[datetime]
    action_uuid: Optional[UUID]
    action_timestamp: Optional[datetime]
    action_order: Optional[int] = 0
    action_retry: Optional[int] = 0
    action_actual_order: Optional[int] = 0
    action_name: Optional[str]
    action_abbr: Optional[str]
    action_params: Optional[dict]
    samples_in: Optional[List[SampleUnion]]
    samples_out: Optional[List[SampleUnion]]
    files: Optional[dict]
