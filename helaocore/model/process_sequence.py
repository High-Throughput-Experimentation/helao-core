__all__ = [
           "ProcessSequenceTemplate",
           "ProcessSequenceModel"
           ]

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


from helaocore.server import version


class ProcessSequenceTemplate(BaseModel):
    sequence_name: Optional[str]
    sequence_params: Optional[dict]
    sequence_label: Optional[str]
    process_plan_list: Optional[List[str]]

    def make_sequence(
                      self, 
                      sequence_timestamp: datetime, 
                      sequence_uuid: UUID
                     ):
        return ProcessSequenceModel(
            **self.dict(), 
            sequence_timestamp=sequence_timestamp, 
            sequence_uuid=sequence_uuid
        )


class ProcessSequenceModel(ProcessSequenceTemplate):
    hlo_version: Optional[str] = version.hlo_version
    sequence_uuid: UUID
    sequence_timestamp: datetime
    process_list: List[UUID] = Field(default_factory=list)
