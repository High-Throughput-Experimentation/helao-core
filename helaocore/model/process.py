__all__ = [
           "ProcessTemplate",
           "ProcessModel"
          ]

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel
from helaocore.model.sample import SampleUnion
from helaocore.model.action import ActionModel

from helaocore.server import version



class ProcessTemplate(BaseModel):
    access: Optional[str]
    technique_name: Optional[str]
    process_name: Optional[str]
    process_params: Optional[dict]

    def make_process(
                     self, 
                     orchestrator: str, 
                     machine_name: str, 
                     sequence_uuid: UUID, 
                     process_uuid: UUID, 
                     process_timestamp: datetime, 
                    ):
        return ProcessModel(
                        **self.dict(),
                        orchestrator = orchestrator,
                        machine_name = machine_name,
                        sequence_uuid = sequence_uuid,
                        process_uuid = process_uuid,
                        process_timestamp = process_timestamp
        )


class ProcessModel(ProcessTemplate):
    hlo_version: Optional[str] = version.hlo_version
    orchestrator: Optional[str]
    machine_name: Optional[str]
    sequence_uuid: Optional[UUID]
    process_uuid: Optional[UUID]
    process_timestamp: Optional[datetime]
    action_uuid_list: Optional[List[UUID]]
    samples_in: Optional[List[SampleUnion]]
    samples_out: Optional[List[SampleUnion]]
    files: Optional[dict]
    _action_list: Optional[List[ActionModel]]

    def get_samples_out(self):
        samples_out = set()
        for action in self._action_list:
            samples_out.update(action.samples_in)
            samples_out.update(action.samples_out)
        return samples_out
