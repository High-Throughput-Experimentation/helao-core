__all__ = [
           "ProcessTemplate",
           "ProcessModel"
          ]

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from .sample import SampleUnion
from .action import ActionModel, ShortActionModel
from .fileinfo import FileInfo
from ..server import version
from ..helper.helaodict import HelaoDict


class ProcessTemplate(BaseModel, HelaoDict):
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
    process_status: Optional[str]
    action_list: List[ShortActionModel] = Field(default_factory=list)
    samples_in: List[SampleUnion] = Field(default_factory=list)
    samples_out: List[SampleUnion] = Field(default_factory=list)
    files: List[FileInfo] = Field(default_factory=list)
    _action_list: List[ActionModel] = []


    def add_action(self, act: ActionModel):
        self._action_list.append(act)


    def update_from_actlist(self):
        for actm in self._action_list:
            self.action_list.append(ShortActionModel(**actm.dict()))

            for file in actm.files:
                if file.action_uuid is None:
                    file.action_uuid = actm.action_uuid
                self.files.append(file)

            for _sample in actm.samples_in:
                if _sample.action_uuid is None:
                    _sample.action_uuid = actm.action_uuid
                self.samples_in.append(_sample)

            for _sample in actm.samples_in:
                if _sample.action_uuid is None:
                    _sample.action_uuid = actm.action_uuid
                self.samples_out.append(_sample)
