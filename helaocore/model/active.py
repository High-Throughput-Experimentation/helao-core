__all__ = ["ActiveParams"]

from typing import List
from pydantic import BaseModel, Field, validator
from uuid import UUID


from .file import FileConnParams

from ..schema import Action
from ..helper.helaodict import HelaoDict


class ActiveParams(BaseModel, HelaoDict):
    # the Action object for this action
    action: Action
    # a list of data file connection parameters
    file_conn_params_list: List[FileConnParams] = Field(default_factory=list)
    aux_listen_uuids: List[UUID] = Field(default_factory=list)


    class Config:
        arbitrary_types_allowed = True


    @validator("action")
    def validate_action(cls, v):
        return v