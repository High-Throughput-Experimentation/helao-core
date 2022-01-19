__all__ = [
           "DataModel",
           "DataPackageModel"
          ]

from typing import List, Dict
from uuid import UUID
from pydantic import BaseModel, Field


from ..helper.helaodict import HelaoDict
from ..error import error_codes


class DataModel(BaseModel, HelaoDict):
    # data is contained in a dict and keyed by file_conn_key
    data: Dict[UUID, dict] = Field(default_factory=dict)
    errors: List[error_codes] = Field(default_factory=list)


class DataPackageModel(BaseModel, HelaoDict):
    action_uuid: UUID
    action_name: str
    datamodel: DataModel
    errors: List[error_codes] = Field(default_factory=list)
