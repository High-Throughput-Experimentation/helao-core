__all__ = [
           "FileInfo"
          ]

from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from ..helper.helaodict import HelaoDict


class FileInfo(BaseModel, HelaoDict):
    file_type: Optional[str]
    file_name: Optional[str]
    data_keys: List[str] = Field(default_factory=list)
    sample: List[str] = Field(default_factory=list)
    action_uuid: Optional[UUID]
