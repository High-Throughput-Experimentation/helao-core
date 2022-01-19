__all__ = [
           "HloFileGroup",
           "HloHeaderModel",
           "FileConnParams",
           "FileConn",
           "FileInfo"
          ]

from enum import Enum
from typing import List, Optional, Dict
from pydantic import BaseModel, Field
from uuid import UUID


from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict

class HloFileGroup(str, Enum):
    aux_files = "aux_files"
    helao_files = "helao_files"


class HloHeaderModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = get_hlo_version()
    action_name: Optional[str]
    column_headings: List[str] = Field(default_factory=list)
    # this can hold instrument/server specific optional header 
    # entries
    optional: Optional[Dict] = Field(default_factory=dict)
    epoch_ns: Optional[float]


class FileConnParams(BaseModel, HelaoDict):
    # we require a file conn key
    # cannot be uuid 'object' as we might have more then one file
    # either use sample_label, or str(action_uuid) (if only one file etc
    file_conn_key: UUID

    # but samples are optional
    # only need the global label, but not the full sample basemodel
    sample_global_labels: List[str] = Field(default_factory=list)
    json_data_keys: List[str] = Field(default_factory=list)
    # type of file
    file_type: str = "helao__file"
    file_group: Optional[HloFileGroup] = HloFileGroup.helao_files
    # None will trigger autogeneration of a file name
    file_name: Optional[str]
    # the header of the hlo file as dict (will be written as yml)
    hloheader: Optional[HloHeaderModel] = HloHeaderModel()


class FileConn(BaseModel, HelaoDict):
    """This is an internal BaseModel for Base which will hold all 
    file connections.
    """
    params: FileConnParams
    added_hlo_separator: bool = False
    # holds the file reference
    file: Optional[object]

    
class FileInfo(BaseModel, HelaoDict):
    file_type: Optional[str]
    file_name: Optional[str]
    data_keys: List[str] = Field(default_factory=list)
    sample: List[str] = Field(default_factory=list)
    action_uuid: Optional[UUID]
