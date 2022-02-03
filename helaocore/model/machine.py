__all__ = [
           "MachineModel"
          ]

from typing import Optional
from pydantic import BaseModel

from ..helper.helaodict import HelaoDict


class MachineModel(BaseModel, HelaoDict):
    server_name: Optional[str]
    machine_name: Optional[str]
