__all__ = [
           "StatusModel",
           "StatusPackageModel"
          ]

from typing import List, Dict
from uuid import UUID
from pydantic import BaseModel, Field


from ..helper.helaodict import HelaoDict
from ..error import error_codes


class StatusModel(BaseModel, HelaoDict):
    pass

class StatusPackageModel(BaseModel, HelaoDict):
    pass
