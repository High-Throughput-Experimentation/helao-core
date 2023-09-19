__all__ = ["MachineModel"]

from typing import Optional
from pydantic import BaseModel

from helaocore.helaodict import HelaoDict


class MachineModel(BaseModel, HelaoDict):
    server_name: Optional[str]
    machine_name: Optional[str]
    hostname: Optional[str]
    port: Optional[int]

    def as_key(self):
        """generates a unique machine/servername
        which can used in dicts as a key"""
        return (self.server_name, self.machine_name)

    def disp_name(self):
        return f"{self.server_name}@{self.machine_name}"
