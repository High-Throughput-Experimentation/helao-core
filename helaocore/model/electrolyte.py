__all__ = ["Electrolyte"]
from enum import Enum


class Electrolyte(str, Enum):
    slf10 = "SLF10"
    oer10 = "OER10"
    pslf10 = "PSLF10"
