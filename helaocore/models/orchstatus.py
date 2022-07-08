__all__ = ["OrchStatus"]

from enum import Enum


class OrchStatus(str, Enum):
    idle = "idle"
    error = "error"
    busy = "busy"
    started = "started"
    estop = "estop"
    stopped = "stopped"
    skip = "skip"
    stop = "stop"
    none = "none"
