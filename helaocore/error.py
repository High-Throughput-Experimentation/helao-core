__all__ = ["ErrorCodes"]

from enum import Enum


class ErrorCodes(str, Enum):
    none = "none"
    critical = "critical_error"
    start_timeout = "start_timeout"
    continue_timeout = "continue_timeout"
    done_timeout = "done_timeout"
    in_progress = "already_in_progress"
    not_available = "not_available"
    ssh_error = "ssh_error"
    not_initialized = "not_initialized"
    bug = "bug"
    cmd_error = "cmd_error"
