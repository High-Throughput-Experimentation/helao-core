__all__ = ["ActionStartCondition"]

from enum import Enum


class ActionStartCondition(int, Enum):
    no_wait = 0  # orch is dispatching an unconditional action
    wait_for_endpoint = 1  # orch is waiting for endpoint to become available
    wait_for_server = 2  # orch is waiting for server to become available
    wait_for_all = 3  # (or other): orch is waiting for all action_dq to finish
