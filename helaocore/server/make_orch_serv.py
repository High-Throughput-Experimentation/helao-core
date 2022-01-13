__all__ = ["makeOrchServ"]

import asyncio
import json
import time

from fastapi import WebSocket
from typing import Optional, Union, List

from .api import HelaoFastAPI
from .orch import Orch
from ..model.action import ActionModel

def makeOrchServ(config, server_key, server_title, description, version, driver_class=None):
    app = HelaoFastAPI(config, server_key, title=server_title, description=description, version=version)

    @app.on_event("startup")
    async def startup_event():
        """Run startup actions.

        When FastAPI server starts, create a global OrchHandler object, initiate the
        monitor_states coroutine which runs forever, and append dummy experiments to the
        experiment queue for testing.
        """
        app.orch = Orch(app)
        if driver_class:
            app.driver = driver_class(app.orch)

    @app.post("/update_status")
    async def update_status(server: str, status: str):
        app.orch.print_message(f"got status from {server}: {status}")
        return await app.orch.update_status(action_serv=server, status_dict=json.loads(status))

    @app.post("/attach_client")
    async def attach_client(client_servkey: str):
        return await app.orch.attach_client(client_servkey)

    @app.websocket("/ws_status")
    async def websocket_status(websocket: WebSocket):
        """Subscribe to orchestrator status messages.

        Args:
        websocket: a fastapi.WebSocket object
        """
        await app.orch.ws_status(websocket)

    @app.websocket("/ws_data")
    async def websocket_data(websocket: WebSocket):
        """Subscribe to action server status dicts.

        Args:
        websocket: a fastapi.WebSocket object
        """
        await app.orch.ws_data(websocket)

    @app.post("/start")
    async def start():
        """Begin experimenting experiment and action queues."""
        if app.orch.loop_state == "stopped":
            if app.orch.action_dq or app.orch.experiment_dq or app.orch.sequence_dq:  # resume actions from a paused run
                await app.orch.start_loop()
            else:
                app.orch.print_message("experiment list is empty")
        else:
            app.orch.print_message("already running")
        return {}

    @app.post("/estop")
    async def estop():
        """Emergency stop experiment and action queues, interrupt running actions."""
        if app.orch.loop_state == "started":
            await app.orch.estop_loop()
        elif app.orch.loop_state == "E-STOP":
            app.orch.print_message("orchestrator E-STOP flag already raised")
        else:
            app.orch.print_message("orchestrator is not running")
        return {}

    @app.post("/stop")
    async def stop():
        """Stop experimenting experiment and action queues after current actions finish."""
        if app.orch.loop_state == "started":
            await app.orch.intend_stop()
        elif app.orch.loop_state == "E-STOP":
            app.orch.print_message("orchestrator E-STOP flag was raised; nothing to stop")
        else:
            app.orch.print_message("orchestrator is not running")
        return {}

    @app.post("/clear_estop")
    async def clear_estop():
        """Remove emergency stop condition."""
        if app.orch.loop_state != "E-STOP":
            app.orch.print_message("orchestrator is not currently in E-STOP")
        else:
            await app.orch.clear_estate(clear_estop=True, clear_error=False)

    @app.post("/clear_error")
    async def clear_error():
        """Remove error condition."""
        if app.orch.loop_state != "ERROR":
            app.orch.print_message("orchestrator is not currently in ERROR")
        else:
            await app.orch.clear_estate(clear_estop=False, clear_error=True)

    @app.post("/skip")
    async def skip_experiment():
        """Clear the present action queue while running."""
        if app.orch.loop_state == "started":
            await app.orch.intend_skip()
        else:
            app.orch.print_message("orchestrator not running, clearing action queue")
            await asyncio.sleep(0.001)
            app.orch.action_dq.clear()
        return {}

    @app.post("/clear_actions")
    async def clear_actions():
        """Clear the present action queue while stopped."""
        app.orch.print_message("clearing action queue")
        await asyncio.sleep(0.001)
        app.orch.action_dq.clear()
        return {}

    @app.post("/clear_experiments")
    async def clear_experiments():
        """Clear the present experiment queue while stopped."""
        app.orch.print_message("clearing experiment queue")
        await asyncio.sleep(0.001)
        app.orch.experiment_dq.clear()
        return {}


    @app.post("/append_sequence")
    async def append_sequence(
        sequence_uuid: str = None,
        sequence_timestamp: str = None,
        sequence_name: str = None,
        sequence_params: dict = None,
        sequence_label: str = None,
        experiment_plan_list: List[dict] = [],
        scratch: Optional[List[None]] = [None], # temp fix so it still works (issue with lists)
    ):
        await app.orch.add_sequence(
            sequence_uuid = sequence_uuid,
            sequence_timestamp = sequence_timestamp,
            sequence_name = sequence_name,
            sequence_params = sequence_params,
            sequence_label = sequence_label,
            experiment_plan_list = experiment_plan_list
        )
        return {}


    # @app.post("/append_experiment")
    # async def append_experiment(
    #     orchestrator: str = None,
    #     experiment_name: str = None,
    #     experiment_params: dict = {},
    #     result_dict: dict = {},
    #     access: str = "hte",
    # ):
    #     """Add a experiment object to the end of the experiment queue.

    #     Args:
    #     experiment_dict: experiment parameters (optional), as dict.
    #     orchestrator: Orchestrator server key (optional), as str.
    #     plate_id: The sample's plate id (no checksum), as int.
    #     sample_no: A sample number, as int.
    #     experiment_name: The name of the experiment for building the action list, as str.
    #     experiment_params: experiment parameters, as dict.
    #     result_dict: action responses dict.
    #     access: Access control group, as str.

    #     Returns:
    #     Nothing.
    #     """
    #     await app.orch.add_experiment(
    #         orchestrator,
    #         experiment_name,
    #         experiment_params,
    #         result_dict,
    #         access,
    #         prepend=False,
    #     )
    #     return {}

    # @app.post("/prepend_experiment")
    # async def prepend_experiment(
    #     orchestrator: str = None,
    #     experiment_name: str = None,
    #     experiment_params: dict = {},
    #     result_dict: dict = {},
    #     access: str = "hte",
    # ):
    #     """Add a experiment object to the start of the experiment queue.

    #     Args:
    #     experiment_dict: experiment parameters (optional), as dict.
    #     orchestrator: Orchestrator server key (optional), as str.
    #     plate_id: The sample's plate id (no checksum), as int.
    #     sample_no: A sample number, as int.
    #     experiment_name: The name of the experiment for building the action list, as str.
    #     experiment_params: experiment parameters, as dict.
    #     result_dict: action responses dict.
    #     access: Access control group, as str.

    #     Returns:
    #     Nothing.
    #     """
    #     await app.orch.add_experiment(
    #         orchestrator,
    #         experiment_name,
    #         experiment_params,
    #         result_dict,
    #         access,
    #         prepend=True,
    #     )
    #     return {}

    # @app.post("/insert_experiment")
    # async def insert_experiment(
    #     idx: int,
    #     experiment_dict: dict = None,
    #     orchestrator: str = None,
    #     experiment_name: str = None,
    #     experiment_params: dict = {},
    #     result_dict: dict = {},
    #     access: str = "hte",
    # ):
    #     """Insert a experiment object at experiment queue index.

    #     Args:
    #     idx: index in experiment queue for insertion, as int
    #     experiment_dict: experiment parameters (optional), as dict.
    #     orchestrator: Orchestrator server key (optional), as str.
    #     plate_id: The sample's plate id (no checksum), as int.
    #     sample_no: A sample number, as int.
    #     experiment_name: The name of the experiment for building the action list, as str.
    #     experiment_params: experiment parameters, as dict.
    #     result_dict: action responses dict.
    #     access: Access control group, as str.

    #     Returns:
    #     Nothing.
    #     """
    #     await app.orch.add_experiment(
    #         experiment_dict,
    #         orchestrator,
    #         experiment_name,
    #         experiment_params,
    #         result_dict,
    #         access,
    #         at_index=idx,
    #     )
    #     return {}

    @app.post("/list_experiments")
    def list_experiments():
        """Return the current list of experiments."""
        return app.orch.list_experiments()

    @app.post("/active_experiment")
    def active_experiment():
        """Return the active experiment."""
        return app.orch.get_experiment(last=False)

    @app.post("/last_experiment")
    def last_experiment():
        """Return the last experiment."""
        return app.orch.get_action_group(last=True)

    @app.post("/list_actions")
    def list_actions():
        """Return the current list of actions."""
        return app.orch.list_actions()

    @app.post("/list_active_actions")
    def list_active_actions():
        """Return the current list of actions."""
        return app.orch.list_active_actions()

    @app.post("/endpoints")
    def get_all_urls():
        """Return a list of all endpoints on this server."""
        return app.orch.get_endpoint_urls(app)

    @app.on_event("shutdown")
    def disconnect():
        """Run shutdown actions."""
        # emergencyStop = True
        time.sleep(0.75)

    return app
