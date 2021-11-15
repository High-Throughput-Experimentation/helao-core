__all__ = ["makeOrchServ"]

import asyncio
import json
import time

from fastapi import WebSocket

from .api import HelaoFastAPI
from .orch import Orch


def makeOrchServ(config, server_key, server_title, description, version, driver_class=None):
    app = HelaoFastAPI(config, server_key, title=server_title, description=description, version=version)

    @app.on_event("startup")
    async def startup_event():
        """Run startup processes.

        When FastAPI server starts, create a global OrchHandler object, initiate the
        monitor_states coroutine which runs forever, and append dummy sequences to the
        sequence queue for testing.
        """
        app.orch = Orch(app)
        if driver_class:
            app.driver = driver_class(app.orch)

    @app.post("/update_status")
    async def update_status(server: str, status: str):
        return await app.orch.update_status(process_serv=server, status_dict=json.loads(status))

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
        """Subscribe to process server status dicts.

        Args:
        websocket: a fastapi.WebSocket object
        """
        await app.orch.ws_data(websocket)

    @app.post("/start")
    async def start_process():
        """Begin processing sequence and process queues."""
        if app.orch.loop_state == "stopped":
            if app.orch.process_dq or app.orch.sequence_dq:  # resume processes from a paused run
                await app.orch.start_loop()
            else:
                app.orch.print_message("sequence list is empty")
        else:
            app.orch.print_message("already running")
        return {}

    @app.post("/estop")
    async def estop_process():
        """Emergency stop sequence and process queues, interrupt running processes."""
        if app.orch.loop_state == "started":
            await app.orch.estop_loop()
        elif app.orch.loop_state == "E-STOP":
            app.orch.print_message("orchestrator E-STOP flag already raised")
        else:
            app.orch.print_message("orchestrator is not running")
        return {}

    @app.post("/stop")
    async def stop_process():
        """Stop processing sequence and process queues after current processes finish."""
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
    async def skip_sequence():
        """Clear the present process queue while running."""
        if app.orch.loop_state == "started":
            await app.orch.intend_skip()
        else:
            app.orch.print_message("orchestrator not running, clearing process queue")
            await asyncio.sleep(0.001)
            app.orch.process_dq.clear()
        return {}

    @app.post("/clear_processes")
    async def clear_processes():
        """Clear the present process queue while stopped."""
        app.orch.print_message("clearing process queue")
        await asyncio.sleep(0.001)
        app.orch.process_dq.clear()
        return {}

    @app.post("/clear_sequences")
    async def clear_sequences():
        """Clear the present sequence queue while stopped."""
        app.orch.print_message("clearing sequence queue")
        await asyncio.sleep(0.001)
        app.orch.sequence_dq.clear()
        return {}

    @app.post("/append_sequence")
    async def append_sequence(
        orch_name: str = None,
        sequence_label: str = None,
        sequence_name: str = None,
        sequence_params: dict = {},
        result_dict: dict = {},
        access: str = "hte",
    ):
        """Add a sequence object to the end of the sequence queue.

        Args:
        sequence_dict: sequence parameters (optional), as dict.
        orch_name: Orchestrator server key (optional), as str.
        plate_id: The sample's plate id (no checksum), as int.
        sample_no: A sample number, as int.
        sequence_name: The name of the sequence for building the process list, as str.
        sequence_params: sequence parameters, as dict.
        result_dict: process responses dict keyed by process_enum.
        access: Access control group, as str.

        Returns:
        Nothing.
        """
        await app.orch.add_sequence(
            orch_name,
            sequence_label,
            sequence_name,
            sequence_params,
            result_dict,
            access,
            prepend=False,
        )
        return {}

    @app.post("/prepend_sequence")
    async def prepend_sequence(
        orch_name: str = None,
        sequence_label: str = None,
        sequence_name: str = None,
        sequence_params: dict = {},
        result_dict: dict = {},
        access: str = "hte",
    ):
        """Add a sequence object to the start of the sequence queue.

        Args:
        sequence_dict: sequence parameters (optional), as dict.
        orch_name: Orchestrator server key (optional), as str.
        plate_id: The sample's plate id (no checksum), as int.
        sample_no: A sample number, as int.
        sequence_name: The name of the sequence for building the process list, as str.
        sequence_params: sequence parameters, as dict.
        result_dict: process responses dict keyed by process_enum.
        access: Access control group, as str.

        Returns:
        Nothing.
        """
        await app.orch.add_sequence(
            orch_name,
            sequence_label,
            sequence_name,
            sequence_params,
            result_dict,
            access,
            prepend=True,
        )
        return {}

    @app.post("/insert_sequence")
    async def insert_sequence(
        idx: int,
        sequence_dict: dict = None,
        orch_name: str = None,
        sequence_label: str = None,
        sequence_name: str = None,
        sequence_params: dict = {},
        result_dict: dict = {},
        access: str = "hte",
    ):
        """Insert a sequence object at sequence queue index.

        Args:
        idx: index in sequence queue for insertion, as int
        sequence_dict: sequence parameters (optional), as dict.
        orch_name: Orchestrator server key (optional), as str.
        plate_id: The sample's plate id (no checksum), as int.
        sample_no: A sample number, as int.
        sequence_name: The name of the sequence for building the process list, as str.
        sequence_params: sequence parameters, as dict.
        result_dict: process responses dict keyed by process_enum.
        access: Access control group, as str.

        Returns:
        Nothing.
        """
        await app.orch.add_sequence(
            sequence_dict,
            orch_name,
            sequence_label,
            sequence_name,
            sequence_params,
            result_dict,
            access,
            at_index=idx,
        )
        return {}

    @app.post("/list_sequences")
    def list_sequences():
        """Return the current list of sequences."""
        return app.orch.list_sequences()

    @app.post("/active_sequence")
    def active_sequence():
        """Return the active sequence."""
        return app.orch.get_sequence(last=False)

    @app.post("/last_sequence")
    def last_sequence():
        """Return the last sequence."""
        return app.orch.get_process_group(last=True)

    @app.post("/list_processes")
    def list_processes():
        """Return the current list of processes."""
        return app.orch.list_processes()

    @app.post("/list_active_processes")
    def list_active_processes():
        """Return the current list of processes."""
        return app.orch.list_active_processes()

    @app.post("/endpoints")
    def get_all_urls():
        """Return a list of all endpoints on this server."""
        return app.orch.get_endpoint_urls(app)

    @app.on_event("shutdown")
    def disconnect():
        """Run shutdown processes."""
        # emergencyStop = True
        time.sleep(0.75)

    return app
