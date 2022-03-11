__all__ = ["Orch", "makeOrchServ"]

import asyncio
import sys
from collections import deque
from copy import deepcopy
from math import floor
from typing import Optional, Union, List
from uuid import UUID
from datetime import datetime
from socket import gethostname
from importlib import import_module
from enum import Enum
import inspect
from pydantic import BaseModel
import numpy as np
import json
import io
from pybase64 import b64decode

import aiohttp
import colorama
import time
from fastapi import WebSocket, Body
from functools import partial

from bokeh.server.server import Server
from bokeh.layouts import column
from bokeh.layouts import layout, Spacer
from bokeh.models import ColumnDataSource
from bokeh.models import DataTable, TableColumn
from bokeh.models.widgets import Paragraph
from bokeh.models import Select
from bokeh.models import Button, TextInput
import bokeh.models.widgets as bmw
import bokeh.plotting as bpl
from bokeh.events import ButtonClick, DoubleTap
from bokeh.models.widgets import FileInput

from .base import Base
from .api import HelaoFastAPI
from .vis import Vis
from .make_vis_serv import makeVisServ
from .import_experiments import import_experiments
from .import_sequences import import_sequences
from .dispatcher import async_private_dispatcher, async_action_dispatcher

from ..model.action_start_condition import ActionStartCondition
from ..helper.to_json import to_json
from ..schema import Sequence, Experiment, Action

from ..model.experiment_sequence import ExperimentSequenceModel
from ..model.experiment import (
                                ExperimentModel,
                                ShortExperimentModel,
                                ExperimentTemplate
                               )
from ..model.action import ActionModel
from ..model.active import ActiveParams
from ..model.hlostatus import HloStatus
from ..model.server import ActionServerModel, GlobalStatusModel
from ..model.machine import MachineModel
from ..model.orchstatus import OrchStatus
from ..data.legacy import HTELegacyAPI
from ..error import ErrorCodes


# ANSI color codes converted to the Windows versions
colorama.init(strip=not sys.stdout.isatty())  # strip colors if stdout is redirected
# colorama.init()

hlotags_metadata = [
    {
     "name":"public",
     "description":"prublic orchestrator endpoints"
    },
    {
     "name":"private",
     "description":"private orchestrator endpoints"
    },
    ]

def makeOrchServ(
                 config, 
                 server_key, 
                 server_title, 
                 description, 
                 version, 
                 driver_class=None
                ):

    app = HelaoFastAPI(
                       helao_cfg=config, 
                       helao_srv=server_key, 
                       title=server_title, 
                       description=description, 
                       version=version
                      )

    @app.on_event("startup")
    async def startup_event():
        """Run startup actions.

        When FastAPI server starts, create a global OrchHandler object, 
        initiate the monitor_states coroutine which runs forever, 
        and append dummy experiments to the
        experiment queue for testing.
        """
        app.orch = Orch(app)
        if driver_class:
            app.driver = driver_class(app.orch)

    @app.post("/update_status", tags=["private"])
    async def update_status(actionserver: Optional[ActionServerModel] = \
                            Body({} , embed=True)):
        if actionserver is None:
            return False
        app.orch.print_message(f"orch '{app.orch.server.server_name}' "
                                f"got status from "
                                f"'{actionserver.action_server.server_name}': "
                                f"{actionserver.endpoints}")
        return await app.orch.update_status(actionserver = actionserver)

    @app.post("/attach_client", tags=["private"])
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

    @app.post("/start", tags=["private"])
    async def start():
        """Begin experimenting experiment and action queues."""
        await app.orch.start()
        return {}

    @app.post("/estop", tags=["private"])
    async def estop():
        """Emergency stop experiment and action queues, interrupt running actions."""
        if app.orch.orchstatusmodel.loop_state == OrchStatus.started:
            await app.orch.estop_loop()
        elif app.orch.orchstatusmodel.loop_state == OrchStatus.estop:
            app.orch.print_message("orchestrator E-STOP flag already raised")
        else:
            app.orch.print_message("orchestrator is not running")
        return {}

    @app.post("/stop", tags=["private"])
    async def stop():
        """Stop experimenting experiment and action queues after current actions finish."""
        await app.orch.stop()
        return {}


    @app.post("/clear_estop", tags=["private"])
    async def clear_estop():
        """Remove emergency stop condition."""
        if app.orch.orchstatusmodel.loop_state != OrchStatus.estop:
            app.orch.print_message("orchestrator is not currently in E-STOP")
        else:
            await app.orch.clear_estop()


    @app.post("/clear_error", tags=["private"])
    async def clear_error():
        """Remove error condition."""
        if app.orch.orchstatusmodel.loop_state != OrchStatus.error:
            app.orch.print_message("orchestrator is not currently in ERROR")
        else:
            await app.orch.clear_error()


    @app.post("/skip_experiment", tags=["private"])
    async def skip_experiment():
        """Clear the present action queue while running."""
        await app.orch.skip()
        return {}


    @app.post("/clear_actions", tags=["private"])
    async def clear_actions():
        """Clear the present action queue while stopped."""
        await app.orch.clear_actions()
        return {}


    @app.post("/clear_experiments", tags=["private"])
    async def clear_experiments():
        """Clear the present experiment queue while stopped."""
        await app.orch.clear_experiments()
        return {}


    @app.post("/append_sequence", tags=["private"])
    async def append_sequence(
        sequence: Optional[Sequence] = Body({}, embed=True),
    ):
        await app.orch.add_sequence(sequence = sequence)
        return {}


    @app.post(f"/{server_key}/wait")
    async def wait(
        action: Optional[Action] = \
                Body({}, embed=True),
        waittime: Optional[float] = 0.0
        ):
        """Sleep action"""    
        active = await app.orch.setup_and_contain_action()
        waittime = active.action.action_params["waittime"]
        app.orch.print_message(' ... wait action:', waittime)
        start_time = time.time()
        # last_time = start_time
        while time.time()-start_time < waittime:
            await asyncio.sleep(1)
            app.orch.print_message(f" ... orch waited {(time.time()-start_time):.1f} sec / {waittime:.1f} sec")
        # await asyncio.sleep(waittime)
        app.orch.print_message(' ... wait action done')
        finished_action = await active.finish()
        return finished_action.as_dict()


    # @app.post("/append_experiment")
    # async def append_experiment(
    #     orchestrator: str = None,
    #     experiment_name: str = None,
    #     experiment_params: dict = {},
    #     result_dict: dict = {},
    #     access: str = "hte",
    # ):
    #     """Add a experiment object to the end of the experiment queue.

    # @app.post("/prepend_experiment")
    # async def prepend_experiment(
    # ):
    #     """Add a experiment object to the start of the experiment queue.

    # @app.post("/insert_experiment")
    # async def insert_experiment(
    # ):
    #     """Insert a experiment object at experiment queue index.


    @app.post("/list_experiments", tags=["private"])
    def list_experiments():
        """Return the current list of experiments."""
        return app.orch.list_experiments()

    @app.post("/active_experiment", tags=["private"])
    def active_experiment():
        """Return the active experiment."""
        return app.orch.get_experiment(last=False)

    @app.post("/last_experiment", tags=["private"])
    def last_experiment():
        """Return the last experiment."""
        return app.orch.get_action_group(last=True)

    @app.post("/list_actions", tags=["private"])
    def list_actions():
        """Return the current list of actions."""
        return app.orch.list_actions()

    @app.post("/list_active_actions", tags=["private"])
    def list_active_actions():
        """Return the current list of actions."""
        return app.orch.list_active_actions()

    @app.post("/endpoints", tags=["private"])
    def get_all_urls():
        """Return a list of all endpoints on this server."""
        return app.orch.get_endpoint_urls(app)



    @app.post("/shutdown", tags=["private"])
    def post_shutdown():
        shutdown_event()


    @app.on_event("shutdown")
    def shutdown_event():
        """Run shutdown actions."""
        app.orch.print_message("orch shutdown", info = True)
        # emergencyStop = True
        time.sleep(0.75)

    return app



class Orch(Base):
    """Base class for async orchestrator with trigger support and pushed status update.

    Websockets are not used for critical communications. Orch will attach to all action
    servers listed in a config and maintain a dict of {serverName: status}, which is
    updated by POST requests from action servers. Orch will simultaneously dispatch as
    many action_dq as possible in action queue until it encounters any of the following
    conditions:
      (1) last executed action is final action in queue
      (2) last executed action is blocking
      (3) next action to execute is preempted
      (4) next action is on a busy action server
    which triggers a temporary async task to monitor the action server status dict until
    all conditions are cleared.

    POST requests from action servers are added to a multisubscriber queue and consumed
    by a self-subscriber task to update the action server status dict and log changes.
    """

    def __init__(self, fastapp: HelaoFastAPI):
        super().__init__(fastapp)
        self.experiment_lib = import_experiments(
            world_config_dict=self.world_cfg,
            experiment_path=None,
            server_name=self.server.server_name,
        )
        self.sequence_lib = import_sequences(
            world_config_dict = self.world_cfg,
            sequence_path = None,
            server_name=self.server.server_name
        )

        # instantiate experiment/experiment queue, action queue
        self.sequence_dq = deque([])
        self.experiment_dq = deque([])
        self.action_dq = deque([])
        
        # hold schema objects
        self.active_experiment = None
        self.last_experiment = None
        self.active_sequence = None
        self.last_sequence = None
        self.bokehapp = None
        self.orch_op = None
        self.op_enabled = self.server_params.get("enable_op", False)
        if self.op_enabled:
            # asyncio.gather(self.init_Gamry(self.Gamry_devid))
            self.start_operator()
        # basemodel which holds all information for orch
        self.orchstatusmodel = GlobalStatusModel(orchestrator = self.server)
        # this queue is simply used for waiting for any interrupt
        # but it does not do anything with its content
        self.interrupt_q = asyncio.Queue()

        self.init_success = False  # need to subscribe to all fastapi servers in config

        # pointer to dispatch_loop_task
        self.loop_task = None
        self.status_subscriber = asyncio.create_task(self.subscribe_all())


    def start_operator(self):
        servHost = self.server_cfg["host"]
        servPort = self.server_params.get("bokeh_port",self.server_cfg["port"]+1000)
        servPy = "Operator"

        self.bokehapp = Server(
                          {f"/{servPy}": partial(self.makeBokehApp, orch=self)},
                          port=servPort, 
                          address=servHost, 
                          allow_websocket_origin=[f"{servHost}:{servPort}"]
                          )
        self.print_message(f"started bokeh server {self.bokehapp}",
                           info = True)
        self.bokehapp.start()
        self.bokehapp.io_loop.add_callback(self.bokehapp.show, f"/{servPy}")
        # bokehapp.io_loop.start()


    def makeBokehApp(self, doc, orch):
        app = makeVisServ(
            config = self.world_cfg,
            server_key = self.server.server_name,
            doc = doc,
            server_title = self.server.server_name,
            description = f"{self.technique_name} Operator",
            version=2.0,
            driver_class=None,
        )
    
    
        # _ = Operator(app.vis)
        doc.operator = Operator(app.vis, orch)
        # get the event loop
        # operatorloop = asyncio.get_event_loop()
    
        # this periodically updates the GUI (action and experiment tables)
        # operator.vis.doc.add_periodic_callback(operator.IOloop,2000) # time in ms
    
        return doc


    async def wait_for_interrupt(self):
        """interrupt function which waits for any interrupt
           currently only for status changes
           but this can be extended in the future"""
        
        # we wait for at least one status message
        # and then (if it contains more)
        # empty it and then return
        
        # get at least one status
        _ = await self.interrupt_q.get()
        # if not empty clear it
        while not self.interrupt_q.empty():
            _ = await self.interrupt_q.get()


    async def subscribe_all(self, retry_limit: int = 5):
        """Subscribe to all fastapi servers in config."""
        fails = []
        for serv_key, serv_dict in self.world_cfg["servers"].items():
            if "fast" in serv_dict:
                self.print_message(f"trying to subscribe to "
                                   f"{serv_key} status")

                success = False
                serv_addr = serv_dict["host"]
                serv_port = serv_dict["port"]
                for _ in range(retry_limit):
                    try:
                        response, error_code = await async_private_dispatcher(
                            world_config_dict=self.world_cfg,
                            server=serv_key,
                            private_action="attach_client",
                            params_dict={"client_servkey": self.server.server_name},
                            json_dict={},
                        )
                        if response == True \
                        and error_code == ErrorCodes.none:
                            success = True
                            break
                    except aiohttp.client_exceptions.ClientConnectorError:
                        self.print_message(f"failed to subscribe to "
                                           f"{serv_key} at "
                                           f"{serv_addr}:{serv_port}, "
                                           "trying again in 1sec",
                                           error = True)
                        await asyncio.sleep(1) 
                        

                if success:
                    self.print_message(f"Subscribed to {serv_key} "
                                       f"at {serv_addr}:{serv_port}")
                else:
                    fails.append(serv_key)
                    self.print_message(
                        f"Failed to subscribe to {serv_key} at "
                        f"{serv_addr}:{serv_port}. Check connection."
                    )

        if len(fails) == 0:
            self.init_success = True
        else:
            self.print_message(
                "Orchestrator cannot action experiment_dq unless "
                "all FastAPI servers in config file are accessible."
            )


    async def update_status(
                            self, 
                            actionserver: Optional[ActionServerModel] = None
                           ):
        """Dict update method for action server to push status messages.
        """
        if actionserver is None:
            return False
        # update GlobalStatusModel with new ActionServerModel
        # and sort the new status dict
        self.orchstatusmodel.update_global_with_acts(actionserver=actionserver)

        # check if one action is in estop in the error list:
        estop_uuids = self.orchstatusmodel.find_hlostatus_in_finished(
                                              hlostatus = HloStatus.estopped,
                                             )

        error_uuids = self.orchstatusmodel.find_hlostatus_in_finished(
                                          hlostatus = HloStatus.errored,
                                         )

        if estop_uuids and self.orchstatusmodel.loop_state == OrchStatus.started:
            await self.estop_loop()
        elif error_uuids and self.orchstatusmodel.loop_state == OrchStatus.started:
            self.orchstatusmodel.orch_state = OrchStatus.error
        elif not self.orchstatusmodel.active_dict:
            # no uuids in active action dict
            self.orchstatusmodel.orch_state = OrchStatus.idle
        else:
            self.orchstatusmodel.orch_state = OrchStatus.busy
            self.print_message(f"running_states: "
                               f"{self.orchstatusmodel.active_dict}")

        if self.op_enabled and self.orch_op:
            self.orch_op.vis.doc.add_next_tick_callback(partial(self.orch_op.update_tables))

        # now push it to the interrupt_q
        await self.interrupt_q.put(self.orchstatusmodel)

        return True


    def unpack_sequence(self, sequence_name, sequence_params) -> List[ExperimentTemplate]:
        if sequence_name in self.sequence_lib:
            return self.sequence_lib[sequence_name](**sequence_params)
        else:
            return []


    async def dispatch_loop_task(self):
        """Parse experiment and action queues, 
           and dispatch action_dq while tracking run state flags."""
        self.print_message("--- started operator orch ---")
        self.print_message(f"current orch status: {self.orchstatusmodel.orch_state}")
        # clause for resuming paused action list
        self.print_message(f"current orch sequences: {self.sequence_dq}")
        self.print_message(f"current orch descisions: {self.experiment_dq}")
        self.print_message(f"current orch actions: {self.action_dq}")
        self.print_message("--- resuming orch loop now ---")
        
        try:
            if self.sequence_dq:
                self.print_message("getting new sequence from sequence_dq")
                self.active_sequence = self.sequence_dq.popleft()
                self.active_sequence.init_seq(time_offset = self.ntp_offset)
    
                # todo: this is for later, for now the operator needs to unpack the sequence
                # in order to also use a semi manual op mode
                
                # self.print_message(f"unpacking experiments for {self.active_sequence.sequence_name}")
                # if self.active_sequence.sequence_name in self.sequence_lib:
                #     unpacked_prcs = self.sequence_lib[self.active_sequence.sequence_name](**self.active_sequence.sequence_params)
                # else:
                #     unpacked_prcs = []
    
                # for prc in unpacked_prcs:
                #     D = Experiment(**prc.as_dict())
                #     self.active_sequence.experiment_plan_list.append(D)
    
    
                self.seq_file = self.active_sequence.get_seq()
                await self.write_seq(self.active_sequence)
    
                # add all experiments from sequence to experiment queue
                # todo: use seq model instead to initialize some parameters
                # of the experiment
                for experimenttemplate in self.active_sequence.experiment_plan_list:
                        self.print_message(f"unpack experiment "
                                           f"{experimenttemplate.experiment_name}")
                        await self.add_experiment(
                            seq = self.active_sequence.get_seq(),
                            experimenttemplate = experimenttemplate,
                            )


                self.orchstatusmodel.loop_state = OrchStatus.started

            else:
                self.print_message("sequence queue is empty, "
                                   "cannot start orch loop")


            while self.orchstatusmodel.loop_state == OrchStatus.started and (self.action_dq or self.experiment_dq):
                self.print_message(f"current content of action_dq: {self.action_dq}")
                self.print_message(f"current content of experiment_dq: {self.experiment_dq}")
                await asyncio.sleep(
                    0.001
                )  
                
                # if status queue is empty get first new actions from new experiment
                if not self.action_dq:
                    self.print_message("action_dq is empty, getting new actions")
                    # wait for all actions in last/active experiment to finish 
                    self.print_message("finishing last active experiment first")
                    await self.finish_active_experiment()

                    self.print_message("getting new experiment to fill action_dq")
                    # generate uids when populating, 
                    # generate timestamp when acquring
                    self.active_experiment = self.experiment_dq.popleft()
                    self.print_message(f"new active experiment is {self.active_experiment.experiment_name}")

                    self.active_experiment.technique_name = self.technique_name
                    self.active_experiment.orchestrator = self.server
                    self.active_experiment.init_prc(time_offset=self.ntp_offset)

                    self.orchstatusmodel.new_experiment(
                        exp_uuid = self.active_experiment.experiment_uuid)


                    # additional experiment params should be stored 
                    # in experiment.experiment_params
                    self.print_message(f"unpacking actions for {self.active_experiment.experiment_name}")
                    unpacked_acts = self.experiment_lib[
                        self.active_experiment.experiment_name
                                                    ](self.active_experiment)
                    if unpacked_acts is None:
                        self.print_message(
                            "no actions in experiment",
                            error = True
                        )
                        self.action_dq = deque([])
                        continue

                    self.print_message("setting action order")
                    for i, act in enumerate(unpacked_acts):
                        act.action_order = int(i)
                        # actual order should be the same at the beginning
                        # will be incremented as necessary
                        act.orch_submit_order = int(i)

                    # TODO:update experiment code
                    self.print_message("adding unpacked actions to action_dq")
                    self.action_dq = deque(unpacked_acts)
                    self.print_message(f"got: {self.action_dq}")
                    self.print_message(f"optional params: {self.active_experiment.experiment_params}")


                    # write a temporary prc
                    await self.write_active_experiment_prc()

                else:
                    self.print_message("actions in action_dq, processing them")
                    if self.orchstatusmodel.loop_intent == OrchStatus.stop:
                        self.print_message("stopping orchestrator")
                        # monitor status of running action_dq, then end loop
                        while self.orchstatusmodel.loop_state != OrchStatus.stopped:
                            # wait for all orch actions to finish first
                            await self.orch_wait_for_all_actions()
                            if self.orchstatusmodel.orch_state == OrchStatus.idle:
                                await self.intend_none()
                                self.print_message("got stop")
                                self.orchstatusmodel.loop_state = OrchStatus.stopped
                                break

                    elif self.orchstatusmodel.loop_intent == OrchStatus.skip:
                        # clear action queue, forcing next experiment
                        self.action_dq.clear()
                        await self.intend_none()
                        self.print_message("skipping to next experiment")
                    else:
                        # all action blocking is handled like preempt, 
                        # check Action requirements
                        A = self.action_dq.popleft()
                        # append previous results to current action
                        A.result_dict = self.active_experiment.result_dict

                        # see async_action_dispatcher for unpacking

                        if A.start_condition == ActionStartCondition.no_wait:
                            self.print_message("orch is dispatching an unconditional action")
                        else:
                            if A.start_condition == ActionStartCondition.wait_for_endpoint:
                                self.print_message(
                                    "orch is waiting for endpoint to become available"
                                )
                                while True:
                                    await self.wait_for_interrupt()
                                    endpoint_free = \
                                        self.orchstatusmodel.endpoint_free(
                                            action_server = A.action_server,
                                            endpoint_name = A.action_name
                                        )
                                    if endpoint_free:
                                        break
                            elif A.start_condition == ActionStartCondition.wait_for_server:
                                self.print_message("orch is waiting for "
                                                   "server to become available")
                                while True:
                                    await self.wait_for_interrupt()
                                    server_free = \
                                    self.orchstatusmodel.server_free(
                                                action_server = A.action_server
                                    )

                                    if server_free:
                                        break
                            elif A.start_condition == ActionStartCondition.wait_for_all:
                                await self.orch_wait_for_all_actions()

                            else:  #unsupported value
                                await self.orch_wait_for_all_actions()

                        self.print_message("copying global vars to action")

                        # copy requested global param to action params
                        for k, v in A.from_global_params.items():
                            self.print_message(f"{k}:{v}")
                            if k in self.active_experiment.global_params:
                                A.action_params.update({v: self.active_experiment.global_params[k]})

                        self.print_message(
                            f"dispatching action {A.action_name} "
                            f"on server {A.action_server.server_name}"
                        )
                        # keep running counter of dispatched actions
                        A.orch_submit_order = \
                            self.orchstatusmodel.counter_dispatched_actions[self.active_experiment.experiment_uuid]
                        self.orchstatusmodel.counter_dispatched_actions[self.active_experiment.experiment_uuid] +=1

                        A.init_act(time_offset = self.ntp_offset)
                        result, error_code = await async_action_dispatcher(self.world_cfg, A)

                        self.print_message("copying global vars "
                                           "back to experiment")
                        # self.print_message(result)
                        if "to_global_params" in result:
                            for k in result["to_global_params"]:
                                if k in result["action_params"]:
                                    if (
                                        result["action_params"][k] is None
                                        and k in self.active_experiment.global_params
                                    ):
                                        self.active_experiment.global_params.pop(k)
                                    else:
                                        self.active_experiment.global_params.update(
                                            {k: result["action_params"][k]}
                                        )
                        self.print_message("done copying global "
                                           "vars back to experiment")

            self.print_message("experiment queue is empty")
            self.print_message("--- stopping operator orch ---", info = True)

            # finish the last prc
            # this wait for all actions in active experiment
            # to finish and then updates the prc with the acts
            self.print_message("finishing final experiment")
            await self.finish_active_experiment()
            self.print_message("finishing final sequence")
            await self.finish_active_sequence()


            self.orchstatusmodel.loop_state = OrchStatus.stopped
            await self.intend_none()
            return True
        # except asyncio.CancelledError:
        #     self.print_message("serious orch exception occurred",error = True)
        #     return False
        except Exception as e:
            self.print_message("serious orch exception occurred", error=True)
            self.print_message(f"ERROR: {e}", error=True)
            return False


    async def orch_wait_for_all_actions(self):
        """waits for all action assigned to this orch to finish"""
    
        self.print_message("orch is waiting for all action_dq to finish")
        
        if not self.orchstatusmodel.actions_idle():
            # some actions are active
            # we need to wait for them to finish
            while True:
                self.print_message("some actions are still active, "
                                   "waiting for status update")
                # we check again once the active action
                # updates its status again
                await self.wait_for_interrupt()
                self.print_message("got status update")
                # we got a status update
                # check if all actions are idle now
                if self.orchstatusmodel.actions_idle():
                    self.print_message("all actions are idle now")
                    break
        else:
            self.print_message("all actions are idle")


    async def start(self):
        """Begin experimenting experiment and action queues."""
        if self.orchstatusmodel.loop_state == OrchStatus.stopped:
            if self.action_dq \
            or self.experiment_dq \
            or self.sequence_dq:  # resume actions from a paused run
                await self.start_loop()
            else:
                self.print_message("experiment list is empty")
        else:
            self.print_message("already running")


    async def start_loop(self):
        if self.orchstatusmodel.loop_state == OrchStatus.stopped:
            self.print_message("starting orch loop")
            self.loop_task = asyncio.create_task(self.dispatch_loop_task())
        elif self.orchstatusmodel.loop_state == OrchStatus.estop:
            self.print_message("E-STOP flag was raised, "
                               "clear E-STOP before starting.")
        else:
            self.print_message("loop already started.")
        return self.orchstatusmodel.loop_state


    async def estop_loop(self):
        # set orchstatusmodel.loop_state to estop
        self.orchstatusmodel.loop_state = OrchStatus.estop
        # cancels current dispatch loop task
        self.loop_task.cancel()
        # force stop all running actions in the status dict (for this orch)
        # TODO
        await self.estop_actions(switch = True)
        # reset loop intend
        await self.intend_none()


    async def stop_loop(self):
        await self.intend_stop()


    async def estop_actions(self, switch: bool):
        # create a dict for current active_experiment
        # (estop happens during the active_experiment)

        if self.active_experiment is not None:
            active_exp_dict = self.active_experiment.as_dict()
        elif self.last_experiment is not None:
            active_exp_dict = self.last_experiment.as_dict()
        else:
            exp = Experiment()
            exp.sequence_name = "orch_estop"
            # need to set status, else init will set in to active
            exp.sequence_status = [HloStatus.estopped, HloStatus.finished]
            exp.init_seq(time_offset=self.base.ntp_offset)

            exp.technique_name = self.technique_name
            exp.orchestrator = self.server
            exp.experiment_status = [HloStatus.estopped, HloStatus.finished]
            exp.init_prc(time_offset=self.base.ntp_offset)
            active_exp_dict = exp.as_dict()


        for action_server, actionservermodel \
        in self.server_dict.items():
            action_dict = deepcopy(active_exp_dict)
            action_dict.update({
                "action_name":"estop",
                "action_server":actionservermodel.action_server.dict(),
                "action_params":{"switch":switch},
                "start_condition":ActionStartCondition.no_wait
                })

            A = Action(**action_dict)
            try:
                self.print_message(f"Sending estop={switch} request to "
                                   f"{actionservermodel.action_server.disp_name()}",
                                   info = True)
                _ = await async_action_dispatcher(self.world_cfg, A)
            except Exception as e:
                pass
                # no estop endpoint for this action server?
                self.print_message(f"estop for "
                                   f"{actionservermodel.action_server.disp_name()} "
                                   f"failed with: {e}", error = True)


    async def skip(self):
        """Clear the present action queue while running."""
        if self.orchstatusmodel.loop_state == OrchStatus.started:
            await self.intend_skip()
        else:
            self.print_message("orchestrator not running, "
                               "clearing action queue")
            await asyncio.sleep(0.001)
            self.action_dq.clear()        


    async def intend_skip(self):
        await asyncio.sleep(0.001)
        self.orchstatusmodel.loop_intent = OrchStatus.skip
        await self.interrupt_q.put(self.orchstatusmodel.loop_intent)


    async def stop(self):
        """Stop experimenting experiment and 
           action queues after current actions finish."""
        if self.orchstatusmodel.loop_state == OrchStatus.started:
            await self.intend_stop()
        elif self.orchstatusmodel.loop_state == OrchStatus.estop:
            self.print_message("orchestrator E-STOP flag was raised; "
                               "nothing to stop")
        else:
            self.print_message("orchestrator is not running")
        

    async def intend_stop(self):
        await asyncio.sleep(0.001)
        self.orchstatusmodel.loop_intent = OrchStatus.stop
        await self.interrupt_q.put(self.orchstatusmodel.loop_intent)


    async def intend_none(self):
        await asyncio.sleep(0.001)
        self.orchstatusmodel.loop_intent = OrchStatus.none
        await self.interrupt_q.put(self.orchstatusmodel.loop_intent)


    async def clear_estop(self):
        # which were estopped first
        await asyncio.sleep(0.001)
        self.print_message("clearing estopped uuids")
        self.orchstatusmodel.clear_in_finished(hlostatus = HloStatus.estopped)
        # release estop for all action servers
        await self.estop_actions(switch = False)
        # set orch status from estop back to stopped
        self.orchstatusmodel.loop_state = OrchStatus.stopped
        await self.interrupt_q.put("cleared_estop")


    async def clear_error(self):
        # currently only resets the error dict
        self.print_message("clearing errored uuids")
        await asyncio.sleep(0.001)
        self.orchstatusmodel.clear_in_finished(hlostatus = HloStatus.errored)
        await self.interrupt_q.put("cleared_errored")


    async def clear_experiments(self):
        self.print_message("clearing experiment queue")
        await asyncio.sleep(0.001)
        self.experiment_dq.clear()


    async def clear_actions(self):
        self.print_message("clearing action queue")
        await asyncio.sleep(0.001)
        self.action_dq.clear()


    async def add_sequence(
                           self,
                           sequence: Sequence,
                          ):
        # for D_dict in sequence.experiment_plan_list:
        #     D = Experiment(D_dict)
        #     seq.experiment_plan_list.append(D)
        self.sequence_dq.append(sequence)


    async def add_experiment(
        self,
        seq: ExperimentSequenceModel,
        experimenttemplate: ExperimentTemplate,
        # orchestrator: str = None,
        # experiment_name: str = None,
        # experiment_params: dict = {},
        # result_dict: dict = {},
        # access: str = "hte",
        prepend: Optional[bool] = False,
        at_index: Optional[int] = None,
    ):
        Ddict = experimenttemplate.dict()
        Ddict.update(seq.dict())
        D = Experiment(**Ddict)

        # reminder: experiment_dict values take precedence over keyword args
        if D.orchestrator.server_name is None \
        or D.orchestrator.machine_name is None:
            D.orchestrator = self.server

        await asyncio.sleep(0.001)
        if at_index:
            self.experiment_dq.insert(i=at_index, x=D)
        elif prepend:
            self.experiment_dq.appendleft(D)
            self.print_message(f"experiment {D.experiment_name} "
                               "prepended to queue")
        else:
            self.experiment_dq.append(D)
            self.print_message(f"experiment {D.experiment_name} "
                               "appended to queue")


    def list_experiments(self):
        """Return the current queue of experiment_dq."""
        return [experiment.get_prc() for experiment in self.experiment_dq]


    def get_experiment(self, last=False):
        """Return the active or last experiment."""
        active_experiment_list = []
        if last:
            experiment = self.last_experiment
        else:
            experiment = self.active_experiment
        if experiment is not None:
            active_experiment_list.append(experiment.get_prc())
        return active_experiment_list


    def list_active_actions(self):
        """Return the current queue running actions."""
        action_list = []
        index = 0
        for uuid, statusmodel in self.orchstatusmodel.active_dict.items():
            action_list.append(
                {"index":index,
                 "action_uuid":f"{statusmodel.act.action_uuid}",
                 "server":statusmodel.act.action_server.disp_name(),
                 "action_name":statusmodel.act.action_name,
                })
            index = index+1
        return action_list


    def list_actions(self):
        """Return the current queue of action_dq."""
        return [action.get_act() for action in self.action_dq]


    def supplement_error_action(self, check_uuid: UUID, sup_action: Action):
        """Insert action at front of action queue with 
           subversion of errored action, 
           inherit parameters if desired."""

        error_uuids = self.orchstatusmodel.find_hlostatus_in_finished(
                                          hlostatus = HloStatus.errored,
                                         )
        if not error_uuids:
            self.print_message("There are no error statuses to replace")
        else:
            if check_uuid in error_uuids:
                EA_act = error_uuids[check_uuid]
                # sup_action can be a differnt one, 
                # but for now we treat it thats a retry of the errored one
                new_action = sup_action
                new_action.action_order = EA_act.action_order
                 # will be updated again once its dispatched again
                new_action.actual_order = EA_act.actual_order
                new_action.action_retry = EA_act.action_retry + 1
                self.action_dq.appendleft(new_action)
            else:
                self.print_message(f"uuid {check_uuid} not found "
                                   "in list of error statuses:")
                self.print_message(", ".join(self.error_uuids))


    def remove_experiment(
                          self, 
                          by_index: Optional[int] = None, 
                          by_uuid: Optional[UUID] = None
                         ):
        """Remove experiment in list by enumeration index or uuid."""
        if by_index:
            i = by_index
        elif by_uuid:
            i = [i for i, D in enumerate(list(self.experiment_dq)) if D.experiment_uuid == by_uuid][0]
        else:
            self.print_message("No arguments given for "
                               "locating existing experiment to remove.")
            return None
        del self.experiment_dq[i]


    def replace_action(
        self,
        sup_action: Action,
        by_index: Optional[int] = None,
        by_uuid: Optional[UUID] = None,
        by_action_order: Optional[int] = None,
    ):
        """Substitute a queued action."""
        if by_index:
            i = by_index
        elif by_uuid:
            i = [i for i, A in enumerate(list(self.action_dq)) if A.action_uuid == by_uuid][0]
        elif by_action_order:
            i = [i for i, A in enumerate(list(self.action_dq)) if A.action_order == by_action_order][0]
        else:
            self.print_message("No arguments given for "
                               "locating existing action to replace.")
            return None
        # get action_order of selected action which gets replaced
        current_action_order = self.action_dq[i].action_order
        new_action = sup_action
        new_action.action_order = current_action_order
        self.action_dq.insert(i, new_action)
        del self.action_dq[i + 1]


    def append_action(self, sup_action: Action):
        """Add action to end of current action queue."""
        if len(self.action_dq) == 0:
            last_action_order = self.orchstatusmodel.counter_dispatched_actions[self.active_experiment.experiment_uuid] - 1
            if last_action_order < 0:
                # no action was dispatched yet
                last_action_order = 0
        else:
            last_action_order = self.action_dq[-1].action_order

        new_action_order = last_action_order + 1
        new_action = sup_action
        new_action.action_order = new_action_order
        self.action_dq.append(new_action)

    
    async def finish_active_sequence(self):
        if self.active_sequence is not None:
            self.replace_status(
                       status_list = self.active_sequence.sequence_status,
                       old_status = HloStatus.active,
                       new_status = HloStatus.finished
                      )
            await self.write_seq(self.active_sequence)
            self.last_sequence = deepcopy(self.active_sequence)
            self.active_sequence = None


    async def finish_active_experiment(self):
        # we need to wait for all actions to finish first
        await self.orch_wait_for_all_actions()
        if self.active_experiment is not None:
            self.print_message(f"finished prc uuid is: "
                               f"{self.active_experiment.experiment_uuid}, "
                               f"adding matching acts to it")

            self.active_experiment.experiment_action_list = []
            
            # TODO use exp uuid to filter actions?
            self.active_experiment.experiment_action_list = self.orchstatusmodel.finish_experiment(exp_uuid = self.active_experiment.experiment_uuid)
            # set exp status to finished
            self.replace_status(
                       status_list = self.active_experiment.experiment_status,
                       old_status = HloStatus.active,
                       new_status = HloStatus.finished
                      )
            
            # add finished prc to seq
            # !!! add to experimentmodel_list
            # not to experiment_list !!!!
            self.active_sequence.experimentmodel_list.append(
                deepcopy(self.active_experiment.get_prc())
            )

            # write new updated seq
            await self.write_active_sequence_seq()

            # write final prc
            await self.write_prc(self.active_experiment)


        self.last_experiment = deepcopy(self.active_experiment)
        self.active_experiment = None


    async def write_active_experiment_prc(self):
        await self.write_prc(self.active_experiment)


    async def write_active_sequence_seq(self):
        await self.write_seq(self.active_sequence)


    async def shutdown(self):
        await self.detach_subscribers()
        self.status_logger.cancel()
        self.ntp_syncer.cancel()
        self.status_subscriber.cancel()



class return_sequence_lib(BaseModel):
    """Return class for queried sequence objects."""
    index: int
    sequence_name: str
    doc: str
    args: list
    defaults: list


class return_experiment_lib(BaseModel):
    """Return class for queried experiment objects."""
    index: int
    experiment_name: str
    doc: str
    args: list
    defaults: list


class Operator:
    def __init__(self, visServ: Vis, orch):
        self.vis = visServ
        self.orch = orch
        self.dataAPI = HTELegacyAPI(self.vis)

        self.config_dict = self.vis.server_cfg.get("params", dict())
        self.pal_name = None
        # find pal server if configured in world config
        for server_name, server_config \
        in self.vis.world_cfg["servers"].items():
            if server_config.get("fast", "") == "pal_server":
                self.pal_name = server_name
                self.vis.print_message(f"found PAL server: '{self.pal_name}'", 
                                       info = True)
                break

        self.dev_customitems = []
        if self.pal_name is not None:
            pal_server_params = self.vis.world_cfg["servers"][self.pal_name]["params"]
            if "positions" in pal_server_params:
                dev_custom = pal_server_params["positions"].get("custom",dict())
            else:
                dev_custom = dict()
            self.dev_customitems = [key for key in dev_custom.keys()]

        self.color_sq_param_inputs = "#BDB76B"

        # holds the page layout
        self.layout = []
        self.seq_param_layout = []
        self.seq_param_input = []
        self.seq_private_input = []
        self.prc_param_layout = []
        self.prc_param_input = []
        self.prc_private_input = []

        self.sequence = None
        self.experiment_plan_list = dict()
        self.experiment_list = dict()
        self.action_list = dict()
        self.active_action_list = dict()

        self.sequence_select_list = []
        self.sequences = []
        self.sequence_lib = self.orch.sequence_lib

        self.experiment_select_list = []
        self.experiments = []
        self.experiment_lib = self.orch.experiment_lib

        # FastAPI calls
        self.get_sequence_lib()
        self.get_experiment_lib()



        self.vis.doc.add_next_tick_callback(partial(self.get_experiments))
        self.vis.doc.add_next_tick_callback(partial(self.get_actions))
        self.vis.doc.add_next_tick_callback(partial(self.get_active_actions))

        self.sequence_source = ColumnDataSource(data=self.experiment_plan_list)
        self.columns_seq = [TableColumn(field=key, title=key) for key in self.experiment_plan_list]
        self.experimentplan_table = DataTable(
                                        source=self.sequence_source, 
                                        columns=self.columns_seq, 
                                        width=620, 
                                        height=200,
                                        autosize_mode = "fit_columns"
                                        )

        self.experiment_source = ColumnDataSource(data=self.experiment_list)
        self.columns_prc = [TableColumn(field=key, title=key) for key in self.experiment_list]
        self.experiment_table = DataTable(
                                       source=self.experiment_source, 
                                       columns=self.columns_prc, 
                                       width=620, 
                                       height=200,
                                       autosize_mode = "fit_columns"
                                      )

        self.action_source = ColumnDataSource(data=self.action_list)
        self.columns_act = [TableColumn(field=key, title=key) for key in self.action_list]
        self.action_table = DataTable(
                                      source=self.action_source, 
                                      columns=self.columns_act, 
                                      width=620, 
                                      height=200,
                                      autosize_mode = "fit_columns"
                                     )

        self.active_action_source = ColumnDataSource(data=self.active_action_list)
        self.columns_active_action = [TableColumn(field=key, title=key) for key in self.active_action_list]
        self.active_action_table = DataTable(
                                             source=self.active_action_source, 
                                             columns=self.columns_active_action, 
                                             width=620, 
                                             height=200,
                                             autosize_mode = "fit_columns"
                                            )

        self.sequence_dropdown = Select(
                                        title="Select sequence:",
                                        value = None,
                                        options=self.sequence_select_list,
                                       )
        self.sequence_dropdown.on_change("value", self.callback_sequence_select)

        self.experiment_dropdown = Select(
                                       title="Select experiment:",
                                       value = None,
                                       options=self.experiment_select_list
                                      )
        self.experiment_dropdown.on_change("value", self.callback_experiment_select)


        # buttons to control orch
        self.button_start = Button(label="Start Orch", button_type="default", width=70)
        self.button_start.on_event(ButtonClick, self.callback_start)
        self.button_stop = Button(label="Stop Orch", button_type="default", width=70)
        self.button_stop.on_event(ButtonClick, self.callback_stop)
        self.button_skip = Button(label="Skip prc", button_type="danger", width=70)
        self.button_skip.on_event(ButtonClick, self.callback_skip_dec)
        self.button_update = Button(label="update tables", button_type="default", width=120)
        self.button_update.on_event(ButtonClick, self.callback_update_tables)
        self.button_clear_seqg = Button(label="clear seqg", button_type="default", width=70)
        self.button_clear_seqg.on_event(ButtonClick, self.callback_clear_seqg)

        self.button_clear_prg = Button(label="clear prc", button_type="danger", width=100)
        self.button_clear_prg.on_event(ButtonClick, self.callback_clear_experiments)
        self.button_clear_action = Button(label="clear act", button_type="danger", width=100)
        self.button_clear_action.on_event(ButtonClick, self.callback_clear_actions)

        self.button_prepend_prc = Button(label="prepend prc", button_type="default", width=150)
        self.button_prepend_prc.on_event(ButtonClick, self.callback_prepend_prc)
        self.button_append_prc = Button(label="append prc", button_type="default", width=150)
        self.button_append_prc.on_event(ButtonClick, self.callback_append_prc)

        self.button_prepend_seqg = Button(label="prepend seqg", button_type="default", width=150)
        self.button_prepend_seqg.on_event(ButtonClick, self.callback_prepend_seqg)
        self.button_append_seqg = Button(label="append seqg", button_type="default", width=150)
        self.button_append_seqg.on_event(ButtonClick, self.callback_append_seqg)


        self.sequence_descr_txt = bmw.Div(text="""select a sequence item""", width=600)
        self.experiment_descr_txt = bmw.Div(text="""select a experiment item""", width=600)
        self.error_txt = Paragraph(text="""no error""", width=600, height=30, style={"font-size": "100%", "color": "black"})

        self.input_sequence_label = TextInput(value="nolabel", title="sequence label", disabled=False, width=120, height=40)

        self.layout0 = layout([
            layout(
                [Spacer(width=20), bmw.Div(text=f"<b>{self.config_dict.get('doc_name', 'Operator')} on {gethostname()}</b>", width=620, height=32, style={"font-size": "200%", "color": "red"})],
                background="#C0C0C0",width=640),
            Spacer(height=10),
            layout(
                [Spacer(width=20), bmw.Div(text="<b>Sequences:</b>", width=620, height=32, style={"font-size": "150%", "color": "red"})],
                background="#C0C0C0",width=640),
            layout([
                [self.sequence_dropdown],
                [Spacer(width=10), bmw.Div(text="<b>sequence description:</b>", width=200+50, height=15)],
                [self.sequence_descr_txt],
                Spacer(height=10),
                ],background="#808080",width=640),
            layout([
                [self.button_append_seqg, self.button_prepend_seqg],
                ],background="#808080",width=640)
            ])

        self.layout2 = layout([
            Spacer(height=10),
            layout(
                [Spacer(width=20), bmw.Div(text="<b>Experiments:</b>", width=620, height=32, style={"font-size": "150%", "color": "red"})],
                background="#C0C0C0",width=640),
            Spacer(height=10),
            layout([
                [self.experiment_dropdown],
                [Spacer(width=10), bmw.Div(text="<b>experiment description:</b>", width=200+50, height=15)],
                [self.experiment_descr_txt],
                Spacer(height=20),
                ],background="#808080",width=640),
                layout([
                    [self.button_append_prc, self.button_prepend_prc],
                ],background="#808080",width=640)
            ])


        self.layout4 = layout([
                Spacer(height=10),
                layout(
                    [Spacer(width=20), bmw.Div(text="<b>Orch:</b>", width=620, height=32, style={"font-size": "150%", "color": "red"})],
                    background="#C0C0C0",width=640),
                layout([
                    [self.input_sequence_label, self.button_start, Spacer(width=10), self.button_stop,  Spacer(width=10), self.button_clear_seqg],
                    Spacer(height=10),
                    [Spacer(width=10), bmw.Div(text="<b>Error message:</b>", width=200+50, height=15, style={"font-size": "100%", "color": "black"})],
                    [Spacer(width=10), self.error_txt],
                    Spacer(height=10),
                    ],background="#808080",width=640),
                layout([
                [Spacer(width=20), bmw.Div(text="<b>Experiment Plan:</b>", width=200+50, height=15)],
                [self.experimentplan_table],
                [Spacer(width=20), bmw.Div(text="<b>queued experiments:</b>", width=200+50, height=15)],
                [self.experiment_table],
                [Spacer(width=20), bmw.Div(text="<b>queued actions:</b>", width=200+50, height=15)],
                [self.action_table],
                [Spacer(width=20), bmw.Div(text="<b>Active actions:</b>", width=200+50, height=15)],
                [self.active_action_table],
                Spacer(height=10),
                [self.button_skip, Spacer(width=5), self.button_clear_prg, Spacer(width=5), self.button_clear_action, self.button_update],
                Spacer(height=10),
                ],background="#7fdbff",width=640),
            ])


        self.dynamic_col = column(
                                  self.layout0, 
                                  layout(), # placeholder
                                  self.layout2, 
                                  layout(),  # placeholder
                                  self.layout4
                                  )
        self.vis.doc.add_root(self.dynamic_col)


        # select the first item to force an update of the layout
        if self.experiment_select_list:
            self.experiment_dropdown.value = self.experiment_select_list[0]

        if self.sequence_select_list:
            self.sequence_dropdown.value = self.sequence_select_list[0]

        self.orch.orch_op = self

    def get_sequence_lib(self):
        """Return the current list of sequences."""
        self.sequences = []
        self.vis.print_message(f"found sequences: "
                               f"{[sequence for sequence in self.sequence_lib]}")
        for i, sequence in enumerate(self.sequence_lib):
            tmpdoc = self.sequence_lib[sequence].__doc__ 
            if tmpdoc == None:
                tmpdoc = ""

            argspec = inspect.getfullargspec(self.sequence_lib[sequence])
            tmpargs = argspec.args
            tmpdef = argspec.defaults

            if tmpdef == None:
                tmpdef = []
            
            for t in tmpdef:
                t = json.dumps(t)
            
            self.sequences.append(return_sequence_lib(
                index=i,
                sequence_name = sequence,
                doc = tmpdoc,
                args = tmpargs,
                defaults = tmpdef,
                ).dict()
            )
        for item in self.sequences:
            self.sequence_select_list.append(item["sequence_name"])


    def get_experiment_lib(self):
        """Return the current list of experiments."""
        self.experiments = []
        self.vis.print_message(f"found experiment: {[experiment for experiment in self.experiment_lib]}")
        for i, experiment in enumerate(self.experiment_lib):
            tmpdoc = self.experiment_lib[experiment].__doc__ 
            if tmpdoc == None:
                tmpdoc = ""

            argspec = inspect.getfullargspec(self.experiment_lib[experiment])
            tmpargs = argspec.args
            tmpdefs = argspec.defaults
            if tmpdefs == None:
                tmpdefs = []

            # filter the Experiment BaseModel
            idxlist = []
            for idx, tmparg in enumerate(argspec.args):
                if argspec.annotations.get(tmparg, None) == Experiment:
                    idxlist.append(idx)

            tmpargs = list(tmpargs)
            tmpdefs = list(tmpdefs)
            for i, idx in enumerate(idxlist):
                if len(tmpargs) == len(tmpdefs):
                    tmpargs.pop(idx-i)
                    tmpdefs.pop(idx-i)
                else:
                    tmpargs.pop(idx-i)
            tmpargs = tuple(tmpargs)
            tmpdefs = tuple(tmpdefs)
                

            for t in tmpdefs:
                t = json.dumps(t)
            
            self.experiments.append(return_experiment_lib(
                index=i,
                experiment_name = experiment,
                doc = tmpdoc,
                args = tmpargs,
                defaults = tmpdefs,
               ).dict()
            )
        for item in self.experiments:
            self.experiment_select_list.append(item["experiment_name"])


    async def get_experiments(self):
        """get experiment list from orch"""
        experiments = self.orch.list_experiments()
        self.experiment_list = dict()
        if experiments:
            for key,val in experiments[0].json_dict().items():
                if val is not None:
                    self.experiment_list[key] = []
            for exp in experiments:
                for key, value in exp.json_dict().items():
                    if key in self.experiment_list:
                        self.experiment_list[key].append(value)
        self.vis.print_message(f"current queued experiments: {self.experiment_list}")


    async def get_actions(self):
        """get action list from orch"""
        actions = self.orch.list_actions()
        self.action_list = dict()
        if actions:
            for key,val in actions[0].json_dict().items():
                if val is not None:
                    self.action_list[key] = []
            for act in actions:
                for key, value in act.json_dict().items():
                    if key in self.action_list:
                        self.action_list[key].append(value)
        self.vis.print_message(f"current queued actions: {self.action_list}")


    async def get_active_actions(self):
        """get action list from orch"""
        actions = self.orch.list_active_actions()
        self.active_action_list = dict()
        if actions:
            for key in actions[0]:
                self.active_action_list[key] = []
            for act in actions:
                for key, value in act.items():
                    self.active_action_list[key].append(value)
        self.vis.print_message(f"current active actions: {self.active_action_list}")


    def callback_sequence_select(self, attr, old, new):
        idx = self.sequence_select_list.index(new)
        self.update_seq_param_layout(idx)
        self.vis.doc.add_next_tick_callback(
            partial(self.update_seq_doc,self.sequences[idx]["doc"])
        )


    def callback_experiment_select(self, attr, old, new):
        idx = self.experiment_select_list.index(new)
        self.update_prc_param_layout(idx)
        self.vis.doc.add_next_tick_callback(
            partial(self.update_prc_doc,self.experiments[idx]["doc"])
        )


    def callback_clicked_pmplot(self, event, sender):
        """double click/tap on PM plot to add/move marker"""
        self.vis.print_message(f"DOUBLE TAP PMplot: {event.x}, {event.y}")
        # get coordinates of doubleclick
        platex = event.x
        platey = event.y
        # transform to nearest sample point
        PMnum = self.get_samples([platex], [platey], sender)
        self.get_sample_infos(PMnum, sender)


    def callback_changed_plateid(self, attr, old, new, sender):
        """callback for plateid text input"""
        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return None

        plateid = to_int(new)
        if plateid is not None:
            self.get_pm(new, sender)
            self.get_elements_plateid(new, sender)

            private_input, param_input = self.find_param_private_input(sender)
            if private_input is None or param_input is None:
                return

            # after selecting a new plate, we reset the sample_no
            input_sample_no = self.find_input(param_input, "solid_sample_no")
            if input_sample_no is not None:
                self.vis.doc.add_next_tick_callback(partial(
                                                            self.callback_changed_sampleno,
                                                            attr = "value",
                                                            old = input_sample_no.value,
                                                            new = "1",
                                                            sender=input_sample_no
                                                           ))

        else:
            self.vis.doc.add_next_tick_callback(partial(self.update_input_value,sender,""))


    def callback_plate_sample_no_list_file(self, attr, old, new, sender, inputfield):
        f = io.BytesIO(b64decode(sender.value))
        sample_nos = json.dumps(np.loadtxt(f).astype(int).tolist())
        self.vis.doc.add_next_tick_callback(partial(self.update_input_value,inputfield,sample_nos))


    def callback_changed_sampleno(self, attr, old, new, sender):
        """callback for sampleno text input"""
        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return None

        sample_no = to_int(new)
        if sample_no is not None:
            self.get_sample_infos([sample_no-1], sender)
        else:
            self.vis.doc.add_next_tick_callback(partial(self.update_input_value,sender,""))


    def callback_start(self, event):
        if self.sequence is not None:
            sellabel = self.input_sequence_label.value
            self.sequence.sequence_label = sellabel
            self.vis.print_message("starting orch")
            self.vis.doc.add_next_tick_callback(partial(self.orch.add_sequence,self.sequence))
            self.vis.doc.add_next_tick_callback(partial(self.orch.start))
            self.vis.doc.add_next_tick_callback(partial(self.update_tables))
        else:
            self.vis.print_message("Cannot start orch. Sequence is empty.")


    def callback_stop(self, event):
        self.vis.print_message("stopping operator orch")
        self.vis.doc.add_next_tick_callback(partial(self.orch.stop))
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_skip_dec(self, event):
        self.vis.print_message("skipping experiment")
        self.vis.doc.add_next_tick_callback(partial(self.orch.skip))
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_clear_seqg(self, event):
        self.vis.print_message("clearing seqg table")
        self.sequence = None
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_clear_experiments(self, event):
        self.vis.print_message("clearing experiments")
        self.vis.doc.add_next_tick_callback(partial(self.orch.clear_experiments))
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_clear_actions(self, event):
        self.vis.print_message("clearing actions")
        self.vis.doc.add_next_tick_callback(partial(self.orch.clear_actions))
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_prepend_seqg(self, event):
        sequence = self.populate_sequence()
        for i, D in enumerate(sequence.experiment_plan_list):
            self.sequence.experiment_plan_list.insert(i,D)
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_append_seqg(self, event):
        sequence = self.populate_sequence()
        for D in sequence.experiment_plan_list:
            self.sequence.experiment_plan_list.append(D)
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))        


    def callback_prepend_prc(self, event):
        self.prepend_experiment()
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_append_prc(self, event):
        self.append_experiment()
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def callback_update_tables(self, event):
        self.vis.doc.add_next_tick_callback(partial(self.update_tables))


    def append_experiment(self):
        experimenttemplate = self.populate_experimenttemplate()
        self.sequence.experiment_plan_list.append(experimenttemplate)


    def prepend_experiment(self):
        experimenttemplate = self.populate_experimenttemplate()
        self.sequence.experiment_plan_list.insert(0,experimenttemplate)


    def populate_sequence(self):
        selected_sequence = self.sequence_dropdown.value
        self.vis.print_message(f"selected sequence from list: {selected_sequence}")

        sequence_params = {paraminput.title: to_json(paraminput.value) for paraminput in self.seq_param_input}
        expplan_list = self.orch.unpack_sequence(
                        sequence_name = selected_sequence,
                        sequence_params = sequence_params
                       )

        sequence = Sequence()
        sequence.sequence_name = selected_sequence
        sequence.sequence_label = self.input_sequence_label.value
        sequence.sequence_params = sequence_params
        for expplan in expplan_list:
            sequence.experiment_plan_list.append(expplan)

        if self.sequence is None:
            self.sequence = Sequence()
        self.sequence.sequence_name = sequence.sequence_name
        self.sequence.sequence_label = sequence.sequence_label
        self.sequence.sequence_params = sequence.sequence_params

        return sequence


    def populate_experimenttemplate(self) -> ExperimentTemplate:
        selected_experiment = self.experiment_dropdown.value
        self.vis.print_message(f"selected experiment from list: {selected_experiment}")
        experiment_params = {paraminput.title: to_json(paraminput.value) for paraminput in self.prc_param_input}
        experimenttemplate = ExperimentTemplate(
            experiment_name = selected_experiment,
            experiment_params = experiment_params
        )
        if self.sequence is None:
            self.sequence = Sequence()
        self.sequence.sequence_name = "manual_orch_seq"
        self.sequence.sequence_label = self.input_sequence_label.value
        return experimenttemplate


    def refresh_inputs(self, param_input, private_input):
        input_plate_id = self.find_input(param_input, "solid_plate_id")
        input_sample_no = self.find_input(param_input, "solid_sample_no")
        if input_plate_id is not None:
            self.vis.doc.add_next_tick_callback(partial(
                                                        self.callback_changed_plateid,
                                                        attr = "value",
                                                        old = input_plate_id.value,
                                                        new = input_plate_id.value,
                                                        sender=input_plate_id
                                                       ))
        if input_sample_no is not None:
            self.vis.doc.add_next_tick_callback(partial(
                                                        self.callback_changed_sampleno,
                                                        attr = "value",
                                                        old = input_sample_no.value,
                                                        new = input_sample_no.value,
                                                        sender=input_sample_no
                                                       ))


    def update_input_value(self, sender, value):
        sender.value = value


    def update_seq_param_layout(self, idx):
        args = self.sequences[idx]["args"]
        defaults = self.sequences[idx]["defaults"]
        self.dynamic_col.children.pop(1)

        for _ in range(len(args)-len(defaults)):
            defaults.insert(0,"")

        self.seq_param_input = []
        self.seq_private_input = []
        self.seq_param_layout = [
            layout([
                [Spacer(width=10), bmw.Div(text="<b>Optional sequence parameters:</b>", width=200+50, height=15, style={"font-size": "100%", "color": "black"})],
                ],background=self.color_sq_param_inputs,width=640),
            ]


        self.add_dynamic_inputs(
                                self.seq_param_input,
                                self.seq_private_input,
                                self.seq_param_layout,
                                args,
                                defaults
                               )


        if not self.seq_param_input:
            self.seq_param_layout.append(
                    layout([
                    [Spacer(width=10), bmw.Div(text="-- none --", width=200+50, height=15, style={"font-size": "100%", "color": "black"})],
                    ],background=self.color_sq_param_inputs,width=640),
                )

        self.dynamic_col.children.insert(1, layout(self.seq_param_layout))

        self.refresh_inputs(self.seq_param_input, self.seq_private_input)


    def update_prc_param_layout(self, idx):
        args = self.experiments[idx]["args"]
        defaults = self.experiments[idx]["defaults"]
        self.dynamic_col.children.pop(3)

        for _ in range(len(args)-len(defaults)):
            defaults.insert(0,"")

        self.prc_param_input = []
        self.prc_private_input = []
        self.prc_param_layout = [
            layout([
                [Spacer(width=10), bmw.Div(text="<b>Optional experiment parameters:</b>", width=200+50, height=15, style={"font-size": "100%", "color": "black"})],
                ],background=self.color_sq_param_inputs,width=640),
            ]
        self.add_dynamic_inputs(
                                self.prc_param_input,
                                self.prc_private_input,
                                self.prc_param_layout,
                                args,
                                defaults
                               )


        if not self.prc_param_input:
            self.prc_param_layout.append(
                    layout([
                    [Spacer(width=10), bmw.Div(text="-- none --", width=200+50, height=15, style={"font-size": "100%", "color": "black"})],
                    ],background=self.color_sq_param_inputs,width=640),
                )

        self.dynamic_col.children.insert(3, layout(self.prc_param_layout))

        self.refresh_inputs(self.prc_param_input, self.prc_private_input)



    def add_dynamic_inputs(
                           self,
                           param_input,
                           private_input,
                           param_layout,
                           args,
                           defaults
                          ):
        item = 0
        for idx in range(len(args)):
            def_val = f"{defaults[idx]}"
            # if args[idx] == "experiment":
            #     continue
            disabled = False

            param_input.append(TextInput(value=def_val, title=args[idx], disabled=disabled, width=400, height=40))
            param_layout.append(layout([
                        [param_input[item]],
                        Spacer(height=10),
                        ],background=self.color_sq_param_inputs,width=640))
            item = item + 1

            # special key params
            if args[idx] == "solid_plate_id":
                param_input[-1].on_change("value", partial(self.callback_changed_plateid, sender=param_input[-1]))
                private_input.append(bpl.figure(
                    title="PlateMap", 
                    # height=300,
                    x_axis_label="X (mm)", 
                    y_axis_label="Y (mm)",
                    width = 640,
                    aspect_ratio  = 6/4,
                    aspect_scale = 1
                    ))
                private_input[-1].border_fill_color = self.color_sq_param_inputs
                private_input[-1].border_fill_alpha = 0.5
                private_input[-1].background_fill_color = self.color_sq_param_inputs
                private_input[-1].background_fill_alpha = 0.5
                private_input[-1].on_event(DoubleTap, partial(self.callback_clicked_pmplot, sender=param_input[-1]))
                self.update_pm_plot(private_input[-1], [])
                param_layout.append(layout([
                            [private_input[-1]],
                            Spacer(height=10),
                            ],background=self.color_sq_param_inputs,width=640))

                private_input.append(TextInput(value="", title="elements", disabled=True, width=120, height=40))
                private_input.append(TextInput(value="", title="code", disabled=True, width=60, height=40))
                private_input.append(TextInput(value="", title="composition", disabled=True, width=220, height=40))
                param_layout.append(layout([
                            [private_input[-3], private_input[-2], private_input[-1]],
                            Spacer(height=10),
                            ],background=self.color_sq_param_inputs,width=640))

            elif args[idx] == "solid_sample_no":
                param_input[-1].on_change("value", partial(self.callback_changed_sampleno, sender=param_input[-1]))

            elif args[idx] == "x_mm":
                param_input[-1].disabled = True

            elif args[idx] == "y_mm":
                param_input[-1].disabled = True

            elif args[idx] == "solid_custom_position":
                param_input[-1] = Select(title=args[idx], value = None, options=self.dev_customitems)
                if self.dev_customitems:
                    if def_val in self.dev_customitems:
                        param_input[-1].value = def_val
                    else:
                        param_input[-1].value = self.dev_customitems[0]
                param_layout[-1] = layout([
                            [param_input[-1]],
                            Spacer(height=10),
                            ],background=self.color_sq_param_inputs,width=640)

            elif args[idx] == "liquid_custom_position":
                param_input[-1] = Select(title=args[idx], value = None, options=self.dev_customitems)
                if self.dev_customitems:
                    if def_val in self.dev_customitems:
                        param_input[-1].value = def_val
                    else:
                        param_input[-1].value = self.dev_customitems[0]
                param_layout[-1] = layout([
                            [param_input[-1]],
                            Spacer(height=10),
                            ],background=self.color_sq_param_inputs,width=640)

            elif args[idx] == "plate_sample_no_list":
                private_input.append(FileInput(width=200,accept=".txt"))
                param_layout.append(layout([
                            [private_input[-1]],
                            Spacer(height=10),
                            ],background=self.color_sq_param_inputs,width=640))
                private_input[-1].on_change("value", 
                                            partial(self.callback_plate_sample_no_list_file, 
                                                    sender=private_input[-1], 
                                                    inputfield=param_input[-1]))


    def update_seq_doc(self, value):
        self.sequence_descr_txt.text = value.replace("\n", "<br>")


    def update_prc_doc(self, value):
        self.experiment_descr_txt.text = value.replace("\n", "<br>")


    def update_error(self, value):
        self.error_txt.text = value


    def update_xysamples(self, xval, yval, sender):

        private_input, param_input = self.find_param_private_input(sender)
        if private_input is None or param_input is None:
            return False

        for paraminput in param_input:
            if paraminput.title == "x_mm":
                paraminput.value = xval
            if paraminput.title == "y_mm":
                paraminput.value = yval


    def update_pm_plot(self, plot_mpmap, pmdata):
        """plots the plate map"""
        x = [col["x"] for col in pmdata]
        y = [col["y"] for col in pmdata]
        # remove old Pmplot
        old_point = plot_mpmap.select(name="PMplot")
        if len(old_point)>0:
            plot_mpmap.renderers.remove(old_point[0])
        plot_mpmap.square(x, y, size=5, color=None, alpha=0.5, line_color="black",name="PMplot")


    def get_pm(self, plateid, sender):
        """"gets plate map"""
        private_input, param_input = self.find_param_private_input(sender)
        if private_input is None or param_input is None:
            return False

        # pmdata = json.loads(self.dataAPI.get_platemap_plateid(plateid))
        pmdata = self.dataAPI.get_platemap_plateid(plateid)
        if len(pmdata) == 0:
            self.vis.doc.add_next_tick_callback(partial(self.update_error,"no pm found"))

        plot_mpmap = self.find_plot(private_input, "PlateMap")
        if plot_mpmap is not None:
            self.vis.doc.add_next_tick_callback(partial(self.update_pm_plot, plot_mpmap, pmdata))


    def xy_to_sample(self, xy, pmapxy):
        """get point from pmap closest to xy"""
        if len(pmapxy):
            diff = pmapxy - xy
            sumdiff = (diff ** 2).sum(axis=1)
            return np.int(np.argmin(sumdiff))
        else:
            return None
    
    
    def get_samples(self, X, Y, sender):
        """get list of samples row number closest to xy"""
        # X and Y are vectors

        private_input, param_input = self.find_param_private_input(sender)
        if private_input is None or param_input is None:
            return False

        input_plate_id = self.find_input(param_input, "solid_plate_id")

        if input_plate_id is not None:
            # pmdata = json.loads(self.dataAPI.get_platemap_plateid(input_plate_id.value))
            pmdata = self.dataAPI.get_platemap_plateid(input_plate_id.value)
    
            xyarr = np.array((X, Y)).T
            pmxy = np.array([[col["x"], col["y"]] for col in pmdata])
            samples = list(np.apply_along_axis(self.xy_to_sample, 1, xyarr, pmxy))
            return samples
        else:
            return [None]


    def get_elements_plateid(self, plateid: int, sender):
        """gets plate elements from aligner server"""


        private_input, param_input = self.find_param_private_input(sender)
        if private_input is None or param_input is None:
            return False

        input_elements = self.find_input(private_input, "elements")

        if input_elements is not None:

            elements =  self.dataAPI.get_elements_plateid(
                plateid,
                multielementink_concentrationinfo_bool=False,
                print_key_or_keyword="screening_print_id",
                exclude_elements_list=[""],
                return_defaults_if_none=False)
            if elements is not None:
                self.vis.doc.add_next_tick_callback(partial(self.update_input_value, input_elements, ",".join(elements)))


    def find_plot(self, inputs, name):
        for inp in inputs:
            if isinstance(inp, bpl.Figure):
                if inp.title.text == name:
                    return inp
        return None

    def find_input(self, inputs, name):
        for inp in inputs:
            if isinstance(inp, bmw.inputs.TextInput):
                if inp.title == name:
                    return inp
        return None
    
    
    def find_param_private_input(self, sender):
        private_input = None
        param_input = None

        if sender in self.prc_param_input \
        or sender in self.prc_private_input:
            private_input = self.prc_private_input
            param_input = self.prc_param_input

        elif sender in self.seq_param_input \
        or sender in self.seq_private_input:
            private_input = self.seq_private_input
            param_input = self.seq_param_input
        
        return  private_input, param_input

 
    def get_sample_infos(self, PMnum: List = None, sender = None):
        self.vis.print_message("updating samples")

        private_input, param_input = self.find_param_private_input(sender)
        if private_input is None or param_input is None:
            return False

        plot_mpmap = self.find_plot(private_input, "PlateMap")
        input_plate_id = self.find_input(param_input, "solid_plate_id")
        input_sample_no = self.find_input(param_input, "solid_sample_no")
        input_code = self.find_input(private_input, "code")
        input_composition = self.find_input(private_input, "composition")
        if plot_mpmap is not None \
        and input_plate_id is not None \
        and input_sample_no is not None:
            # pmdata = json.loads(self.dataAPI.get_platemap_plateid(input_plate_id.value))
            pmdata = self.dataAPI.get_platemap_plateid(input_plate_id.value)
            buf = ""
            if PMnum is not None and pmdata:
                if PMnum[0] is not None: # need to check as this can also happen
                    self.vis.print_message(f"selected sample_no: {PMnum[0]+1}")
                    if PMnum[0] > len(pmdata) or PMnum[0] < 0:
                        self.vis.print_message("invalid sample no")
                        self.vis.doc.add_next_tick_callback(partial(self.update_input_value,input_sample_no,""))
                        return False
                    
                    platex = pmdata[PMnum[0]]["x"]
                    platey = pmdata[PMnum[0]]["y"]
                    code = pmdata[PMnum[0]]["code"]
    
                    buf = ""
                    for fraclet in ("A", "B", "C", "D", "E", "F", "G", "H"):
                        buf = "%s%s_%s " % (buf,fraclet, pmdata[PMnum[0]][fraclet])
                    if len(buf) == 0:
                        buf = "-"
                    if input_sample_no != str(PMnum[0]+1):
                        self.vis.doc.add_next_tick_callback(partial(self.update_input_value, input_sample_no,str(PMnum[0]+1)))
                    self.vis.doc.add_next_tick_callback(partial(self.update_xysamples,str(platex), str(platey), sender))
                    if input_composition is not None:
                        self.vis.doc.add_next_tick_callback(partial(self.update_input_value,input_composition,buf))
                    if input_code is not None:
                        self.vis.doc.add_next_tick_callback(partial(self.update_input_value,input_code,str(code)))
    
                    # remove old Marker point
                    old_point = plot_mpmap.select(name="selsample")
                    if len(old_point)>0:
                        plot_mpmap.renderers.remove(old_point[0])
                    # plot new Marker point
                    plot_mpmap.square(platex, platey, size=7,line_width=2, color=None, alpha=1.0, line_color=(255,0,0), name="selsample")

                    return True
            else:
                return False

        return False


    async def add_experiment_to_sequence(self):
        pass
        

    async def update_tables(self):
        await self.get_experiments()
        await self.get_actions()
        await self.get_active_actions()

        self.experiment_plan_list = dict()

        self.experiment_plan_list["sequence_name"] = []
        self.experiment_plan_list["sequence_label"] = []
        self.experiment_plan_list["experiment_name"] = []
        if self.sequence is not None:
            for D in self.sequence.experiment_plan_list:
                self.experiment_plan_list["sequence_name"].append(self.sequence.sequence_name)
                self.experiment_plan_list["sequence_label"].append(self.sequence.sequence_label)
                self.experiment_plan_list["experiment_name"].append(D.experiment_name)


        self.columns_seq = [TableColumn(field=key, title=key) for key in self.experiment_plan_list]
        self.experimentplan_table.source.data = self.experiment_plan_list
        self.experimentplan_table.columns=self.columns_seq


        self.columns_prc = [TableColumn(field=key, title=key) for key in self.experiment_list]
        self.experiment_table.source.data = self.experiment_list
        self.experiment_table.columns=self.columns_prc

        self.columns_act = [TableColumn(field=key, title=key) for key in self.action_list]
        self.action_table.source.data=self.action_list
        self.action_table.columns=self.columns_act

        self.columns_active_action = [TableColumn(field=key, title=key) for key in self.active_action_list]
        self.active_action_table.source.data=self.active_action_list
        self.active_action_table.columns=self.columns_active_action


    async def IOloop(self):
        await self.update_tables()
