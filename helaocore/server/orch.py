__all__ = ["Orch", "makeOrchServ"]

import asyncio
import sys
from collections import deque
from copy import copy, deepcopy
from math import floor
from typing import Optional, Union, List
from uuid import UUID
from datetime import datetime
from socket import gethostname

import aiohttp
import colorama
import time
from fastapi import WebSocket, Body

from enum import Enum

from .base import Base
from .api import HelaoFastAPI
from .import_experiments import import_experiments
from .import_sequences import import_sequences
from .dispatcher import async_private_dispatcher, async_action_dispatcher
from .action_start_condition import action_start_condition

from ..helper.multisubscriber_queue import MultisubscriberQueue
from ..schema import Sequence, Experiment, Action
from ..model.experiment_sequence import ExperimentSequenceModel
from ..model.action import ActionModel
from ..model.hlostatus import HloStatus
from ..model.server import ActionServerModel, GlobalStatusModel
from ..model.machine import MachineModel
from ..model.orchstatus import OrchStatus


# ANSI color codes converted to the Windows versions
colorama.init(strip=not sys.stdout.isatty())  # strip colors if stdout is redirected
# colorama.init()


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

    @app.post("/update_status")
    async def update_status(actionserver: Optional[ActionServerModel] = \
                            Body(None , embed=True)):
        if actionserver is None:
            return False

        app.orch.print_message(f"orch '{app.orch.server.server_name}' "
                                f"got status from "
                                f"'{actionserver.action_server.server_name}': "
                                f"{actionserver.endpoints}")
        return await app.orch.update_status(actionserver = actionserver)

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
        if app.orch.orchstatusmodel.loop_state == OrchStatus.stopped:
            if app.orch.action_dq \
            or app.orch.experiment_dq \
            or app.orch.sequence_dq:  # resume actions from a paused run
                await app.orch.start_loop()
            else:
                app.orch.print_message("experiment list is empty")
        else:
            app.orch.print_message("already running")
        return {}

    @app.post("/estop")
    async def estop():
        """Emergency stop experiment and action queues, interrupt running actions."""
        if app.orch.orchstatusmodel.loop_state == OrchStatus.started:
            await app.orch.estop_loop()
        elif app.orch.orchstatusmodel.loop_state == OrchStatus.estop:
            app.orch.print_message("orchestrator E-STOP flag already raised")
        else:
            app.orch.print_message("orchestrator is not running")
        return {}

    @app.post("/stop")
    async def stop():
        """Stop experimenting experiment and action queues after current actions finish."""
        if app.orch.orchstatusmodel.loop_state == OrchStatus.started:
            await app.orch.intend_stop()
        elif app.orch.orchstatusmodel.loop_state == OrchStatus.estop:
            app.orch.print_message("orchestrator E-STOP flag was raised; nothing to stop")
        else:
            app.orch.print_message("orchestrator is not running")
        return {}

    @app.post("/clear_estop")
    async def clear_estop():
        """Remove emergency stop condition."""
        if app.orch.orchstatusmodel.loop_state != OrchStatus.estop:
            app.orch.print_message("orchestrator is not currently in E-STOP")
        else:
            await app.orch.clear_estop()

    @app.post("/clear_error")
    async def clear_error():
        """Remove error condition."""
        if app.orch.orchstatusmodel.loop_state != OrchStatus.error:
            app.orch.print_message("orchestrator is not currently in ERROR")
        else:
            await app.orch.clear_error()

    @app.post("/skip")
    async def skip_experiment():
        """Clear the present action queue while running."""
        if app.orch.orchstatusmodel.loop_state == OrchStatus.started:
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
        sequence_params: Optional[dict] = Body(None, embed=True),
        sequence_label: str = None,
        experiment_plan_list: List[dict] = Body([], embed=True),
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


        # basemodel which holds all information for orch
        self.orchstatusmodel = GlobalStatusModel(orchestrator = self.server)
        # this queue is simply used for waiting for any interrupt
        # but it does not do anything with its content
        self.interrupt_q = MultisubscriberQueue()


        self.init_success = False  # need to subscribe to all fastapi servers in config

        # pointer to dispatch_loop_task
        self.loop_task = None
        self.status_subscriber = asyncio.create_task(self.subscribe_all())


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


    async def check_all_actions_idle(self) -> bool:
        """checks if this orch as runnung acts
           orther orch can still have active acts
        """
        global_free = len(self.orchstatusmodel.active_acts) == 0
        self.print_message(f"check for running acts: "
                           f"{global_free}")
        return global_free



    async def check_server_free(
                                self, 
                                action_server: MachineModel
                               ) -> bool:
        return self.orchstatusmodel.server_free(
            action_server = action_server, # MachineModel
        )


    async def check_endpoint_free(
                                  self, 
                                  action_server: MachineModel, 
                                  endpoint_name: str
                                 ) -> bool:
        """check if an endpoint can accept new actions for this orch
           for multiple orch the enpoints need to implement internal queueing
           TODO
           """
        return self.orchstatusmodel.endpoint_free(
            action_server = action_server, # MachineModel
            endpoint_name = endpoint_name # name of the endpoint
        )


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
                        response = await async_private_dispatcher(
                            world_config_dict=self.world_cfg,
                            server=serv_key,
                            private_action="attach_client",
                            params_dict={"client_servkey": self.server.server_name},
                            json_dict={},
                        )
                        if response == True:
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
        elif not self.orchstatusmodel.active_acts:
            # no uuids in active action dict
            self.orchstatusmodel.orch_state = OrchStatus.idle
        else:
            self.orchstatusmodel.orch_state = OrchStatus.busy
            self.print_message(f"running_states: "
                               f"{self.orchstatusmodel.active_acts}")


        # now push it to the interrupt_q
        await self.interrupt_q.put(self.orchstatusmodel)

        return True


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
                self.active_sequence.sequence_status = [HloStatus.active]
                self.active_sequence.sequence_output_dir = self.get_sequence_dir(self.active_sequence)
    
                # todo: this is for later, for now the operator needs to unpack the sequence
                # in order to also use a semi manual op mode
                
                # self.print_message(f"unpacking experiments for {self.active_sequence.sequence_name}")
                # if self.active_sequence.sequence_name in self.sequence_lib:
                #     unpacked_prcs = self.sequence_lib[self.active_sequence.sequence_name](**self.active_sequence.sequence_params)
                # else:
                #     unpacked_prcs = []
    
                # for prc in unpacked_prcs:
                #     D = Experiment(inputdict=prc)
                #     self.active_sequence.experiment_plan_list.append(D)
    
    
                self.seq_file = self.active_sequence.get_seq()
                await self.write_seq(self.active_sequence)
    
                # add all experiments from sequence to experiment queue
                # todo: use seq model instead to initialize some parameters
                # of the experiment
                for prc in self.active_sequence.experiment_plan_list:
                        self.print_message(f"unpack experiment "
                                           f"{prc.experiment_name}")
                        await self.add_experiment(
                            seq = self.active_sequence.get_seq(),
                            # prc = prc,
                            experiment_name = prc.experiment_name,
                            experiment_params = prc.experiment_params,
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
                    self.active_experiment.set_dtime(offset=self.ntp_offset)
                    self.active_experiment.gen_uuid_experiment()
                    self.active_experiment.access = "hte"
                    self.active_experiment.experiment_status = [HloStatus.active]
                    self.active_experiment.experiment_output_dir = self.get_experiment_dir(self.active_experiment)

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
                        act.action_actual_order = int(i)

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

                        if A.start_condition == action_start_condition.no_wait:
                            self.print_message("orch is dispatching an unconditional action")
                        else:
                            if A.start_condition == action_start_condition.wait_for_endpoint:
                                self.print_message(
                                    "orch is waiting for endpoint to become available"
                                )
                                while True:
                                    await self.wait_for_interrupt()
                                    endpoint_free = \
                                        await self.check_endpoint_free(
                                            action_server=A.action_server,
                                            endpoint_name=A.action_name)
                                    if endpoint_free:
                                        break
                            elif A.start_condition == action_start_condition.wait_for_server:
                                self.print_message("orch is waiting for "
                                                   "server to become available")
                                while True:
                                    await self.wait_for_interrupt()
                                    server_free = \
                                        self.check_server_free(
                                            check_server_free = A.action_server)
                                    if server_free:
                                        break
                            elif A.start_condition == action_start_condition.wait_for_all:
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
                        A.action_actual_order = self.orchstatusmodel.counter_dispatched_actions
                        self.orchstatusmodel.counter_dispatched_actions +=1

                        A.init_act(time_offset = self.ntp_offset)
                        result = await async_action_dispatcher(self.world_cfg, A)

                        self.active_experiment.result_dict[A.action_actual_order] = result

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
            self.print_message("stopping operator orch")

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
        
        if not await self.check_all_actions_idle():
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
                if await self.check_all_actions_idle():
                    self.print_message("all actions are idle now")
                    break
        else:
            self.print_message("all actions are idle")


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
            exp = Experiment(inputdict = {})
            exp.sequence_name = "orch_estop"
            exp.init_seq(time_offset=self.base.ntp_offset)
            exp.sequence_status = [HloStatus.estopped, HloStatus.finished]
            exp.sequence_output_dir = self.get_sequence_dir(exp)

            exp.technique_name = self.technique_name
            exp.orchestrator = self.server
            exp.set_dtime(offset=self.ntp_offset)
            exp.gen_uuid_experiment()
            exp.access = "hte"
            exp.experiment_status = [HloStatus.estopped, HloStatus.finished]
            exp.experiment_output_dir = self.get_experiment_dir(exp)
            active_exp_dict = exp.as_dict()


        for action_server, actionservermodel \
        in self.server_dict.items():
            action_dict = copy(active_exp_dict)
            action_dict.update({
                "action_name":"estop",
                "action_server":actionservermodel.action_server.dict(),
                "action_params":{"switch":switch},
                "start_condition":action_start_condition.no_wait
                })

            A = Action(inputdict=action_dict,
                      act=ActionModel(**action_dict)
                     )
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


    async def intend_skip(self):
        await asyncio.sleep(0.001)
        self.orchstatusmodel.loop_intent = OrchStatus.skip
        await self.interrupt_q.put(self.orchstatusmodel.loop_intent)


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


    async def add_sequence(
                           self,
                           sequence_uuid: UUID = None,
                           sequence_timestamp: datetime = None,
                           sequence_name: str = None,
                           sequence_params: dict = None,
                           sequence_label: str = None,
                           experiment_plan_list: List[dict] = []
                           ):
        seq = Sequence()
        seq.sequence_uuid = sequence_uuid
        seq.sequence_timestamp = sequence_timestamp
        seq.sequence_name = sequence_name
        seq.sequence_params = sequence_params
        seq.sequence_label = sequence_label
        for D_dict in experiment_plan_list:
            D = Experiment(D_dict)
            seq.experiment_plan_list.append(D)
        self.sequence_dq.append(seq)


    async def add_experiment(
        self,
        seq: ExperimentSequenceModel,
        orchestrator: str = None,
        experiment_name: str = None,
        experiment_params: dict = {},
        result_dict: dict = {},
        access: str = "hte",
        prepend: Optional[bool] = False,
        at_index: Optional[int] = None,
    ):
        Ddict = {
                "orchestrator": orchestrator,
                "experiment_name": experiment_name,
                "experiment_params": experiment_params,
                "result_dict": result_dict,
                "access": access,
            }
        Ddict.update(seq.dict())
        D = Experiment(Ddict)

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
        for uuid, statusmodel in self.orchstatusmodel.active_acts.items():
            action_list.append(
                {"index":index,
                 "action_uuid":statusmodel.action_uuid,
                 "server":statusmodel.action_server.disp_name(),
                 "action_name":statusmodel.action_name,
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
            last_action_order = self.orchstatusmodel.counter_dispatched_actions - 1
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
            self.last_sequence = copy(self.active_sequence)
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
            self.active_sequence.experiment_list.append(
                deepcopy(self.active_experiment.get_prc())
            )
            # write new updated seq
            await self.write_active_sequence_seq()

            # write final prc
            await self.write_prc(self.active_experiment)


        self.last_experiment = copy(self.active_experiment)
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
