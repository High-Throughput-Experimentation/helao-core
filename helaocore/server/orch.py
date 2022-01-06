__all__ = ["Orch"]

import asyncio
import sys
from collections import defaultdict, deque
from copy import copy, deepcopy
from math import floor
from typing import Optional, Union, List
from uuid import UUID
from datetime import datetime

import aiohttp
import colorama

from .base import Base
from .api import HelaoFastAPI
from .import_processes import import_processes
from .import_sequences import import_sequences
from .dispatcher import async_private_dispatcher, async_action_dispatcher
from .action_start_condition import action_start_condition

from ..helper.multisubscriber_queue import MultisubscriberQueue
from ..schema import Sequence, Process, Action
from ..model.process_sequence import ProcessSequenceModel
from ..model.action import ActionModel

# ANSI color codes converted to the Windows versions
colorama.init(strip=not sys.stdout.isatty())  # strip colors if stdout is redirected
# colorama.init()


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
        self.process_lib = import_processes(
            world_config_dict=self.world_cfg,
            process_path=None,
            server_name=self.server_name,
        )
        self.sequence_lib = import_sequences(
            world_config_dict = self.world_cfg,
            sequence_path = None,
            server_name=self.server_name
        )

        # instantiate process/experiment queue, action queue
        self.sequence_dq = deque([])
        self.process_dq = deque([])
        self.action_dq = deque([])
        self.dispatched_actions = {}
        self.finished_actions = []
        self.active_process = None
        self.last_process = None
        self.active_sequence = None
        self.last_sequence = None
        self.prc_file = None
        self.seq_file = None

        # compilation of action server status dicts
        self.global_state_dict = defaultdict(lambda: defaultdict(list))
        self.global_state_dict["_internal"]["async_action_dispatcher"] = []
        self.global_q = MultisubscriberQueue()  # passes global_state_dict dicts
        self.dispatch_q = self.global_q.queue()

        # global state of all instruments as string [idle|busy] independent of dispatch loop
        self.global_state_str = None

        # uuid lists for estop and error tracking used by update_global_state_task
        self.error_uuids = []
        self.estop_uuids = []
        self.running_uuids = []

        self.init_success = False  # need to subscribe to all fastapi servers in config
        # present dispatch loop state [started|stopped]
        self.loop_state = "stopped"

        # separate from global state, signals dispatch loop control [skip|stop|None]
        self.loop_intent = None

        # pointer to dispatch_loop_task
        self.loop_task = None
        self.status_subscriber = asyncio.create_task(self.subscribe_all())
        self.status_subscriber = asyncio.create_task(self.update_global_state_task())


    async def check_dispatch_queue(self):
        val = await self.dispatch_q.get()
        while not self.dispatch_q.empty():
            val = await self.dispatch_q.get()
        return val


    async def check_all_actions_idle(self):
        running_states, _ = await self.check_global_state()
        global_free = len(running_states) == 0
        self.print_message(f"check len(running_states): {len(running_states)}")
        return global_free


    async def subscribe_all(self, retry_limit: int = 5):
        """Subscribe to all fastapi servers in config."""
        fails = []
        for serv_key, serv_dict in self.world_cfg["servers"].items():
            if "fast" in serv_dict:
                self.print_message(f"trying to subscribe to {serv_key} status")

                success = False
                serv_addr = serv_dict["host"]
                serv_port = serv_dict["port"]
                for _ in range(retry_limit):
                    try:
                        response = await async_private_dispatcher(
                            world_config_dict=self.world_cfg,
                            server=serv_key,
                            private_action="attach_client",
                            params_dict={"client_servkey": self.server_name},
                            json_dict={},
                        )
                        if response == True:
                            success = True
                            break
                    except aiohttp.client_exceptions.ClientConnectorError:
                        self.print_message(f"failed to subscribe to {serv_key} at {serv_addr}:{serv_port}, trying again in 1sec", error = True)
                        await asyncio.sleep(1) 
                        

                if success:
                    self.print_message(f"Subscribed to {serv_key} at {serv_addr}:{serv_port}")
                else:
                    fails.append(serv_key)
                    self.print_message(
                        f"Failed to subscribe to {serv_key} at {serv_addr}:{serv_port}. Check connection."
                    )

        if len(fails) == 0:
            self.init_success = True
        else:
            self.print_message(
                "Orchestrator cannot action process_dq unless all FastAPI servers in config file are accessible."
            )


    async def update_status(self, action_serv: str, status_dict: dict):
        """Dict update method for action server to push status messages.

        Async task for updating orch status dict {action_serv_key: {act_name: [act_uuid]}}
        """
        last_dict = self.global_state_dict[action_serv]
        for act_name, acts in status_dict.items():
            if act_name == "act":
                self.finished_actions.append(acts)
            else:
                self.global_state_dict[action_serv].update({act_name:acts})
                await self.global_q.put(self.global_state_dict)
                if set(acts) != set(last_dict[act_name]):
                    started = set(acts).difference(last_dict[act_name])
                    removed = set(last_dict[act_name]).difference(acts)
                    ongoing = set(acts).intersection(last_dict[act_name])
                    if removed:
                        self.print_message(f"'{action_serv}:{act_name}' finished {','.join(removed)}")
                    if started:
                        self.print_message(f"'{action_serv}:{act_name}' started {','.join(started)}")
                    if ongoing:
                        self.print_message(f"'{action_serv}:{act_name}' ongoing {','.join(ongoing)}")
        # self.global_state_dict[action_serv].update(status_dict)
        # await self.global_q.put(self.global_state_dict)
        return True


    async def update_global_state(self, status_dict: dict):
        _running_uuids = []
        for action_serv, act_named in status_dict.items():
            for act_name, uuids in act_named.items():
                for myuuid in uuids:
                    uuid_tup = (action_serv, act_name, myuuid)
                    if myuuid.endswith("__estop"):
                        self.estop_uuids.append(uuid_tup)
                    elif myuuid.endswith("__error"):
                        self.error_uuids.append(uuid_tup)
                    else:
                        _running_uuids.append(uuid_tup)
        self.running_uuids = _running_uuids


    async def update_global_state_task(self):
        """Self-subscribe to global_q and update status dict."""
        async for status_dict in self.global_q.subscribe():
            self.print_message(f"got message: {status_dict}")
            await self.update_global_state(status_dict)
            running_states, _ = await self.check_global_state()
            if self.estop_uuids and self.loop_state == "started":
                await self.estop_loop()
            elif self.error_uuids and self.loop_state == "started":
                self.global_state_str = "error"
            elif len(running_states) == 0:
                self.global_state_str = "idle"
            else:
                self.global_state_str = "busy"
                self.print_message(f"running_states: {running_states}")


    async def check_global_state(self):
        """Return global state of action servers."""
        running_states = []
        idle_states = []
        # self.print_message("checking global state:")
        # self.print_message(self.global_state_dict.items())
        for action_serv, act_dict in self.global_state_dict.items():
            self.print_message(f"checking {action_serv} state")
            for act_name, act_uuids in act_dict.items():
                if len(act_uuids) == 0:
                    idle_states.append(f"{action_serv}:{act_name}")
                else:
                    running_states.append(f"{action_serv}:{act_name}:{len(act_uuids)}")
            await asyncio.sleep(
                0.001
            )
            # allows status changes to affect between action_dq, 
            # also enforce unique timestamp
        return running_states, idle_states



    async def dispatch_loop_task(self):
        """Parse process and action queues, and dispatch action_dq while tracking run state flags."""
        self.print_message("running operator orch")
        self.print_message(f"orch status: {self.global_state_str}")
        # clause for resuming paused action list
        self.print_message(f"orch descisions: {self.process_dq}")
        try:
            if self.sequence_dq:
                self.last_sequence = copy(self.active_sequence)
                self.active_sequence = self.sequence_dq.popleft()
                self.active_sequence.init_seq(machine_name = self.hostname,
                                              time_offset = self.ntp_offset)
                self.active_sequence.sequence_status = "active"
                self.active_sequence.sequence_output_dir = self.get_sequence_dir(self.active_sequence)
    
                # todo: this is for later, for now the operator needs to unpack the sequence
                # in order to also use a semi manual op mode
                
                # self.print_message(f"unpacking processes for {self.active_sequence.sequence_name}")
                # if self.active_sequence.sequence_name in self.sequence_lib:
                #     unpacked_prcs = self.sequence_lib[self.active_sequence.sequence_name](**self.active_sequence.sequence_params)
                # else:
                #     unpacked_prcs = []
    
                # for prc in unpacked_prcs:
                #     D = Process(inputdict=prc)
                #     self.active_sequence.process_plan_list.append(D)
    
    
                self.seq_file = self.active_sequence.get_seq()
                await self.write_seq(self.active_sequence)
    
                # add all processes from sequence to process queue
                # todo: use seq model instead to initialize some parameters
                # of the process
                for prc in self.active_sequence.process_plan_list:
                        self.print_message(f"unpack process {prc.process_name}")
                        await self.add_process(
                            seq = self.active_sequence.get_seq(),
                            # prc = prc,
                            process_name = prc.process_name,
                            process_params = prc.process_params,
                            )


                self.loop_state = "started"

            else:

                self.print_message("sequence queue is empty, cannot start orch loop")



            while self.loop_state == "started" and (self.action_dq or self.process_dq):
                self.print_message(f"current content of action_dq: {self.action_dq}")
                self.print_message(f"current content of process_dq: {self.process_dq}")
                await asyncio.sleep(
                    0.001
                )  
                # allows status changes to affect between action_dq, 
                # also enforce unique timestamp
                if not self.action_dq:

                    # this wait for all actions in active process
                    # to finish and then updates the prc with the acts
                    await self.finish_active_process()

                    self.print_message("getting action_dq from new process")
                    # generate uids when populating, 
                    # generate timestamp when acquring
                    self.last_process = copy(self.active_process)
                    self.active_process = self.process_dq.popleft()
                    self.print_message(f"new active process is {self.active_process.process_name}")

                    self.active_process.technique_name = self.technique_name
                    self.active_process.machine_name = self.hostname
                    self.active_process.set_dtime(offset=self.ntp_offset)
                    self.active_process.gen_uuid_process(self.hostname)
                    self.active_process.access = "hte"
                    self.active_process.process_status = "active"
                    self.active_process.process_output_dir = self.get_process_dir(self.active_process)

                    # additional process params should be stored 
                    # in process.process_params
                    self.print_message(f"unpacking actions for {self.active_process.process_name}")
                    unpacked_acts = self.process_lib[
                        self.active_process.process_name
                                                    ](self.active_process)
                    if unpacked_acts is None:
                        self.print_message(
                            "no actions in process",
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

                    # TODO:update process code
                    self.print_message("adding unpacked actions to action_dq")
                    self.action_dq = deque(unpacked_acts)
                    # self.dispatched_actions = {} # moved to finish prc function
                    self.print_message(f"got: {self.action_dq}")
                    self.print_message(f"optional params: {self.active_process.process_params}")


                    # write a temporary prc
                    self.prc_file = self.active_process.get_prc()
                    await self.write_active_process_prc()

                else:
                    if self.loop_intent == "stop":
                        self.print_message("stopping orchestrator")
                        # monitor status of running action_dq, then end loop
                        while True:
                            _ = await self.check_dispatch_queue()
                            if self.global_state_str == "idle":
                                self.loop_state = "stopped"
                                await self.intend_none()
                                self.print_message("got stop")
                                break
                    elif self.loop_intent == "skip":
                        # clear action queue, forcing next process
                        self.action_dq.clear()
                        await self.intend_none()
                        self.print_message("skipping to next process")
                    else:
                        # all action blocking is handled like preempt, 
                        # check Action requirements
                        A = self.action_dq.popleft()
                        # append previous results to current action
                        A.result_dict = self.active_process.result_dict

                        # see async_action_dispatcher for unpacking
                        if isinstance(A.start_condition, int):
                            if A.start_condition == action_start_condition.no_wait:
                                self.print_message("orch is dispatching an unconditional action")
                            else:
                                if A.start_condition == action_start_condition.wait_for_endpoint:
                                    self.print_message(
                                        "orch is waiting for endpoint to become available"
                                    )
                                    # async for _ in self.global_q.subscribe():
                                    while True:
                                        _ = await self.check_dispatch_queue()
                                        endpoint_free = (
                                            len(self.global_state_dict[A.action_server_name][A.action_name]) == 0
                                        )
                                        if endpoint_free:
                                            break
                                elif A.start_condition == action_start_condition.wait_for_server:
                                    self.print_message("orch is waiting for server to become available")
                                    # async for _ in self.global_q.subscribe():
                                    while True:
                                        _ = await self.check_dispatch_queue()
                                        server_free = all(
                                            [
                                                len(uuid_list) == 0
                                                for _, uuid_list in self.global_state_dict[
                                                    A.action_server_name
                                                ].items()
                                            ]
                                        )
                                        if server_free:
                                            break
                                else:  # start_condition is 3 or unsupported value
                                    await self.orch_wait_for_all_actions()


                                    # self.print_message("orch is waiting for all action_dq to finish")
                                    # if not await self.check_all_actions_idle():
                                    #     while True:
                                    #         _ = await self.check_dispatch_queue()
                                    #         if await self.check_all_actions_idle():
                                    #             break
                                    # else:
                                    #     self.print_message("global_free is true")



                        elif isinstance(A.start_condition, dict):
                            self.print_message("waiting for multiple conditions on external servers")
                            condition_dict = A.start_condition
                            # async for _ in self.global_q.subscribe():
                            while True:
                                _ = await self.check_dispatch_queue()
                                conditions_free = all(
                                    [
                                        len(self.global_state_dict[k][v] == 0)
                                        for k, vlist in condition_dict.items()
                                        if vlist and isinstance(vlist, list)
                                        for v in vlist
                                    ]
                                    + [
                                        len(uuid_list) == 0
                                        for k, v in condition_dict.items()
                                        if v == [] or v is None
                                        for _, uuid_list in self.global_state_dict[k].items()
                                    ]
                                )
                                if conditions_free:
                                    break
                        else:
                            self.print_message(
                                "invalid start condition, waiting for all action_dq to finish"
                            )
                            await self.orch_wait_for_all_actions()
                            # # async for _ in self.global_q.subscribe():
                            # while True:
                            #     _ = await self.check_dispatch_queue()
                            #     if await self.check_all_actions_idle():
                            #         break

                        self.print_message("copying global vars to action")

                        # copy requested global param to action params
                        for k, v in A.from_global_params.items():
                            self.print_message(f"{k}:{v}")
                            if k in self.active_process.global_params:
                                A.action_params.update({v: self.active_process.global_params[k]})

                        self.print_message(
                            f"dispatching action {A.action_name} on server {A.action_server_name}"
                        )
                        # keep running list of dispatched actions
                        # action_actual_order is len of self.dispatched_actions
                        # action_actual_order starts at 0, len is 0 for empty
                        # else >=1
                        A.action_actual_order = len(self.dispatched_actions)

                        A.init_act(machine_name = self.hostname,
                                   time_offset = self.ntp_offset)

                        self.dispatched_actions[A.action_actual_order] = copy(A)
                        result = await async_action_dispatcher(self.world_cfg, A)

                        self.active_process.result_dict[A.action_actual_order] = result

                        self.print_message("copying global vars back to process")
                        # self.print_message(result)
                        if "to_global_params" in result:
                            for k in result["to_global_params"]:
                                if k in result["action_params"]:
                                    if (
                                        result["action_params"][k] is None
                                        and k in self.active_process.global_params
                                    ):
                                        self.active_process.global_params.pop(k)
                                    else:
                                        self.active_process.global_params.update(
                                            {k: result["action_params"][k]}
                                        )
                        self.print_message("done copying global vars back to process")

            self.print_message("process queue is empty")
            self.print_message("stopping operator orch")

            # finish the last prc
            # this wait for all actions in active process
            # to finish and then updates the prc with the acts
            await self.finish_active_process()
            await self.finish_active_sequence()


            self.loop_state = "stopped"
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
    
        self.print_message("orch is waiting for all action_dq to finish")
        
        if not await self.check_all_actions_idle():
            # some actions are active
            # we need to wait for them to finish
            while True:
                self.print_message("some actions are still active, waiting for status update")
                # we check again once the active action
                # updates its status again
                _ = await self.check_dispatch_queue()
                self.print_message("got status update")
                # we got a status update
                # check if all actions are idle now
                if await self.check_all_actions_idle():
                    self.print_message("all actions are idle now")
                    break
        else:
            self.print_message("all actions are idle")


    async def start_loop(self):
        if self.loop_state == "stopped":
            self.print_message("starting orch loop")
            self.loop_task = asyncio.create_task(self.dispatch_loop_task())
        elif self.loop_state == "E-STOP":
            self.print_message("E-STOP flag was raised, clear E-STOP before starting.")
        else:
            self.print_message("loop already started.")
        return self.loop_state


    async def estop_loop(self):
        self.loop_state = "E-STOP"
        self.loop_task.cancel()
        await self.force_stop_running_action_q()
        await self.intend_none()


    async def force_stop_running_action_q(self):
        running_uuids = []
        estop_uuids = []
        for action_serv, act_named in self.global_state_dict.items():
            for act_name, uuids in act_named.items():
                for myuuid in uuids:
                    uuid_tup = (action_serv, act_name, myuuid)
                    if myuuid.endswith("__estop"):
                        estop_uuids.append(uuid_tup)
                    else:
                        running_uuids.append(uuid_tup)
        running_servers = list(set([serv for serv, _, _ in running_uuids]))
        for serv in running_servers:
            serv_conf = self.world_cfg["servers"][serv]
            async with aiohttp.ClientSession() as session:
                self.print_message(f"Sending force-stop request to {serv}")
                async with session.post(f"http://{serv_conf['host']}:{serv_conf['port']}/force_stop") as resp:
                    response = await resp.text()
                    self.print_message(response)


    async def intend_skip(self):
        await asyncio.sleep(0.001)
        self.loop_intent = "skip"


    async def intend_stop(self):
        await asyncio.sleep(0.001)
        self.loop_intent = "stop"


    async def intend_none(self):
        await asyncio.sleep(0.001)
        self.loop_intent = None


    async def clear_estate(self, clear_estop=True, clear_error=True):
        if not clear_estop and not clear_error:
            self.print_message("both clear_estop and clear_error parameters are False, nothing to clear")
        cleared_status = copy(self.global_state_dict)
        if clear_estop:
            for serv, action, myuuid in self.estop_uuids:
                self.print_message(f"clearing E-STOP {action} on {serv}")
                cleared_status[serv][action] = cleared_status[serv][action].remove(myuuid)
        if clear_error:
            for serv, action, myuuid in self.error_uuids:
                self.print_message(f"clearing error {action} on {serv}")
                cleared_status[serv][action] = cleared_status[serv][action].remove(myuuid)
        await self.global_q.put(cleared_status)
        self.print_message("resetting dispatch loop state")
        self.loop_state = "stopped"
        self.print_message(
            f"{len(self.running_uuids)} running action_dq did not fully stop after E-STOP/error was raised"
        )


    async def add_sequence(
                           self,
                           sequence_uuid: UUID = None,
                           sequence_timestamp: datetime = None,
                           sequence_name: str = None,
                           sequence_params: dict = None,
                           sequence_label: str = None,
                           process_plan_list: List[dict] = []
                           ):
        seq = Sequence()
        seq.sequence_uuid = sequence_uuid
        seq.sequence_timestamp = sequence_timestamp
        seq.sequence_name = sequence_name
        seq.sequence_params = sequence_params
        seq.sequence_label = sequence_label
        for D_dict in process_plan_list:
            D = Process(D_dict)
            seq.process_plan_list.append(D)
        self.sequence_dq.append(seq)


    async def add_process(
        self,
        seq: ProcessSequenceModel,
        orchestrator: str = None,
        process_name: str = None,
        process_params: dict = {},
        result_dict: dict = {},
        access: str = "hte",
        prepend: Optional[bool] = False,
        at_index: Optional[int] = None,
    ):
        Ddict = {
                "orchestrator": orchestrator,
                "process_name": process_name,
                "process_params": process_params,
                "result_dict": result_dict,
                "access": access,
            }
        Ddict.update(seq.dict())
        D = Process(Ddict)

        # reminder: process_dict values take precedence over keyword args
        if D.orchestrator is None:
            D.orchestrator = self.server_name

        await asyncio.sleep(0.001)
        if at_index:
            self.process_dq.insert(i=at_index, x=D)
        elif prepend:
            self.process_dq.appendleft(D)
            self.print_message(f"process {str(D.process_uuid)} prepended to queue")
        else:
            self.process_dq.append(D)
            self.print_message(f"process {str(D.process_uuid)} appended to queue")


    def list_processes(self):
        """Return the current queue of process_dq."""
        return [process.get_prc() for process in self.process_dq]


    def get_process(self, last=False):
        """Return the active or last process."""
        active_process_list = []
        if last:
            process = self.last_process
        else:
            process = self.active_process
        if process is not None:
            active_process_list.append(process.get_prc())
        return active_process_list


    def list_active_actions(self):
        """Return the current queue running actions."""
        action_list = []
        index = 0
        for action_serv, action_dict in self.global_state_dict.items():
            for action_name, action_uuids in action_dict.items():
                for action_uuid in action_uuids:
                    action_list.append(
                        {"index":index,
                         "action_uuid":action_uuid,
                         "server":action_serv,
                         "action_name":action_name,                         
                        }
                    )
        return action_list


    def list_actions(self):
        """Return the current queue of action_dq."""
        return [action.get_act() for action in self.action_dq]


    def supplement_error_action(self, check_uuid: str, sup_action: Action):
        """Insert action at front of action queue with subversion of errored action, inherit parameters if desired."""
        if self.error_uuids == []:
            self.print_message("There are no error statuses to replace")
        else:
            matching_error = [tup for tup in self.error_uuids if tup[2] == check_uuid]
            if matching_error:
                _, _, error_uuid = matching_error[0]
                EA = [A for _, A in self.dispatched_actions.items() if str(A.action_uuid) == error_uuid][0]
                # sup_action can be a differnt one, 
                # but for now we treat it thats a retry of the errored one
                new_action = sup_action
                new_action.action_order = EA.action_order
                 # will be updated again once its dispatched again
                new_action.actual_order = EA.actual_order
                new_action.action_retry = EA.action_retry + 1
                self.action_dq.appendleft(new_action)
            else:
                self.print_message(f"uuid {check_uuid} not found in list of error statuses:")
                self.print_message(", ".join(self.error_uuids))


    def remove_process(self, by_index: Optional[int] = None, by_uuid: Optional[str] = None):
        """Remove process in list by enumeration index or uuid."""
        if by_index:
            i = by_index
        elif by_uuid:
            i = [i for i, D in enumerate(list(self.process_dq)) if str(D.process_uuid) == by_uuid][0]
        else:
            self.print_message("No arguments given for locating existing process to remove.")
            return None
        del self.process_dq[i]


    def replace_action(
        self,
        sup_action: Action,
        by_index: Optional[int] = None,
        by_uuid: Optional[str] = None,
        by_action_order: Optional[Union[int, float]] = None,
    ):
        """Substitute a queued action."""
        if by_index:
            i = by_index
        elif by_uuid:
            i = [i for i, A in enumerate(list(self.action_dq)) if str(A.action_uuid) == by_uuid][0]
        elif by_action_order:
            i = [i for i, A in enumerate(list(self.action_dq)) if A.action_order == by_action_order][0]
        else:
            self.print_message("No arguments given for locating existing action to replace.")
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
            # last_ordering = floor(max(list(self.dispatched_actions)))
            last_action_order = len(self.dispatched_actions) - 1

            if last_action_order < 0:
                # no action was dispatched yet
                last_action_order = 0
        else:
            last_action_order = floor(self.action_dq[-1].action_order)

        new_action_order = int(last_action_order + 1)
        new_action = sup_action
        new_action.action_order = new_action_order
        self.action_dq.append(new_action)

    
    async def finish_active_sequence(self):
        if self.seq_file is not None:
            self.active_sequence.sequence_status = "finished"
            await self.write_seq(self.active_sequence)
            self.seq_file = None


    async def finish_active_process(self):
        # we need to wait for all actions to finish first
        await self.orch_wait_for_all_actions()
        if self.prc_file is not None:
            self.print_message(f"finished prc uuid is: {self.prc_file.process_uuid}, adding matching acts to it")

            # todo: update here all acts from self.dispatched_actions list
            self.active_process.process_action_uuid_list = []
            self.active_process.process_action_list = []
            self.print_message("getting uuids from all dispatched actions of active process")
            for action_actual_order, dispachted_act in self.dispatched_actions.items():
                self.active_process.process_action_uuid_list.append(dispachted_act.action_uuid)

            self.active_process.process_status = "finished"

            self.print_message("getting all finished actions")
            for act in self.finished_actions:
                actm = ActionModel(**act)
                # getting only "finished" actions updates
                if actm.orchestrator == self.server_name \
                and actm.action_status == "finished" \
                and actm.action_uuid in self.active_process.process_action_uuid_list:
                    self.print_message(f"adding finished action '{actm.action_name}' to process")
                    self.active_process.process_action_list.append(actm)
            
            # add finished prc to seq
            self.active_sequence.processmodel_list.append(deepcopy(self.active_process.get_prc()))
            # write new updated seq
            await self.write_active_sequence_seq()

            # write final prc
            await self.write_prc(self.active_process)


        # set prc to None to signal that its done
        self.prc_file = None
        # set dispatched actions to zero again
        self.dispatched_actions = {}
        self.finished_actions = []


    async def write_active_process_prc(self):
        await self.write_prc(self.active_process)


    async def write_active_sequence_seq(self):
        await self.write_seq(self.active_sequence)


    async def shutdown(self):
        await self.detach_subscribers()
        self.status_logger.cancel()
        self.ntp_syncer.cancel()
        self.status_subscriber.cancel()
