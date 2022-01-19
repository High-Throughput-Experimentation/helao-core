__all__ = [
           "Base",
           "FileConnParams",
           "ActiveParams",
           "make_action_serv"
          ]

import asyncio
import json
import os
import sys
from socket import gethostname
from time import ctime, time, time_ns
from typing import List, Optional, Dict
from uuid import UUID
from fastapi import Request
import hashlib

import aiofiles
import colorama
import ntplib
import numpy as np
import pyaml
from enum import Enum

from pydantic import BaseModel, Field, validator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.openapi.utils import get_flat_params


from .api import HelaoFastAPI
from .dispatcher import async_private_dispatcher
from .setup_action import setup_action

from ..helper.helao_dirs import helao_dirs
from ..helper.multisubscriber_queue import MultisubscriberQueue
from ..helper.print_message import print_message
from ..helper import async_copy
from ..schema import Action
from ..model.hlostatus import HloStatus
from ..model.sample import SampleUnion, NoneSample
from ..model.sample import SampleInheritance, SampleStatus
from ..model.fileinfo import FileInfo
from ..model.data import DataModel, DataPackageModel
from ..helper.file_in_use import file_in_use
from ..helper.helaodict import HelaoDict
from ..version import get_hlo_version


# ANSI color codes converted to the Windows versions
# strip colors if stdout is redirected
colorama.init(strip=not sys.stdout.isatty())
# colorama.init()

class HloFileGroup(str, Enum):
    aux_files = "aux_files"
    helao_files = "helao_files"


class HloHeaderModel(BaseModel, HelaoDict):
    hlo_version: Optional[str] = get_hlo_version()
    action_name: str
    column_headings: List[str] = Field(default_factory=list)


class FileConnParams(BaseModel, HelaoDict):
    # we require a file conn key
    # cannot be uuid 'object' as we might have more then one file
    # either use sample_label, or str(action_uuid) (if only one file etc
    file_conn_key: UUID

    # but samples are optional
    # only need the global label, but not the full sample basemodel
    sample_global_labels: List[str] = Field(default_factory=list)
    json_data_keys: List[str] = Field(default_factory=list)
    # type of file
    file_type: str = "helao__file"
    file_group: Optional[HloFileGroup] = HloFileGroup.helao_files
    # None will trigger autogeneration of a file name
    file_name: Optional[str]
    # the header of the hlo file as dict (will be written as yml)
    header: Optional[Dict] = Field(default_factory=dict)


class FileConn(BaseModel):
    """This is an internal BaseModel for Base which will hold all 
    file connections.
    """
    params: FileConnParams
    # signal if a header was written or not
    finished_hlo_header: bool = False
    added_hlo_separator: bool = False
    # holds the file reference
    file: Optional[object]


class ActiveParams(BaseModel, HelaoDict):
    # the Action object for this action
    action: Action
    # a list of data file connection parameters
    file_conn_params_list: List[FileConnParams] = Field(default_factory=list)
    aux_listen_uuids: List[UUID] = Field(default_factory=list)


    class Config:
        arbitrary_types_allowed = True


    @validator("action")
    def validate_action(cls, v):
        return v


def make_action_serv(config, server_key, server_title, description, version, driver_class=None):
    app = HelaoFastAPI(config, server_key, title=server_title, description=description, version=version)

    @app.on_event("startup")
    def startup_event():
        app.base = Base(app)
        if driver_class:
            app.driver = driver_class(app.base)

    @app.websocket("/ws_status")
    async def websocket_status(websocket: WebSocket):
        """Broadcast status messages.

        Args:
        websocket: a fastapi.WebSocket object
        """
        await app.base.ws_status(websocket)

    @app.websocket("/ws_data")
    async def websocket_data(websocket: WebSocket):
        """Broadcast status dicts.

        Args:
        websocket: a fastapi.WebSocket object
        """
        await app.base.ws_data(websocket)

    @app.post("/get_status")
    def status_wrapper():
        return app.base.status

    @app.post("/attach_client")
    async def attach_client(client_servkey: str):
        return await app.base.attach_client(client_servkey)

    @app.post("/endpoints")
    def get_all_urls():
        """Return a list of all endpoints on this server."""
        return app.base.get_endpoint_urls(app)

    # @app.post(f"/split")
    # async def split(target_uuid: Optional[UUID],
    #                 new_samples_in: Optional[List[SampleUnion]],
    #                 new_params: Optional[dict] = None,
    #                 scratch: Optional[List[None]] = [None], # temp fix so swagger still works 
    #                 ):
    #     return app.base.split(target_uuid, new_samples_in, new_params)

    return app



class Base(object):
    """Base class for all HELAO servers.

    Base is a general class which implements message passing, 
    status update, data writing, and data streaming via async tasks. 
    Every instrument and action server should import this class 
    for efficient integration into an orchestrated environment.

    A Base initialized within a FastAPI startup event 
    will launch three async tasks to the server's event loop for handling:
    (1) broadcasting status updates via websocket and 
        http POST requests to an attached
        orchestrator's status updater if available,
    (2) data streaming via websocket,
    (3) data writing to local disk.

    Websocket connections are broadcast from a multisubscriber queue 
    in order to handle consumption from multiple clients 
    awaiting a single queue. Self-subscriber tasks are
    also created as initial subscribers 
    to log all events and prevent queue overflow.

    The data writing method will update a class attribute 
    with the currently open file.
    For a given root directory, files 
    and folders will be written as follows:
        TBD
    """

    def __init__(self, fastapp: HelaoFastAPI, calibration: dict = {}):
        self.server_name = fastapp.helao_srv
        self.server_cfg = fastapp.helao_cfg["servers"][self.server_name]
        self.server_params = \
            fastapp.helao_cfg["servers"][self.server_name].\
            get("params", dict())
        self.world_cfg = fastapp.helao_cfg
        self.hostname = gethostname()
        self.technique_name = None
        self.aloop = asyncio.get_running_loop()

        self.root, \
        self.save_root, \
        self.log_root, \
        self.states_root, \
        self.db_root = helao_dirs(self.world_cfg)

        if self.root is None:
            raise ValueError(
                "Warning: root directory was not defined. "
                "Logs, PRCs, PRGs, and data will not be written.",
                error=True,
            )

        if "technique_name" in self.world_cfg:
            self.print_message(
                f"Found technique_name in config: "
                f"{self.world_cfg['technique_name']}",
            )
            self.technique_name = self.world_cfg["technique_name"]
        else:
            raise ValueError(
                "Missing 'technique_name' in config, "
                "cannot create server object.",
                error=True,
            )

        self.calibration = calibration
        self.actives = {}
        self.status = {}
        self.endpoints = []
        self.status_q = MultisubscriberQueue()
        self.data_q = MultisubscriberQueue()
        self.status_clients = set()
        self.ntp_server = "time.nist.gov"
        self.ntp_response = None
        self.ntp_offset = None  # add to system time for correction
        self.ntp_last_sync = None

        self.ntp_last_sync_file = None
        if self.root is not None:
            self.ntp_last_sync_file = os.path.join(
                                                   self.states_root, 
                                                   "ntpLastSync.txt"
                                                  )
            if os.path.exists(self.ntp_last_sync_file):
                with open(self.ntp_last_sync_file, "r") as f:
                    tmps = f.readline().strip().split(",")
                    if len(tmps) == 2:
                        self.ntp_last_sync, self.ntp_offset = tmps
                        self.ntp_offset = float(self.ntp_offset)

        if self.ntp_last_sync is None:
            asyncio.gather(self.get_ntp_time())
        self.init_endpoint_status(fastapp)
        self.fast_urls = self.get_endpoint_urls(fastapp)
        self.status_logger = self.aloop.create_task(self.log_status_task())
        self.ntp_syncer = self.aloop.create_task(self.sync_ntp_task())


    def print_message(self, *args, **kwargs):
        print_message(
                      self.server_cfg, 
                      self.server_name, 
                      log_dir=self.log_root, 
                      *args, 
                      **kwargs
                     )


    def init_endpoint_status(self, app: FastAPI):
        """Populate status dict 
           with FastAPI server endpoints for monitoring."""
        for route in app.routes:
            if route.path.startswith(f"/{self.server_name}"):
                self.status[route.name] = []
                self.endpoints.append(route.name)
        self.print_message(f"Found {len(self.status)} endpoints "
                           f"for status monitoring on {self.server_name}.")


    def get_endpoint_urls(self, app: HelaoFastAPI):
        """Return a list of all endpoints on this server."""
        url_list = []
        for route in app.routes:
            routeD = {"path": route.path, "name": route.name}
            if "dependant" in dir(route):
                flatParams = get_flat_params(route.dependant)
                paramD = {
                    par.name: {
                        "outer_type": str(par.outer_type_).split("'")[1]
                        if len(str(par.outer_type_).split("'")) >= 2
                        else str(par.outer_type_),
                        "type": str(par.type_).split("'")[1]
                        if len(str(par.type_).split("'")) >= 2
                        else str(par.type_),
                        "required": par.required,
                        "shape": par.shape,
                        "default": par.default,
                    }
                    for par in flatParams
                }
                routeD["params"] = paramD
            else:
                routeD["params"] = []
            url_list.append(routeD)
        return url_list


    async def setup_and_contain_action(
                                       self, 
                                       request: Request,
                                       json_data_keys: List[str],
                                       action_abbr: Optional[str] = None
                                      ) -> object:
        """This is a simple shortcut for very basic endpoints
        which just want to return some simple data"""
        A = await setup_action(request)
        if action_abbr is not None:
            A.action_abbr = action_abbr
        active = await self.contain_action(ActiveParams(
            action = A,
            file_conn_params_list = [FileConnParams(
                                    file_conn_key = self.dflt_file_conn_key(),
                                    json_data_keys=json_data_keys
                                    )]))
        return active


    async def contain_action(
        self,
        activeparams: ActiveParams
    ) -> object:
        """return an active Action:
            file_type: type of output data file
            json_data_keys: data keys for json encoded data (dict)
            file_sample_label: list of sample labels  
            file_conn_keys: 
            header: header for data file
        """
        self.actives[str(activeparams.action.action_uuid)] = Base.Active(
            self,
            activeparams = activeparams
        )
        await self.actives[str(activeparams.action.action_uuid)].myinit()
        return self.actives[str(activeparams.action.action_uuid)]


    async def get_active_info(self, action_uuid: UUID):
        if action_uuid in self.actives:
            action_dict = \
                await self.actives[str(action_uuid)].active.as_dict()
            return action_dict
        else:
            self.print_message(
                               f"Specified action uuid "
                               f"{str(action_uuid)} was not found.", 
                               error=True
                              )
            return None


    async def get_ntp_time(self):
        "Check system clock against NIST clock for trigger operations."
        lock = asyncio.Lock()
        async with lock:
            c = ntplib.NTPClient()
            try:
                response = c.request(self.ntp_server, version=3)
                self.ntp_response = response
                self.ntp_last_sync = response.orig_time
                self.ntp_offset = response.offset
                self.print_message(
                    f"retrieved time at {ctime(self.ntp_response.tx_timestamp)} from {self.ntp_server}",
                )
            except ntplib.NTPException:
                self.print_message(f"{self.ntp_server} ntp timeout", error=True)
                self.ntp_last_sync = time()
                self.ntp_offset = 0.0

            self.print_message(f"ntp_offset: {self.ntp_offset}")
            self.print_message(f"ntp_last_sync: {self.ntp_last_sync}")

            if self.ntp_last_sync_file is not None:
                while file_in_use(self.ntp_last_sync_file):
                    self._base.print_message("ntp file already in use, waiting", info=True)
                    await asyncio.sleep(0.1)
                async with aiofiles.open(self.ntp_last_sync_file, "w") as f:
                    await f.write(f"{self.ntp_last_sync},{self.ntp_offset}")

    async def attach_client(self, client_servkey: str, retry_limit=5):
        "Add client for pushing status updates via HTTP POST."
        success = False

        if client_servkey in self.world_cfg["servers"]:

            if client_servkey in self.status_clients:
                self.print_message(
                    f"Client {client_servkey} is already subscribed to {self.server_name} status updates."
                )
            else:
                self.status_clients.add(client_servkey)

                current_status = self.status
                for _ in range(retry_limit):
                    response = await async_private_dispatcher(
                        world_config_dict=self.world_cfg,
                        server=client_servkey,
                        private_action="update_status",
                        params_dict={
                            "server": self.server_name,
                            "status": json.dumps(current_status),
                            # "act": json.dumps(ActionModel().as_dict())
                        },
                        json_dict={},
                    )
                    if response == True:
                        self.print_message(
                            f"Added {client_servkey} to {self.server_name} status subscriber list."
                        )
                        success = True
                        break
                    else:
                        self.print_message(
                            f"Failed to add {client_servkey} to {self.server_name} status subscriber list.",
                            error=True,
                        )

            if success:
                self.print_message(
                    f"Updated {self.server_name} status to {current_status} on {client_servkey}."
                )
            else:
                self.print_message(
                    f"Failed to push status message to {client_servkey} after {retry_limit} attempts.",
                    error=True,
                )

        return success

    def detach_client(self, client_servkey: str):
        "Remove client from receiving status updates via HTTP POST"
        if client_servkey in self.status_clients:
            self.status_clients.remove(client_servkey)
            self.print_message(f"Client {client_servkey} will no longer receive status updates.")
        else:
            self.print_message(f"Client {client_servkey} is not subscribed.")

    async def ws_status(self, websocket: WebSocket):
        "Subscribe to status queue and send message to websocket client."
        self.print_message("got new status subscriber")
        await websocket.accept()
        try:
            async for status_msg in self.status_q.subscribe():
                await websocket.send_text(json.dumps(status_msg))
        except WebSocketDisconnect:
            self.print_message(
                f"Status websocket client {websocket.client[0]}:{websocket.client[1]} disconnected.",
                error=True,
            )

    async def ws_data(self, websocket: WebSocket):
        "Subscribe to data queue and send messages to websocket client."
        self.print_message("got new data subscriber")
        await websocket.accept()
        try:
            async for data_msg in self.data_q.subscribe():
                await websocket.send_text(json.dumps(data_msg))
        except WebSocketDisconnect:
            self.print_message(
                f"Data websocket client {websocket.client[0]}:{websocket.client[1]} disconnected.",
                error=True,
            )

    async def log_status_task(self, retry_limit: int = 5):
        "Self-subscribe to status queue, log status changes, POST to clients."
        self.print_message(f"{self.server_name} status log task created.")

        try:
            self.print_message("log_status_task started.")
            async for status_msg in self.status_q.subscribe():
                self.status.update(status_msg)
                self.print_message(
                    f"log_status_task sending message {status_msg} to subscribers ({self.status_clients})."
                )
                for client_servkey in self.status_clients:

                    self.print_message(
                        f"log_status_task sending to {client_servkey}: {json.dumps(status_msg)}."
                    )
                    success = False

                    for _ in range(retry_limit):

                        response = await async_private_dispatcher(
                            world_config_dict=self.world_cfg,
                            server=client_servkey,
                            private_action="update_status",
                            params_dict={
                                "server": self.server_name,
                                "status": json.dumps(status_msg),
                                # "act": json.dumps(ActionModel().as_dict())
                            },
                            json_dict={},
                        )
                        if response == True:
                            self.print_message(f"send status msg to {client_servkey}.")
                            success = True
                            break
                        else:
                            self.print_message(f"Failed to send status msg {client_servkey}.")

                    if success:
                        self.print_message(
                            f"Updated {self.server_name} status to {status_msg} on {client_servkey}."
                        )
                    else:
                        self.print_message(
                            f"Failed to push status message to {client_servkey} after {retry_limit} attempts."
                        )

                    # TODO:write to log if save_root exists
                self.print_message("log_status_task message send.")

            self.print_message("log_status_task done.")

        # except asyncio.CancelledError:
        except Exception as e:
            self.print_message("status logger task was cancelled "
                               f"with error: {e}", error=True)


    async def detach_subscribers(self):
        await self.status_q.put(StopAsyncIteration)
        await self.data_q.put(StopAsyncIteration)
        await asyncio.sleep(5)


    async def set_realtime(
                           self, 
                           epoch_ns: Optional[float] = None, 
                           offset: Optional[float] = None
                          ) -> float:
        return self.set_realtime_nowait(epoch_ns=epoch_ns, offset=offset)


    def set_realtime_nowait(
                            self, 
                            epoch_ns: Optional[float] = None, 
                            offset: Optional[float] = None
                           ) -> float:
        if offset is None:
            if self.ntp_offset is not None:
                offset_ns = int(np.floor(self.ntp_offset * 1e9))
            else:
                offset_ns = 0.0
        else:
            offset_ns = int(np.floor(offset * 1e9))
        if epoch_ns is None:
            real_time = time_ns() + offset_ns
        else:
            real_time = epoch_ns + offset_ns
        return real_time


    async def sync_ntp_task(self, resync_time: int = 600):
        "Regularly sync with NTP server."
        try:
            while True:
                await asyncio.sleep(10)
                lock = asyncio.Lock()
                async with lock:
                    ntp_last_sync = ""
                    if self.ntp_last_sync_file is not None:
                        async with aiofiles.open(self.ntp_last_sync_file, "r") as f:
                            ntp_last_sync = await f.readline()
                    parts = ntp_last_sync.strip().split(",")
                    if len(parts) == 2:
                        self.ntp_last_sync = float(parts[0])
                        self.ntp_offset = float(parts[1])
                    else:
                        self.ntp_last_sync = float(parts[0])
                        self.ntp_offset = 0.0
                    if time() - self.ntp_last_sync > resync_time:
                        self.print_message(
                            f"last time check was more then { resync_time} ago, syncing time again.",
                        )
                        await self.get_ntp_time()
                    else:
                        # wait_time = time() - self.ntp_last_sync
                        wait_time = resync_time
                        self.print_message(f"waiting {wait_time} until next time check")
                        await asyncio.sleep(wait_time)
        except asyncio.CancelledError:
            self.print_message("ntp sync task was cancelled", info=True)


    async def shutdown(self):
        await self.detach_subscribers()
        self.status_logger.cancel()
        self.ntp_syncer.cancel()


    async def write_act(self, action):
        "Create new prc if it doesn't exist."
        if action.save_act:
            act_dict = action.get_act().clean_dict()
            output_path = os.path.join(self.save_root,action.output_dir)
            output_file = os.path.join(
                                       output_path, 
                                       f"{action.action_timestamp.strftime('%Y%m%d.%H%M%S%f')}.yml")
    
            self.print_message(f"writing to act meta file: {output_path}")

            if not os.path.exists(output_path):
                os.makedirs(output_path, exist_ok=True)

            async with aiofiles.open(output_file, mode="w+") as f:
                await f.write(pyaml.dump({"file_type": "action"}))
                await f.write(pyaml.dump(act_dict, sort_dicts=False))
        else:
            self.print_message(f"writing meta file for action '{action.action_name}' is disabled.", info=True)


    async def write_prc(self, experiment):
        prc_dict = experiment.get_prc().clean_dict()
        output_path = os.path.join(
                                   self.save_root, 
                                   self.get_experiment_dir(experiment)
                                  )
        output_file = os.path.join(output_path, f"{experiment.experiment_timestamp.strftime('%Y%m%d.%H%M%S%f')}.yml")

        self.print_message(f"writing to prc meta file: {output_file}")
        output_str = pyaml.dump(prc_dict, sort_dicts=False)

        if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)

        async with aiofiles.open(output_file, mode="w+") as f:
            await f.write(pyaml.dump({"file_type": "experiment"}))
            if not output_str.endswith("\n"):
                output_str += "\n"
            await f.write(output_str)


    async def write_seq(self, sequence):
        seq_dict = sequence.get_seq().clean_dict()
        sequence_dir = self.get_sequence_dir(sequence)
        output_path = os.path.join(self.save_root, sequence_dir)
        output_file = os.path.join(output_path, f"{sequence.sequence_timestamp.strftime('%Y%m%d.%H%M%S%f')}.yml")

        self.print_message(f"writing to seq meta file: {output_file}")
        output_str = pyaml.dump(seq_dict, sort_dicts=False)

        if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)

        async with aiofiles.open(output_file, mode="w+") as f:
            await f.write(pyaml.dump({"file_type": "sequence"}))
            if not output_str.endswith("\n"):
                output_str += "\n"
            await f.write(output_str)

    def get_sequence_dir(self, sequence):
        HMS = sequence.sequence_timestamp.strftime("%H%M%S")
        year_week = sequence.sequence_timestamp.strftime("%y.%U")
        sequence_date = sequence.sequence_timestamp.strftime("%Y%m%d")

        return os.path.join(
            year_week,
            sequence_date,
            f"{HMS}__{sequence.sequence_name}__{sequence.sequence_label}",
        )


    def get_experiment_dir(self, experiment):
        """accepts action or experiment object"""
        experiment_time = experiment.experiment_timestamp.strftime("%H%M%S%f")
        sequence_dir = self.get_sequence_dir(experiment)
        return os.path.join(
            sequence_dir,
            f"{experiment_time}__{experiment.experiment_name}",
        )


    def get_action_dir(self, action):
        experiment_dir = self.get_experiment_dir(action)
        return os.path.join(
            experiment_dir,
            f"{action.action_actual_order}__{action.action_timestamp.strftime('%Y%m%d.%H%M%S%f')}__{action.action_server_name}__{action.action_name}",
        )


    def new_file_conn_key(self, key: str) -> UUID:
        # return shortuuid.decode(key)
        # Instansiate new md5_hash
        md5_hash = hashlib.md5()
        # Pass the_string to the md5_hash as bytes
        md5_hash.update(key.encode("utf-8"))
        # Generate the hex md5 hash of all the read bytes
        the_md5_hex_str = md5_hash.hexdigest()
        # Return a String repersenation of the uuid of the md5 hash
        return UUID(the_md5_hex_str)


    def dflt_file_conn_key(self):
        """simply return a default None"""
        return self.new_file_conn_key(str(None))

    def replace_status(
                       self, 
                       status_list: List[HloStatus], 
                       old_status: HloStatus, 
                       new_status: HloStatus
                      ):
        if old_status in status_list:
            idx = status_list.index(old_status)
            status_list[idx] = new_status
        else:
            status_list.append(new_status)


    class Active(object):
        """Active action holder which wraps data queing and prc writing."""

        def __init__(
            self,
            base,  # outer instance
            activeparams: ActiveParams

        ):
            self.base = base
            self.action = activeparams.action
            self.listen_uuids = []
            
            
            # better call this function instead of directly adding it
            # in case we modify the way the uuids are saved
            # self.add_new_listen_uuid(self.action.action_uuid)
            # action_uuid is added after action is init
            for aux_uuid in activeparams.aux_listen_uuids:
                self.add_new_listen_uuid(aux_uuid)
                


            self.action.action_status = [HloStatus.active]
            self.manual = False

            # signals the data logger that it got data 
            # and hlo header was written or not
            # active.finish_hlo_header should be called 
            # within the driver before
            # any data is pushed to avoid a forced header end write
            self.finished_hlo_header = dict()
            self.file_conn: Dict(str, FileConn) = dict()
            for file_conn_param in activeparams.file_conn_params_list:
                self.file_conn[file_conn_param.file_conn_key] = \
                    FileConn(params = file_conn_param)


            # need to remove swagger workaround value if present
            if "scratch" in self.action.action_params:
                del self.action.action_params["scratch"]

            # this updates timestamp and uuid
            # only if they are None
            # They are None in manual, but already set in orch mode
            self.action.init_act(
                                 machine_name=self.base.hostname, 
                                 time_offset=self.base.ntp_offset
                                )
            self.add_new_listen_uuid(self.action.action_uuid)
            # if manual:
            self.action.action_server_name = self.base.server_name


            # check if its swagger submission
            if self.action.sequence_timestamp is None \
            or self.action.experiment_timestamp is None:
                self.manual = True
                self.base.print_message("Manual Action.", info=True)
                self.action.access = "manual"

                # -- (1) -- set missing sequence parameters
                self.action.sequence_name = "manual_swagger_seq"
                self.action.init_seq(
                                     machine_name=self.base.ntp_offset, 
                                     time_offset=self.base.ntp_offset
                                    )
                self.action.sequence_output_dir = \
                    self.base.get_sequence_dir(self.action)
                self.action.sequence_status = [HloStatus.active]

                # -- (2) -- set missing experiment parameters
                self.action.experiment_name = "MANUAL"
                self.action.set_dtime(offset=self.base.ntp_offset)
                self.action.gen_uuid_experiment(self.base.hostname)
                self.action.experiment_output_dir = \
                    self.base.get_experiment_dir(self.action)
                self.action.experiment_status = [HloStatus.finished]
                # these are set in setup_action
                # self.action.technique_name = "MANUAL"
                # self.action.orchestrator = "MANUAL"
                # machine name is set above

            if not self.base.save_root:
                self.base.print_message("Root save directory not specified, "
                                        "cannot save action results.")
                self.action.save_data = False
                self.action.save_act = False
                self.action.output_dir = None
            else:
                if self.action.save_data is None:
                    self.action.save_data = False
                if self.action.save_act is None:
                    self.action.save_act = False
                # cannot save data without prc
                if self.action.save_data is True:
                    self.action.save_act = True

                self.action.output_dir = self.base.get_action_dir(self.action)

            self.base.print_message(
                f"save_act is '{self.action.save_act}' for action '{self.action.action_name}'", info=True
            )
            self.base.print_message(
                f"save_data is '{self.action.save_data}' for action '{self.action.action_name}'", info=True
            )

            self.data_logger = self.base.aloop.create_task(
                self.log_data_task()
            )


        async def update_act_file(self):
            await self.base.write_act(self.action)


        async def myinit(self):

            if self.action.save_act:
                os.makedirs(
                    os.path.join(self.base.save_root, self.action.output_dir),
                    exist_ok=True,
                )
                await self.update_act_file()
    
                if self.manual:
                    # create and write seq file for manual action
                    await self.base.write_seq(self.action)
                    # create and write prc file for manual action
                    await self.base.write_prc(self.action)


            await self.add_status()


        def init_datafile(
            self,
            header,
            file_type,
            json_data_keys,
            file_sample_label,
            filename,
            file_group: HloFileGroup,
            file_conn_key: Optional[str] = None,
        ):
            filenum = 0
            if file_conn_key in self.file_conn:
                # filenum = self.action.file_conn_keys.index(file_conn_key)
                # filenum = self.file_conn[file_conn_key].file_num
                filenum = list(self.file_conn.keys()).index(file_conn_key)

            if isinstance(header, dict):
                # {} is "{}\n" if not filtered
                if header:
                    header = pyaml.dump(header, sort_dicts=False)
                else:
                    header = ""
            elif isinstance(header, list):
                if header:
                    header = "\n".join(header) + "\n"
                else:
                    header = ""
            elif header is None:
                header = ""

            if json_data_keys is None:
                json_data_keys = []

            if filename is None:  # generate filename
                file_ext = "csv"
                if file_group == HloFileGroup.helao_files:
                    file_ext = "hlo"
                    header_dict = HloHeaderModel(
                        action_name = self.action.action_abbr 
                        if self.action.action_abbr is not None
                        else self.action.action_name,
                        column_headings = json_data_keys
                    ).as_dict()
                    header = pyaml.dump(header_dict, sort_dicts=False) + header
                else:  # aux_files
                    pass

                filename = f"{self.action.action_abbr}-{self.action.action_actual_order}.{self.action.action_order}.{self.action.action_retry}.{self.action.action_split}__{filenum}.{file_ext}"

            if file_sample_label is None:
                file_sample_label = []
            if not isinstance(file_sample_label, list):
                file_sample_label = [file_sample_label]

            file_info = FileInfo(
                file_type=file_type,
                file_name=filename,
                data_keys=json_data_keys,
                sample=file_sample_label,
                # action_uuid: Optional[UUID]
            )

            if header:
                if not header.endswith("\n"):
                    header += "\n"

            return header, file_info


        def finish_hlo_header(
                              self, 
                              file_conn_keys: List[UUID],
                              realtime: Optional[int] = None
                             ):
            """this just adds a timestamp for the data"""
            # needs to be a sync function
            if realtime == None:
                realtime = self.set_realtime_nowait()

            for file_conn_key in file_conn_keys:
                self.file_conn[file_conn_key].params.header["epoch_ns1"] = realtime
                self.file_conn[file_conn_key].finished_hlo_header = True


        async def add_status(self):
            self.base.status[self.action.action_name].append(str(self.action.action_uuid))
            self.base.print_message(
                f"Added {str(self.action.action_uuid)} to {self.action.action_name} status list."
            )
            await self.base.status_q.put(
                {
                    self.action.action_name: self.base.status[self.action.action_name],
                    "act": self.action.get_act().as_dict(),
                }
            )

        async def clear_status(self, clear_uuid: Optional[UUID]=None):
            if clear_uuid is None:
                clear_uuid = self.action.action_uuid
            if str(clear_uuid) in self.base.status[self.action.action_name]:
                self.base.status[self.action.action_name].remove(str(clear_uuid))
                self.base.print_message(
                    f"Removed {str(clear_uuid)} from {self.action.action_name} status list.",
                    info=True,
                )
            else:
                self.base.print_message(
                    f"{str(clear_uuid)} did not exist in {self.action.action_name} status list.",
                    error=True,
                )
            await self.base.status_q.put(
                {
                    self.action.action_name: self.base.status[self.action.action_name],
                    "act": self.action.get_act().as_dict(),
                }
            )


        async def set_estop(self):
            self.base.status[self.action.action_name]\
                .remove(str(self.action.action_uuid))
            self.base.status[self.action.action_name]\
                .append(f"{str(self.action.action_uuid)}__estop")
            self.base.print_message(
                f"E-STOP {str(self.action.action_uuid)} on "
                f"{self.action.action_name} status.",
                error=True,
            )
            await self.base.status_q.put(
                {
                    self.action.action_name: \
                        self.base.status[self.action.action_name],
                    "act": self.action.get_act().as_dict(),
                }
            )


        async def set_error(
                            self, 
                            err_msg: Optional[str] = None
                           ):
            self.base.status[self.action.action_name].\
                remove(str(self.action.action_uuid))
            self.base.status[self.action.action_name].\
                append(f"{str(self.action.action_uuid)}__error")
            self.base.print_message(
                f"ERROR {str(self.action.action_uuid)} on "
                f"{self.action.action_name} status.",
                error=True,
            )
            if err_msg:
                self.action.error_code = err_msg
            else:
                self.action.error_code = "-1 unspecified error"
            await self.base.status_q.put(
                {
                    self.action.action_name: \
                        self.base.status[self.action.action_name],
                    "act": self.action.get_act().as_dict(),
                }
            )


        async def set_realtime(
                               self, 
                               epoch_ns: Optional[float] = None, 
                               offset: Optional[float] = None
                              ) -> float:
            return self.base.set_realtime_nowait(
                                                 epoch_ns=epoch_ns, 
                                                 offset=offset
                                                )


        def set_realtime_nowait(
                                self, 
                                epoch_ns: Optional[float] = None, 
                                offset: Optional[float] = None
                               ) -> float:
            return self.base.set_realtime_nowait(
                                                 epoch_ns=epoch_ns, 
                                                 offset=offset
                                                )


        async def key_to_fileconn(self, file_conn_key: str):
            """gets the FileConn Basemodel for a given file_conn_key"""
            pass
        

        async def set_output_file(self, file_conn_key: UUID):
            "Set active save_path, write header if supplied."

            self.base.print_message(f"creating file for file conn: "
                                    f"{file_conn_key}")
            header, file_info = self.init_datafile(
                header=self.file_conn[file_conn_key].params.header,
                file_type=self.file_conn[file_conn_key].params.file_type,
                json_data_keys=self.file_conn[file_conn_key].params.json_data_keys,
                file_sample_label=self.file_conn[file_conn_key].params.sample_global_labels,
                filename=None,  # always autogen a filename
                file_group=self.file_conn[file_conn_key].params.file_group,
                file_conn_key=file_conn_key,
            )

            if self.action.file_dict is None:
                self.action.file_dict = []
            self.action.file_dict.append(file_info)
            filename = file_info.file_name

            output_path = os.path.join(
                                       self.base.save_root, 
                                       self.action.output_dir, filename
                                      )
            self.base.print_message(f"writing data to: {output_path}")
            # create output file and set connection
            self.file_conn[file_conn_key].file = \
                await aiofiles.open(output_path, mode="a+")

            if header:
                self.base.print_message("adding header to new file")
                if not header.endswith("\n"):
                    header += "\n"
                await self.file_conn[file_conn_key].file.write(header)


        async def write_live_data(
                                  self, 
                                  output_str: str, 
                                  file_conn_key: UUID
                                 ):
            """Appends lines to file_conn."""
            if file_conn_key in self.file_conn:
                if self.file_conn[file_conn_key].file:
                    if not output_str.endswith("\n"):
                        output_str += "\n"
                    await self.file_conn[file_conn_key].file.write(output_str)


        async def enqueue_data_dflt(
                                   self, 
                                   datadict: dict
                                   ):
            """This is a simple wrapper for simple endpoints which just
            push data to a single file using a default data conn key
            """
            await self.enqueue_data(
                datamodel = DataModel(
                    data = {self.base.dflt_file_conn_key():datadict}
                                     )
            )


        async def enqueue_data(
                               self, 
                               datamodel: DataModel
                              ):
            await self.base.data_q.put(
                self.assemble_data_msg(
                    datamodel = datamodel
                )
            )


        def enqueue_data_nowait(
                                self, 
                                datamodel: DataModel
                               ):
            self.base.data_q.put_nowait(
                self.assemble_data_msg(
                    datamodel = datamodel
                )
            )


        def assemble_data_msg(
                              self, 
                              datamodel: DataModel
                             ) -> DataPackageModel:
            return DataPackageModel(
                action_uuid = self.action.action_uuid,
                action_name = self.action.action_name,
                datamodel = datamodel,
                errors = datamodel.errors
            )


        def add_new_listen_uuid(self, new_uuid: UUID):
            """adds a new uuid to the current data logger UUID list"""
            self.listen_uuids.append(new_uuid)


        async def log_data_task(self):
            """Self-subscribe to data queue, write to present file path."""
            if not self.action.save_data:
                self.base.print_message("data writing disabled")
                return


            self.base.print_message("starting data logger")

            try:
                async for data_msg in self.base.data_q.subscribe():
                    # check if the new data_msg is in listen_uuids
                    if data_msg.action_uuid not in self.listen_uuids:
                        self.base.print_message("UUID is not in listen_uuids",
                                                error = True)
                        continue
                    # self.base.print_message("UUID is in listen_uuids",
                    #                             info = True)

                    data_dict = data_msg.datamodel.data

                    # todo: do we still need this?
                    # self.action.data.append(data_dict)


                    for file_conn_key, sample_data in data_dict.items():
                        if not file_conn_key in self.file_conn:
                            if self.action.save_data:
                                self.base.print_message(
                                    f"'{file_conn_key}' does not exist in "
                                    "file_conn '{self.file_conn}'.",
                                    error=True,
                                )
                            else:
                                # got data but saving is disabled,
                                # e.g. no file was created,
                                # e.g. file_conn_key is not in self.file_conn
                                self.base.print_message(
                                    "data logging is disabled for action "
                                    f"'{self.action.action_name}'",
                                    info=True,
                                )
                            
                            continue


                        # check if we need to create the file first
                        if self.file_conn[file_conn_key].file is None:
                            self.base.print_message("creating output file")
                            # create the file for this data stream
                            await self.set_output_file(
                                file_conn_key=file_conn_key
                            )
                        # else:
                        #     self.base.print_message("output file already exists")

                        # write only data if the file connection is open
                        if self.file_conn[file_conn_key].file:
                            # check if end of hlo header was writen
                            # else force it here
                            # e.g. just write the separator
                            # if not self.finished_hlo_header[file_conn_key]:
                            if not self.file_conn[file_conn_key].finished_hlo_header:
                                self.base.print_message(
                                    f"{self.action.action_abbr} data file "
                                    f"{file_conn_key} is missing hlo "
                                    "separator. Writing it.",
                                    error=True,
                                )
                                self.file_conn[file_conn_key].\
                                    finished_hlo_header = True
                                # TODO: add also hlo header (BaseModel)
                                await self.write_live_data(
                                    output_str=pyaml.dump({"epoch_ns": self.set_realtime_nowait()}),
                                    file_conn_key=file_conn_key,
                                )

                            if not self.file_conn[file_conn_key].added_hlo_separator:
                                self.file_conn[file_conn_key].\
                                    added_hlo_separator = True
                                await self.write_live_data(
                                    output_str="%%\n",
                                    file_conn_key=file_conn_key,
                                )


                            
                            if type(sample_data) is dict:
                                try:
                                    output_str = json.dumps(sample_data)
                                except TypeError:
                                    self.base.print_message(
                                        "Data is not json serializable.",
                                        error=True,
                                    )
                                    output_str = "Error: data was not serializable."
                                await self.write_live_data(
                                    output_str=output_str,
                                    file_conn_key=file_conn_key,
                                )
                            else:
                                await self.write_live_data(
                                    output_str=sample_data, 
                                    file_conn_key=file_conn_key
                                )
                        else:
                            self.base.print_message("output file closed?", 
                                                    error = True)


            except asyncio.CancelledError:
                self.base.print_message("data logger task was cancelled", info=True)


        async def write_file(
                             self,
                             output_str: str,
                             file_type: str,
                             filename: Optional[str] = None,
                             file_group: Optional[HloFileGroup] = HloFileGroup.aux_files,
                             header: Optional[str] = None,
                             sample_str: Optional[str] = None,
                             file_sample_label: Optional[str] = None,
                             json_data_keys: Optional[str] = None,
                            ):
            """Write complete file, not used with queue streaming."""
            if self.action.save_data:
                header, file_info = self.init_datafile(
                    header=header,
                    file_type=file_type,
                    json_data_keys=json_data_keys,
                    file_sample_label=file_sample_label,
                    filename=filename,
                    file_group=file_group,
                )
                output_path = os.path.join(
                                           self.base.save_root, 
                                           self.action.output_dir, 
                                           file_info.file_name
                                          )
                self.base.print_message("writing non stream data to: "
                                        f"{output_path}")

                async with aiofiles.open(output_path, mode="w") as f:
                    await f.write(header + output_str)
                    if self.action.file_dict is None:
                        self.action.file_dict = []
                    self.action.file_dict.append(file_info)
                    return output_path
            else:
                return None


        def write_file_nowait(
                              self,
                              output_str: str,
                              file_type: str,
                              filename: Optional[str] = None,
                              file_group: Optional[HloFileGroup] = HloFileGroup.aux_files,
                              header: Optional[str] = None,
                              sample_str: Optional[str] = None,
                              file_sample_label: Optional[str] = None,
                              json_data_keys: Optional[str] = None,
                             ):
            """Write complete file, not used with queue streaming."""
            if self.action.save_data:
                header, file_info = self.init_datafile(
                    header=header,
                    file_type=file_type,
                    json_data_keys=json_data_keys,
                    file_sample_label=file_sample_label,
                    filename=filename,
                    file_group=file_group,
                )
                output_path = os.path.join(
                                           self.base.save_root, 
                                           self.action.output_dir, 
                                           file_info.file_name
                                          )
                self.base.print_message("writing non stream data to: "
                                        f"{output_path}")
                with open(output_path, mode="w") as f:
                    f.write(header + output_str)
                    if self.action.file_dict is None:
                        self.action.file_dict = []
                    self.action.file_dict.append(file_info)
                    return output_path
            else:
                return None


        async def append_sample(
                                self, 
                                samples: List[SampleUnion], 
                                IO: str
                               ):
            """Add sample to samples_out and samples_in dict"""
            # check if samples is empty
            if not samples:
                return

            for sample in samples:

                # skip NoneSamples
                if isinstance(sample, NoneSample):
                    continue

                if sample.inheritance is None:
                    self.base.print_message("sample.inheritance is None. "
                                            "Using 'allow_both'.")
                    sample.inheritance = SampleInheritance.allow_both

                if not sample.status:
                    self.base.print_message("sample.status is None. Using "
                                            f"'{SampleStatus.preserved}'.")
                    sample.status = [SampleStatus.preserved]

                if IO == "in":
                    if self.action.samples_in is None:
                        self.action.samples_in = []
                    self.action.samples_in.append(sample)
                elif IO == "out":
                    if self.action.samples_out is None:
                        self.action.samples_out
                    self.action.samples_out.append(sample)


        async def split(
                        self,
                        target_uuid: Optional[UUID],
                        new_samples_in: Optional[List[SampleUnion]],
                        new_params: Optional[dict] = None,
                       ) -> Action:
            """Splits active action by writing yml of parent action 
            and mutating self with split action parameters."""
            if len(self.base.actives)==0:
                self.base.print_message("There is no active action. "
                                        "Cannot split.")
                return False

            # when target_uuid is specified, 
            # check that it exists in base.actives
            elif target_uuid is not None \
            and target_uuid not in self.base.actives.keys():
                self.base.print_message("target_uuid not found "
                                        "in list of actives. Cannot split.")
                return False
            else:
            
                await asyncio.sleep(1)

                # pseudo-finish current active action
                self.action.action_status.append(HloStatus.split)
                old_uuid = self.action.action_uuid
                await self.base.write_act(self.action)

                # generate new UUID
                self.action.action_uuid = 'TODO'
                # increment action.action_split
                self.action.action_split+=1
                # replace params
                if new_samples_in:
                    self.action.samples_in = new_samples_in
                # replace samples
                if new_params:
                    self.action.action_params = new_params
                # broadcast new action status as "active"
                self.base.add_status()
                # broadcast remove old action
                # consider moving this into clear_status
                _ = self.base.actives.pop(str(old_uuid), None) 
                await self.clear_status(old_uuid)

                return self.action


        async def finish(
                         self, 
                         # end_state: HloStatus = HloStatus.finished
                        ) -> Action:
            """Close file_conn, finish prc, copy aux, 
            set endpoint status, and move active dict to past."""
            await asyncio.sleep(1)
            self.base.replace_status(
                       status_list = self.action.action_status,
                       old_status = HloStatus.active,
                       new_status = HloStatus.finished
                      )
            
            # self.action.action_status = [end_state]
            self.base.print_message("finishing data logging.")
            for filekey in self.file_conn:
                if self.file_conn[filekey].file:
                    await self.file_conn[filekey].file.close()
            self.file_conn = dict()

            if self.manual:
                await self.finish_manual_action()

            # write final act meta file (overwrite existing one)
            await self.base.write_act(self.action)

            await self.clear_status()
            self.data_logger.cancel()
            _ = self.base.actives.pop(str(self.action.action_uuid), None)
            return self.action


        async def track_file(
                             self, 
                             file_type: str, 
                             file_path: str, 
                             samples: List[SampleUnion]
                            ) -> None:
            "Add auxiliary files to file dictionary."
            if os.path.dirname(file_path) != \
            os.path.join(self.base.save_root, self.action.output_dir):
                self.action.AUX_file_paths.append(file_path)

            file_info = FileInfo(
                file_type=file_type,
                file_name=os.path.basename(file_path),
                # data_keys = json_data_keys,
                sample=[sample.get_global_label() for sample in samples],
                # action_uuid: Optional[UUID]
            )

            if self.action.file_dict is None:
                self.action.file_dict = []
            self.action.file_dict.append(file_info)
            self.base.print_message(
                f"{file_info.file_name}"
                " added to files_technique / aux_files list.")


        async def relocate_files(self):
            "Copy auxiliary files from folder path to prc directory."
            for x in self.action.AUX_file_paths:
                new_path = os.path.join(
                                        self.base.save_root, 
                                        self.action.output_dir, 
                                        os.path.basename(x)
                                       )
                await async_copy(x, new_path)

        async def finish_manual_action(self):
            if self.manual:
                self.action.experiment_status = [HloStatus.finished]
                self.action.sequence_status = [HloStatus.finished]

                # add action to experiment
                self.action.experiment_action_list.append(
                    self.action.get_act()
                )

                # add experiment to sequence
                self.action.experimentmodel_list.append(
                    self.action.get_prc()
                )

                # this will write the correct
                # sequence and experiment meta files for
                # manual operation
                # create and write seq file for manual action
                await self.base.write_seq(self.action)
                # create and write prc file for manual action
                await self.base.write_prc(self.action)
