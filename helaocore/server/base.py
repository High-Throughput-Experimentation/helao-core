__all__ = ["Base"]
import asyncio
import json
import os
import sys
from copy import copy
from pathlib import Path
from socket import gethostname
from time import ctime, strftime, strptime, time, time_ns
from typing import Optional

import aiofiles
import colorama
import helaocore.model.file as hcmf
import helaocore.server.version as version
import ntplib
import numpy as np
import pyaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.openapi.utils import get_flat_params
from helaocore.helper import MultisubscriberQueue
from helaocore.helper import async_copy
from helaocore.helper import cleanupdict
from helaocore.helper import print_message
from helaocore.helper import helao_dirs
from helaocore.schema import Action

from .api import HelaoFastAPI
from .dispatcher import async_private_dispatcher

# ANSI color codes converted to the Windows versions
colorama.init(strip=not sys.stdout.isatty())  # strip colors if stdout is redirected
# colorama.init()


class Base(object):
    """Base class for all HELAO servers.

    Base is a general class which implements message passing, status update, data
    writing, and data streaming via async tasks. Every instrument and action server
    should import this class for efficient integration into an orchestrated environment.

    A Base initialized within a FastAPI startup event will launch three async tasks
    to the server's event loop for handling:
    (1) broadcasting status updates via websocket and http POST requests to an attached
        orchestrator's status updater if available,
    (2) data streaming via websocket,
    (3) data writing to local disk.

    Websocket connections are broadcast from a multisubscriber queue in order to handle
    consumption from multiple clients awaiting a single queue. Self-subscriber tasks are
    also created as initial subscribers to log all events and prevent queue overflow.

    The data writing method will update a class attribute with the currently open file.
    For a given root directory, files and folders will be written as follows:
    {%y.%j}/  # sequence_date year.weeknum
        {%Y%m%d}/  # sequence_date
            {%H%M%S}__{sequence_label}/  # sequence_time
                {%Y%m%d.%H%M%S}__{action_server_name}__{action_name}__{action_uuid}/
                    {filename}.{ext}
                    {%Y%m%d.%H%M%S%f}.prc  # action_datetime
                    (aux_files)
    """

    def __init__(self, fastapp: HelaoFastAPI, calibration: dict = {}):
        self.server_name = fastapp.helao_srv
        self.server_cfg = fastapp.helao_cfg["servers"][self.server_name]
        self.server_params = fastapp.helao_cfg["servers"][self.server_name].get("params", dict())
        self.world_cfg = fastapp.helao_cfg
        self.hostname = gethostname()
        self.technique_name = None
        self.aloop = asyncio.get_running_loop()

        self.root, self.save_root, self.log_root, self.states_root = \
            helao_dirs(self.world_cfg)
        
        if self.root is None:
            raise ValueError(
                "Warning: root directory was not defined. Logs, PRCs, PRGs, and data will not be written.",
                error=True,
            )

        if "technique_name" in self.world_cfg:
            self.print_message(
                f"Found technique_name in config: {self.world_cfg['technique_name']}",
            )
            self.technique_name = self.world_cfg["technique_name"]
        else:
            raise ValueError(
                "Missing 'technique_name' in config, cannot create server object.",
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
            self.ntp_last_sync_file = os.path.join(self.states_root, "ntpLastSync.txt")
            if os.path.exists(self.ntp_last_sync_file):
                with open(self.ntp_last_sync_file, "r") as f:
                    tmps = f.readline()
                    self.ntp_last_sync, self.ntp_offset = tmps.strip().split(",")
                    self.ntp_offset = float(self.ntp_offset)

        if self.ntp_last_sync is None:
            asyncio.gather(self.get_ntp_time())
        self.init_endpoint_status(fastapp)
        self.fast_urls = self.get_endpoint_urls(fastapp)
        self.status_logger = self.aloop.create_task(self.log_status_task())
        self.ntp_syncer = self.aloop.create_task(self.sync_ntp_task())

    def print_message(self, *args, **kwargs):
        print_message(self.server_cfg, self.server_name, log_dir = self.log_root, *args, **kwargs)

        # style = self.server_cfg.get("msg_color","")
        # for arg in args:
        #     # print(f"{Style.BRIGHT}{Fore.GREEN}{arg}{Style.RESET_ALL}")
        #     print(f"[{strftime('%H:%M:%S')}_{self.server_name}]: {style}{arg}{Style.RESET_ALL}")

    def init_endpoint_status(self, app: FastAPI):
        "Populate status dict with FastAPI server endpoints for monitoring."
        for route in app.routes:
            if route.path.startswith(f"/{self.server_name}"):
                self.status[route.name] = []
                self.endpoints.append(route.name)
        self.print_message(
            f"Found {len(self.status)} endpoints for status monitoring on {self.server_name}."
        )

    def get_endpoint_urls(self, app: HelaoFastAPI):
        """Return a list of all endpoints on this server."""
        url_list = []
        for route in app.routes:
            routeD = {"path": route.path, "name": route.name}
            if "dependant" in dir(route):
                flatParams = get_flat_params(route.dependant)
                paramD = {
                    par.name: {
                        "outer_type": str(par.outer_type_).split("'")[1],
                        "type": str(par.type_).split("'")[1],
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

    async def contain_action(
        self,
        action: Action,
        file_type: str = "helao__file",
        file_data_keys: Optional[str] = None,  # this is also keyd by file_sample_keys
        file_sample_label: Optional[str] = None,  # this is also keyd by file_sample_keys
        file_sample_keys: Optional[
            list
        ] = None,  # I need one key per datafile, but each datafile can still be based on multiple samples
        header: Optional[str] = None,  # this is also keyd by file_sample_keys
    ):
        self.actives[action.action_uuid] = Base.Active(
            self,
            action=action,
            file_type=file_type,
            file_data_keys=file_data_keys,
            file_sample_label=file_sample_label,
            file_sample_keys=file_sample_keys,
            header=header,
        )
        await self.actives[action.action_uuid].myinit()
        return self.actives[action.action_uuid]

    async def get_active_info(self, action_uuid: str):
        if action_uuid in self.actives:
            action_dict = await self.actives[action_uuid].active.as_dict()
            return action_dict
        else:
            self.print_message(f"Specified action uuid {action_uuid} was not found.", error=True)
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
            async for status_msg in self.status_q.subscribe():
                self.status.update(status_msg)
                for client_servkey in self.status_clients:
                    success = False

                    for _ in range(retry_limit):

                        response = await async_private_dispatcher(
                            world_config_dict=self.world_cfg,
                            server=client_servkey,
                            private_action="update_status",
                            params_dict={
                                "server": self.server_name,
                                "status": json.dumps(status_msg),
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
        except asyncio.CancelledError:
            self.print_message("status logger task was cancelled", error=True)

    async def detach_subscribers(self):
        await self.status_q.put(StopAsyncIteration)
        await self.data_q.put(StopAsyncIteration)
        await asyncio.sleep(5)

    async def set_realtime(self, epoch_ns: Optional[float] = None, offset: Optional[float] = None):
        return self.set_realtime_nowait(epoch_ns=epoch_ns, offset=offset)

    def set_realtime_nowait(self, epoch_ns: Optional[float] = None, offset: Optional[float] = None):
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
            self.print_message("ntp sync task was cancelled", error=True)

    async def shutdown(self):
        await self.detach_subscribers()
        self.status_logger.cancel()
        self.ntp_syncer.cancel()

    async def write_to_prg(self, prg_dict: dict, sequence):
        sequence_timestamp = sequence.sequence_timestamp
        sequence_dir = self.get_sequence_dir(sequence)
        output_path = os.path.join(self.save_root, sequence_dir)
        output_file = os.path.join(output_path, f"{sequence_timestamp}.prg")

        self.print_message(f"writing to prg: {output_file}")
        output_str = pyaml.dump(prg_dict, sort_dicts=False)

        if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)
        
        async with aiofiles.open(output_file, mode="a+") as f:
            if not output_str.endswith("\n"):
                output_str += "\n"
            await f.write(output_str)


    def get_sequence_dir(self, sequence):
        """accepts action or sequence object"""
        sequence_date = sequence.sequence_timestamp.split(".")[0]
        sequence_time = sequence.sequence_timestamp.split(".")[-1]
        year_week = strftime("%y.%U", strptime(sequence_date, "%Y%m%d"))
        return os.path.join(
            year_week,
            sequence_date,
            f"{sequence_time}_{sequence.sequence_label}",
        )

    class Active(object):
        """Active action holder which wraps data queing and prc writing."""

        def __init__(
            self,
            base,  # outer instance
            action: Action,
            file_type: str = "helao__file",
            file_data_keys: Optional[str] = None,
            file_sample_label: Optional[str] = None,
            file_sample_keys: Optional[list] = None,
            header: Optional[str] = None,
        ):
            self.base = base
            self.action = action
            self.action.file_type = file_type
            self.action.file_group = "helao_files"
            self.action.file_data_keys = file_data_keys
            self.action.file_sample_label = file_sample_label
            self.action.header = header
            self.prc_file = None
            self.manual_prg_file = None
            self.manual = False
            self.sequence_dir = None

            if file_sample_keys is None:
                self.action.file_sample_keys = ["None"]
                self.action.file_sample_label = {"None": self.action.file_sample_label}
                self.action.file_data_keys = {"None": self.action.file_data_keys}
                self.action.header = {"None": self.action.header}
            else:
                self.action.file_sample_keys = file_sample_keys
                if type(self.action.file_sample_keys) is not list:
                    self.action.file_sample_keys = [self.action.file_sample_keys]
                if self.action.file_sample_label is None:
                    self.action.file_sample_label = {
                        f"{file_sample_key}": None for file_sample_key in self.action.file_sample_keys
                    }
                if self.action.file_data_keys is None:
                    self.action.file_data_keys = {
                        f"{file_sample_key}": None for file_sample_key in self.action.file_sample_keys
                    }
                if self.action.header is None:
                    self.action.header = {
                        f"{file_sample_key}": None for file_sample_key in self.action.file_sample_keys
                    }

            self.action.set_atime(offset=self.base.ntp_offset)
            self.action.gen_uuid_action(self.base.hostname)
            # signals the data logger that it got data and hlo header was written or not
            # active.finish_hlo_header should be called within the driver before
            # any data is pushed to avoid a forced header end write
            self.finished_hlo_header = dict()
            self.file_conn = dict()
            if self.action.sequence_timestamp is None:
                self.manual = True
                self.base.print_message("Manual Action.", info=True)
                self.action.set_dtime(offset=self.base.ntp_offset)
                self.action.gen_uuid_sequence(self.base.hostname)

            if not self.base.save_root:
                self.base.print_message(
                    "Root save directory not specified, cannot save action results."
                )
                self.action.save_data = False
                self.action.save_prc = False
                self.action.output_dir = None
            else:
                if self.action.save_data is None:
                    self.action.save_data = False
                if self.action.save_prc is None:
                    self.action.save_prc = False
                # cannot save data without prc
                if self.action.save_data is True:
                    self.action.save_prc = True

                self.sequence_dir = self.base.get_sequence_dir(self.action)
                self.action.output_dir = os.path.join(
                    self.sequence_dir,
                    f"{self.action.action_timestamp}__{self.action.action_server}__{self.action.action_name}__{self.action.action_uuid}",
                )
            self.data_logger = self.base.aloop.create_task(self.log_data_task())

        async def update_prc_file(self):
            # need to remove swagger workaround value if present
            if "scratch" in self.action.action_params:
                del self.action.action_params["scratch"]

            if self.action.action_ordering is None:
                self.action.action_ordering = 0.0

            self.prc_file = hcmf.PrcFile(
                hlo_version=f"{version.hlo_version}",
                technique_name=self.action.technique_name,
                server_name=self.base.server_name,
                orchestrator=self.action.orch_name,
                machine_name=self.action.machine_name,
                access=self.action.access,
                output_dir=Path(self.action.output_dir).as_posix(),
                sequence_uuid=self.action.sequence_uuid,
                sequence_timestamp=self.action.sequence_timestamp,
                action_uuid=self.action.action_uuid,
                action_timestamp=self.action.action_timestamp,
                action_ordering=self.action.action_ordering,
                action_name=self.action.action_name,
                action_abbr=self.action.action_abbr,
                action_params=self.action.action_params,
            )
            # write initial temporary prc file
            await self.write_prc()


        async def myinit(self):
            if self.action.save_prc:
                os.makedirs(
                    os.path.join(self.base.save_root, self.action.output_dir),
                    exist_ok=True,
                )
                self.action.action_num = f"{self.action.action_abbr}-{self.action.action_ordering}"
                await self.update_prc_file()

                if self.manual:
                    # create and write prg file for manual action
                    self.manual_prg_file = hcmf.PrgFile(
                        hlo_version=f"{version.hlo_version}",
                        orchestrator=self.action.orch_name,
                        machine_name=gethostname(),
                        access=self.action.access,
                        sequence_uuid=self.action.sequence_uuid,
                        sequence_timestamp=self.action.sequence_timestamp,
                        sequence_label=self.action.sequence_label,
                        technique_name=self.action.technique_name,
                        sequence_name="MANUAL",
                        sequence_params=None,
                        sequence_model=None,
                    )
                    await self.base.write_to_prg(cleanupdict(self.manual_prg_file.dict()), self.action)

                if self.action.save_data:
                    for i, file_sample_key in enumerate(self.action.file_sample_keys):
                        filename, header, file_info = self.init_datafile(
                            header=self.action.header.get(file_sample_key, None),
                            file_type=self.action.file_type,
                            file_data_keys=self.action.file_data_keys.get(file_sample_key, None),
                            file_sample_label=self.action.file_sample_label.get(file_sample_key, None),
                            filename=None,  # always autogen a filename
                            file_group=self.action.file_group,
                            action_ordering=self.action.action_ordering,
                            action_abbr=self.action.action_abbr,
                            filenum=i,
                        )

                        self.action.file_dict.update({filename: file_info})
                        await self.set_output_file(
                            filename=filename,
                            header=header,
                            file_sample_key=file_sample_key,
                        )

            await self.add_status()

        def init_datafile(
            self,
            header,
            file_type,
            file_data_keys,
            file_sample_label,
            filename,
            file_group,
            action_ordering,
            action_abbr,
            filenum: Optional[int] = 0,
        ):

            if header:
                if isinstance(header, dict):
                    header_dict = copy(header)
                    header = pyaml.dump(header, sort_dicts=False)
                    # header_lines = len(header_dict)
                else:
                    if isinstance(header, list):
                        # header_lines = len(header)
                        header = "\n".join(header)
                    # else:
                    #     header_lines = len(header.split("\n"))

            file_info = {"type": file_type}
            if file_data_keys is not None:
                file_info.update({"keys": file_data_keys})
            if file_sample_label is not None:
                if len(file_sample_label) != 0:
                    file_info.update({"sample": file_sample_label})

            if filename is None:  # generate filename
                file_ext = "csv"
                if file_group == "helao_files":
                    file_ext = "hlo"

                    header_dict = {
                        "hlo_version": version.hlo_version,
                        "action_name": self.action.action_abbr
                        if self.action.action_abbr is not None
                        else self.action.action_name,
                        "column_headings": file_data_keys,
                    }

                    if header is None:
                        header = pyaml.dump(header_dict, sort_dicts=False)
                    else:
                        header = pyaml.dump(header_dict, sort_dicts=False) + header
                else:  # aux_files
                    pass

                if action_ordering is not None:
                    filename = f"{action_abbr}-{action_ordering:.1f}__{filenum}.{file_ext}"
                else:
                    filename = f"{action_abbr}-0.0__{filenum}.{file_ext}"

            if header:
                if not header.endswith("\n"):
                    header += "\n"

            return filename, header, file_info

        def finish_hlo_header(self, realtime: Optional[int] = None):
            # needs to be a sync function
            if realtime == None:
                realtime = self.set_realtime_nowait()

            data_dict1 = dict()
            data_dict2 = dict()
            file_keys = []
            for file_key in self.file_conn:
                data_dict1[file_key] = pyaml.dump({"epoch_ns": realtime})
                data_dict2[file_key] = "%%"
                file_keys.append(file_key)
                # before we push the header end onto the dataq, need to set the flag
                self.finished_hlo_header[file_key] = True

            self.enqueue_data_nowait(data_dict1, file_sample_keys=file_keys)
            self.enqueue_data_nowait(data_dict2, file_sample_keys=file_keys)

        async def add_status(self):
            self.base.status[self.action.action_name].append(self.action.action_uuid)
            self.base.print_message(
                f"Added {self.action.action_uuid} to {self.action.action_name} status list."
            )
            await self.base.status_q.put(
                {self.action.action_name: self.base.status[self.action.action_name]}
            )

        async def clear_status(self):
            if self.action.action_uuid in self.base.status[self.action.action_name]:
                self.base.status[self.action.action_name].remove(self.action.action_uuid)
                self.base.print_message(
                    f"Removed {self.action.action_uuid} from {self.action.action_name} status list.",
                    info=True,
                )
            else:
                self.base.print_message(
                    f"{self.action.action_uuid} did not excist in {self.action.action_name} status list.",
                    error=True,
                )
            await self.base.status_q.put(
                {self.action.action_name: self.base.status[self.action.action_name]}
            )

        async def set_estop(self):
            self.base.status[self.action.action_name].remove(self.action.action_uuid)
            self.base.status[self.action.action_name].append(f"{self.action.action_uuid}__estop")
            self.base.print_message(
                f"E-STOP {self.action.action_uuid} on {self.action.action_name} status.",
                error=True,
            )
            await self.base.status_q.put(
                {self.action.action_name: self.base.status[self.action.action_name]}
            )

        async def set_error(self, err_msg: Optional[str] = None):
            self.base.status[self.action.action_name].remove(self.action.action_uuid)
            self.base.status[self.action.action_name].append(f"{self.action.action_uuid}__error")
            self.base.print_message(
                f"ERROR {self.action.action_uuid} on {self.action.action_name} status.",
                error=True,
            )
            if err_msg:
                self.action.error_code = err_msg
            else:
                self.action.error_code = "-1 unspecified error"
            await self.base.status_q.put(
                {self.action.action_name: self.base.status[self.action.action_name]}
            )

        async def set_realtime(self, epoch_ns: Optional[float] = None, offset: Optional[float] = None):
            # return self.set_realtime_nowait(epoch_ns=epoch_ns, offset=offset)
            return self.base.set_realtime_nowait(epoch_ns=epoch_ns, offset=offset)

        def set_realtime_nowait(self, epoch_ns: Optional[float] = None, offset: Optional[float] = None):
            return self.base.set_realtime_nowait(epoch_ns=epoch_ns, offset=offset)

        async def set_output_file(self, filename: str, file_sample_key: str, header: Optional[str] = None):
            "Set active save_path, write header if supplied."
            output_path = os.path.join(self.base.save_root, self.action.output_dir, filename)
            self.base.print_message(f"writing data to: {output_path}")
            # create output file and set connection
            self.file_conn[file_sample_key] = await aiofiles.open(output_path, mode="a+")
            self.finished_hlo_header[file_sample_key] = False
            if header:
                if not header.endswith("\n"):
                    header += "\n"
                await self.file_conn[file_sample_key].write(header)

        async def write_live_data(self, output_str: str, file_conn_key):
            """Appends lines to file_conn."""
            if file_conn_key in self.file_conn:
                if self.file_conn[file_conn_key]:
                    if not output_str.endswith("\n"):
                        output_str += "\n"
                    await self.file_conn[file_conn_key].write(output_str)

        async def enqueue_data(self, data, errors: list = [], file_sample_keys: Optional[list] = None):
            await self.base.data_q.put(
                self.assemble_data_msg(data=data, errors=errors, file_sample_keys=file_sample_keys)
            )

        def enqueue_data_nowait(self, data, errors: list = [], file_sample_keys: Optional[list] = None):
            self.base.data_q.put_nowait(
                self.assemble_data_msg(data=data, errors=errors, file_sample_keys=file_sample_keys)
            )

        def assemble_data_msg(self, data, errors: list = [], file_sample_keys: list = None):
            data_dict = dict()
            if file_sample_keys is None:
                data_dict["None"] = data
            else:
                if type(file_sample_keys) is not list:
                    file_sample_keys = [file_sample_keys]
                for file_sample_key in file_sample_keys:
                    data_dict[file_sample_key] = data.get(file_sample_key, dict())

            data_msg = {
                self.action.action_uuid: {
                    "data": data_dict,
                    "action_name": self.action.action_name,
                    "errors": errors,
                }
            }
            return data_msg

        async def log_data_task(self):
            """Self-subscribe to data queue, write to present file path."""
            self.base.print_message("starting data logger")
            # data_msg should be a dict {uuid: list of values or a list of list of values}
            try:
                async for data_msg in self.base.data_q.subscribe():
                    if self.action.action_uuid in data_msg:  # only write data for this action
                        data_dict = data_msg[self.action.action_uuid]
                        data_val = data_dict["data"]
                        self.action.data.append(data_val)
                        for sample, sample_data in data_val.items():
                            if sample in self.file_conn:
                                if self.file_conn[sample]:
                                    # check if end of hlo header was writen
                                    # else force it here
                                    # e.g. just write the separator
                                    if not self.finished_hlo_header[sample]:
                                        self.base.print_message(
                                            f"{self.action.action_abbr} data file {sample} is missing hlo separator. Writing it.",
                                            error=True,
                                        )
                                        self.finished_hlo_header[sample] = True
                                        await self.write_live_data(
                                            output_str=pyaml.dump({"epoch_ns": self.set_realtime_nowait()})
                                            + "%%\n",
                                            file_conn_key=sample,
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
                                            file_conn_key=sample,
                                        )
                                    else:
                                        await self.write_live_data(
                                            output_str=sample_data, 
                                            file_conn_key=sample
                                        )
                            else:
                                self.base.print_message(
                                    "{sample} doesn not exist in file_conn.",
                                    error=True,
                                )

            except asyncio.CancelledError:
                self.base.print_message("data logger task was cancelled", error=True)

        async def write_file(
            self,
            output_str: str,
            file_type: str,
            filename: Optional[str] = None,
            file_group: Optional[str] = "aux_files",
            header: Optional[str] = None,
            sample_str: Optional[str] = None,
            file_sample_label: Optional[str] = None,
            file_data_keys: Optional[str] = None,
        ):
            "Write complete file, not used with queue streaming."
            if self.action.save_data:
                filename, header, file_info = self.init_datafile(
                    header=header,
                    file_type=file_type,
                    file_data_keys=file_data_keys,
                    file_sample_label=file_sample_label,
                    filename=filename,
                    file_group=file_group,
                    action_ordering=self.action.action_ordering,
                    action_abbr=self.action.action_abbr,
                )
                output_path = os.path.join(self.base.save_root, self.action.output_dir, filename)
                self.base.print_message(f"writing non stream data to: {output_path}")

                async with aiofiles.open(output_path, mode="w") as f:
                    await f.write(header + output_str)
                    self.action.file_dict.update({filename: file_info})
                    return output_path
            else:
                return None

        def write_file_nowait(
            self,
            output_str: str,
            file_type: str,
            filename: Optional[str] = None,
            file_group: Optional[str] = "aux_files",
            header: Optional[str] = None,
            sample_str: Optional[str] = None,
            file_sample_label: Optional[str] = None,
            file_data_keys: Optional[str] = None,
        ):
            "Write complete file, not used with queue streaming."
            if self.action.save_data:
                filename, header, file_info = self.init_datafile(
                    header=header,
                    file_type=file_type,
                    file_data_keys=file_data_keys,
                    file_sample_label=file_sample_label,
                    filename=filename,
                    file_group=file_group,
                    action_ordering=self.action.action_ordering,
                    action_abbr=self.action.action_abbr,
                )
                output_path = os.path.join(self.base.save_root, self.action.output_dir, filename)
                self.base.print_message(f"writing non stream data to: {output_path}")
                with open(output_path, mode="w") as f:
                    f.write(header + output_str)
                    self.action.file_dict.update({filename: file_info})
                    return output_path
            else:
                return None

        async def write_prc(self):
            "Create new prc if it doesn't exist."
            output_path = os.path.join(
                self.base.save_root,
                self.action.output_dir,
                f"{self.action.action_timestamp}.prc",
            )
            self.base.print_message(f"writing to prc: {output_path}")
            async with aiofiles.open(output_path, mode="w") as f:
                await f.write(pyaml.dump(cleanupdict(self.prc_file.dict()), 
                                         sort_dicts=False))


        async def append_sample(self, samples, IO: str):
            "Add sample to samples_out and samples_in dict"

            # - inheritance
            # give_only:
            # receive_only:
            # allow_both:
            # block_both:

            # - status:
            # created: pretty self-explanatory; the sample was created during the action.
            # destroyed: also self-explanatory
            # preserved: the sample exists before and after the action. e.g. an echem experiment
            # incorporated: the sample was combined with others in the action. E.g. the creation of an electrode assembly from electrodes and electrolytes
            # recovered: the opposite of incorporated. E.g. an electrode assembly is taken apart, and the original electrodes are recovered, and further experiments may be done on those electrodes

            if samples is None:
                return

            if type(samples) is not list:
                samples = [samples]

            for sample in samples:
                if sample is None:
                    continue
                
                if sample.inheritance is None:
                    self.base.print_message("sample.inheritance is None. Using 'allow_both'.")
                    sample.inheritance = "allow_both"

                if sample.status is None:
                    self.base.print_message("sample.status is None. Using 'preserved'.")
                    sample.status = ["preserved"]

                append_dict = sample.prc_dict()
                if append_dict is not None:
                    # check if list for safety reasons
                    if type(self.action.prc_samples_in) is not list:
                        self.action.prc_samples_in = []
                    if type(self.action.prc_samples_out) is not list:
                        self.action.prc_samples_out = []

                    if IO == "in":
                        self.action.prc_samples_in.append(append_dict)
                    elif IO == "out":
                        self.action.prc_samples_out.append(append_dict)

        async def finish(self):
            "Close file_conn, finish prc, copy aux, set endpoint status, and move active dict to past."
            await asyncio.sleep(1)
            self.base.print_message("finishing data logging.")
            for filekey in self.file_conn:
                if self.file_conn[filekey]:
                    await self.file_conn[filekey].close()
            self.file_conn = dict()
            # (1) update sample_in and sample_out
            if self.action.prc_samples_in:
                self.prc_file.samples_in = self.action.prc_samples_in
            if self.action.prc_samples_out:
                self.prc_file.samples_out = self.action.prc_samples_out
            # (2) update file dict in prc header
            if self.action.file_dict:
                self.prc_file.files = self.action.file_dict

            # write full prc header to file
            await self.write_prc()

            await self.clear_status()
            self.data_logger.cancel()
            _ = self.base.actives.pop(self.action.action_uuid, None)
            return self.action

        async def track_file(self, file_type: str, file_path: str, sample_no: str):
            "Add auxiliary files to file dictionary."
            if os.path.dirname(file_path) != os.path.join(self.base.save_root, self.action.output_dir):
                self.action.file_paths.append(file_path)
            file_info = f"{file_type};{sample_no}"
            filename = os.path.basename(file_path)
            self.action.file_dict.update({filename: file_info})
            self.base.print_message(
                f"{filename} added to files_technique__{self.action.action_num} / aux_files list."
            )

        async def relocate_files(self):
            "Copy auxiliary files from folder path to prc directory."
            for x in self.action.file_paths:
                new_path = os.path.join(self.base.save_root, self.action.output_dir, os.path.basename(x))
                await async_copy(x, new_path)
