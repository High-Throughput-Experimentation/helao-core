""" schema.py
Standard classes for experiment queue objects.

"""

__all__ = ["Sequence", "Process", "Action", "Sequencer"]


import inspect
import copy
from pathlib import Path
from typing import Optional
from datetime import datetime
from pydantic import BaseModel


from .helper.print_message import print_message
from .helper.gen_uuid import gen_uuid
from .helper.set_time import set_time
from .helper.helaodict import HelaoDict
from .server import version
from .model.action import ActionModel
from .model.process import ProcessModel
from .model.process_sequence import ProcessSequenceModel


class _to_datetime(BaseModel):
    time: Optional[datetime]


class Sequence(HelaoDict, object):
    def __init__(
        self,
        inputdict: dict = {},
    ):
        imports = {}
        imports.update(inputdict)
        self.sequence_uuid = imports.get("sequence_uuid", None)
        self.sequence_timestamp = _to_datetime(time = imports.get("sequence_timestamp", None)).time
        self.sequence_status = imports.get("sequence_status", None)
        self.sequence_name = imports.get("sequence_name", None)
        self.sequence_label = imports.get("sequence_label", "noLabel")
        self.sequence_params = imports.get("sequence_params", {})
        if self.sequence_params is None:
            self.sequence_params = {}
        self.process_list = imports.get("process_list", [])

        def _to_time(Basemodel):
            time: datetime

    def __repr__(self):
        return f"<sequence_name:{self.sequence_name}>" 


    def __str__(self):
        return f"sequence_name:{self.sequence_name}" 


    def gen_uuid_sequence(self, machine_name: str):
        "server_name can be any string used in generating random uuid"
        if self.sequence_uuid:
            print_message(
                {},
                "process",
                f"process_uuid: {self.process_uuid} already exists",
                info=True,
            )
        else:
            self.sequence_uuid = gen_uuid(label=machine_name, timestamp=self.sequence_timestamp)
            print_message(
                {},
                "sequence",
                f"sequence_uuid: {self.sequence_uuid} assigned",
                info=True,
            )


    def set_sequence_time(self, offset: float = 0):
        self.sequence_timestamp = set_time(offset)


    def get_seq(self):
        return ProcessSequenceModel(
            hlo_version=f"{version.hlo_version}",
            sequence_name = self.sequence_name,
            sequence_params = self.sequence_params,
            sequence_label = self.sequence_label,
            sequence_uuid = self.sequence_uuid,
            sequence_timestamp = self.sequence_timestamp,
            sequence_status = self.sequence_status,
            process_plan_list = [process.process_name for process in self.process_list],
            # process_list = [process.process_uuid for process in self.process_list]
        )

    
    def init_seq(self, machine_name: str, time_offset: float = 0):
        if self.sequence_timestamp is None:
            self.set_sequence_time(offset=time_offset)
        if self.sequence_uuid is None:
            self.gen_uuid_sequence(machine_name)



class Process(Sequence):
    "Sample-action grouping class."

    def __init__(
        self,
        inputdict: dict = {},
    ):
        super().__init__(inputdict)  # grab sequence keys
        imports = {}
        imports.update(inputdict)


        # main process parameters
        self.process_uuid = imports.get("process_uuid", None) #
        self.process_status = imports.get("process_status", None)
        # main parametes for Action, need to put it into 
        # the new Basemodel in the future
        self.machine_name = imports.get("machine_name", None)
        

        # others parameter
        self.orchestrator = imports.get("orchestrator", "orchestrator")
        self.process_timestamp = _to_datetime(time = imports.get("process_timestamp", None)).time
        self.access = imports.get("access", "hte")
        self.process_name = imports.get("process_name", None)
        self.process_params = imports.get("process_params", {})
        if self.process_params is None:
            self.process_params = {}

        # name of "instrument": sdc, anec, adss etc. defined in world config
        self.technique_name = imports.get("technique_name", None)

        # TODO: make the following attributes private
        self.result_dict = {}  # imports.get("result_dict", {})# this gets big really fast, bad for debugging
        self.global_params = {}  # TODO: reserved for internal use, do not write to .prg

        self.action_uuid_list = imports.get("action_uuid_list", None)
        self.action_file_list = imports.get("action_file_list", None)
        # sequence parameters which should go in its own class later

    def __repr__(self):
        return f"<process_name:{self.process_name}>" 


    def __str__(self):
        return f"process_name:{self.process_name}" 


    def gen_uuid_process(self, machine_name: str):
        "server_name can be any string used in generating random uuid"
        if self.process_uuid:
            print_message(
                {},
                "process",
                f"process_uuid: {self.process_uuid} already exists",
                info=True,
            )
        else:
            self.process_uuid = gen_uuid(label=machine_name, timestamp=self.process_timestamp)
            print_message(
                {},
                "process",
                f"process_uuid: {self.process_uuid} assigned",
                info=True,
            )

    def set_dtime(self, offset: float = 0):
        self.process_timestamp = set_time(offset)


    def get_prc(self):
        
        prc = ProcessModel(
            hlo_version=f"{version.hlo_version}",
            orchestrator=self.orchestrator,
            machine_name=self.machine_name,#gethostname(),
            sequence_uuid=self.sequence_uuid,
            access=self.access,
            process_uuid=self.process_uuid,
            process_timestamp=self.process_timestamp,
            process_status = self.process_status,
            technique_name=self.technique_name,
            process_name=self.process_name,
            process_params=self.process_params,
            # action_uuid_list=self.action_uuid_list,
            # samples_in: Optional[List[sample.SampleUnion]]
            # samples_out: Optional[List[sample.SampleUnion]]
            # files = self.action_file_list
        )
        prc.update_from_actlist()
        return prc


class Action(Process):
    "Sample-action identifier class."

    def __init__(
        self,
        inputdict: dict = {},
        act: ActionModel = None,
    ):
        super().__init__(inputdict)  # grab process keys
        imports = {}
        imports.update(inputdict)
        
        
        # overwrite some other params from seq, prc
        # usually also in dict (but we want to remove that in the future)
        self.technique_name = act.technique_name
        self.orchestrator = act.orchestrator
        self.machine_name = act.machine_name
        self.process_uuid = act.process_uuid
        self.process_timestamp = act.process_timestamp
        self.access = act.access
        
        
        # main fixed parameters for Action
        self.action_uuid = act.action_uuid
        self.action_timestamp = act.action_timestamp
        self.action_status = act.action_status
        # machine_name # get it from process later
        self.action_order = act.action_order
        self.action_retry = act.action_retry
        self.action_actual_order = act.action_actual_order

        # name of the action server
        self.action_server_name = act.action_server_name

        # other parameters
        self.action_name = act.action_name
        self.action_params = act.action_params
        self.action_abbr = act.action_abbr
        self.output_dir = act.output_dir

        # holds samples basemodel for parsing between actions etc
        self.samples_in = act.samples_in
        self.samples_out = act.samples_out

        # other params which are not in ActionModel
        self.start_condition = imports.get("start_condition", 3)

        # the following attributes are set during action dispatch but can be imported

        self.file_dict = imports.get("file_dict", [])
        if self.file_dict is None:
            self.file_dict = []

        # TODO: make the following attributes private
        self.save_act = imports.get("save_act", True) # default should be true
        self.save_data = imports.get("save_data", True) # default should be true
        self.file_paths = imports.get("file_paths", [])
        self.data = imports.get("data", [])  # will be written to .hlo file
        self.column_names = imports.get("column_names", None)  # deprecated in .hlo file format
        self.header = imports.get("header", None)  # deprecated in .hlo file format
        self.file_type = imports.get("file_type", None)
        self.filename = imports.get("filename", None)
        self.file_data_keys = imports.get("file_data_keys", None)
        self.file_sample_label = imports.get("file_sample_label", None)
        self.file_sample_keys = imports.get("file_sample_keys", None)
        self.file_group = imports.get("file_group", None)
        self.error_code = imports.get("error_code", "0")
        self.from_global_params = imports.get("from_global_params", {})
        self.to_global_params = imports.get("to_global_params", [])


    def __repr__(self):
        return f"<action_name:{self.action_name}>" 


    def __str__(self):
        return f"action_name:{self.action_name}" 


    def gen_uuid_action(self, machine_name: str):
        if self.action_uuid:
            print_message(
                {},
                "action",
                f"action_uuid: {self.action_uuid} already exists",
                error=True,
            )
        else:
            self.action_uuid = gen_uuid(
                label=f"{machine_name}_{self.action_name}",
                timestamp=self.action_timestamp,
            )
            print_message({}, "action", f"action_uuid: {self.action_uuid} assigned", info=True)

    def set_atime(self, offset: float = 0.0):
        self.action_timestamp = set_time(offset)


    def get_act(self):
        return ActionModel(
            hlo_version=f"{version.hlo_version}",
            technique_name=self.technique_name,
            action_server_name=self.action_server_name,
            orchestrator=self.orchestrator,
            machine_name=self.machine_name,
            access=self.access,
            output_dir=Path(self.output_dir).as_posix() \
                if self.output_dir is not None else None,
            process_uuid=self.process_uuid,
            process_timestamp=self.process_timestamp,
            action_status=self.action_status,
            action_uuid=self.action_uuid,
            action_timestamp=self.action_timestamp,
            action_order=self.action_order,
            action_retry=self.action_retry,
            action_actual_order=self.action_actual_order,
            action_name=self.action_name,
            action_abbr=self.action_abbr,
            action_params=self.action_params,
            samples_in = self.samples_in,
            samples_out = self.samples_out,
            files = self.file_dict
        )

    def init_act(self, machine_name: str, time_offset: float = 0):
        if self.action_timestamp is None:
            self.set_atime(offset=time_offset)
        if self.action_uuid is None:
            self.gen_uuid_action(machine_name)



class Sequencer(object):
    def __init__(
        self,
        pg: Process,
    ):
        frame = inspect.currentframe().f_back
        _args, _varargs, _keywords, _locals = inspect.getargvalues(frame)
        self._pg = copy.deepcopy(pg)
        self.action_list = []
        self.pars = self._C()
        if self._pg.process_params is not None:
            for key, val in self._pg.process_params.items():
                setattr(self.pars, key, val)  # we could also add it direcly to the class root by just using self

        for key, val in _locals.items():
            if key != "pg_Obj" and key not in self._pg.process_params.keys():
                print_message(
                    {},
                    "processr",
                    f"local var '{key}' not found in pg_Obj, addding it to sq.pars",
                    error=True,
                )
                setattr(
                    self.pars, key, val
                )  # we could also add it direcly to the class root by just using self

    class _C:
        pass

    def add_action(self, action_dict: dict):
        new_action_dict = self._pg.as_dict()
        new_action_dict.update(action_dict)
        self.action_list.append(Action(
                                       inputdict=new_action_dict,
                                       act=ActionModel(**new_action_dict)
                                      ))


    def add_action_list(self, action_list: list):
        for action in action_list:
            self.action_list.append(action)


class SequenceListMaker(object):
    def __init__(
        self,
    ):
        self.process_list = []


    def add_process(self, selected_process, process_params):
        D = Process(inputdict={
            "process_name":selected_process,
            "process_params":process_params,
        })
        self.process_list.append(D.as_dict())
