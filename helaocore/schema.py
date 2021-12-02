""" schema.py
Standard classes for experiment queue objects.

"""

__all__ = ["Process", "Action", "Sequencer"]


import inspect
import types
from collections import defaultdict
from datetime import datetime
import copy

import helaocore.model.sample as hcms
from helaocore.helper import gen_uuid, print_message


# rename later to Process
class Process(object):
    "Sample-action grouping class."

    def __init__(
        self,
        inputdict: dict = {},
    ):
        imports = {}
        imports.update(inputdict)


        # main process parameters
        self.process_uuid = imports.get("process_uuid", None) #

        # main parametes for Action, need to put it into 
        # the new Basemodel in the future
        self.machine_name = imports.get("machine_name", None)
        

        # others parameter
        self.orch_name = imports.get("orch_name", "orchestrator")
        self.process_timestamp = imports.get("process_timestamp", None)
        self.process_label = imports.get("process_label", "noLabel")
        self.access = imports.get("access", "hte")
        self.process_name = imports.get("process_name", None)
        self.process_params = imports.get("process_params", {})
        # name of "instrument": sdc, anec, adss etc. defined in world config
        self.technique_name = imports.get("technique_name", None)

        # TODO: make the following attributes private
        self.result_dict = {}  # imports.get("result_dict", {})# this gets big really fast, bad for debugging
        self.global_params = {}  # TODO: reserved for internal use, do not write to .prg

    def as_dict(self):
        d = vars(self)
        attr_only = {k: v for k, v in d.items() if not isinstance(v,types.FunctionType) and not k.startswith("__")}
        return attr_only

    def fastdict(self):
        d = vars(self)
        params_dict = {
            k: int(v) if isinstance(v, bool) else v
            for k, v in d.items()
            if not isinstance(v,types.FunctionType)
            and not k.startswith("__")
            and (v is not None)
            and not isinstance(v,dict)
            and (v != {})
        }
        json_dict = {
            k: v
            for k, v in d.items()
            if not isinstance(v,types.FunctionType)
            and not k.startswith("__")
            and (v is not None)
            and isinstance(v,dict)
        }
        return params_dict, json_dict

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
        dtime = datetime.now()
        dtime = datetime.fromtimestamp(dtime.timestamp() + offset)
        self.process_timestamp = dtime.strftime("%Y%m%d.%H%M%S%f")


class Action(Process):
    "Sample-action identifier class."

    def __init__(
        self,
        inputdict: dict = {},
    ):
        super().__init__(inputdict)  # grab process keys
        imports = {}
        imports.update(inputdict)
        
        # main fixed parameters for Action
        self.action_uuid = imports.get("action_uuid", None)
        self.action_timestamp = None
        # machine_name # get it from process later
        self.action_ordering = imports.get("action_ordering", None)


        # other parameters
        self.action_server = imports.get("action_server", None)
        self.action_name = imports.get("action_name", None)
        self.action_params = imports.get("action_params", {})
        self.action_abbr = imports.get("action_abbr", None)
        self.start_condition = imports.get("start_condition", 3)

        # holds samples basemodel for parsing between actions etc
        self.samples_in: hcms.SampleList = []
        self.samples_out: hcms.SampleList = []

        # the following attributes are set during action dispatch but can be imported
        self.file_dict = defaultdict(lambda: defaultdict(dict))  # TODO: replace with model
        self.file_dict.update(imports.get("file_dict", {}))

        # TODO: make the following attributes private
        self.save_prc = imports.get("save_prc", True) # default should be true
        self.save_data = imports.get("save_data", True) # default should be true
        # self.plate_id = imports.get("plate_id", None) # not needed anymore
        self.prc_samples_in = []  # holds sample list of dict for prc writing
        self.prc_samples_out = []
        self.file_paths = imports.get("file_paths", [])
        self.data = imports.get("data", [])  # will be written to .hlo file
        self.output_dir = imports.get("output_dir", None)
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

        check_args = {"server": self.action_server, "name": self.action_name}
        missing_args = [k for k, v in check_args.items() if v is None]
        if missing_args:
            print_message(
                {},
                "action",
                f'action {" and ".join(missing_args)} not specified. Placeholder actions will only affect the action queue enumeration.',
                info=True,
            )

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
        atime = datetime.now()
        if offset is not None:
            atime = datetime.fromtimestamp(atime.timestamp() + offset)
        self.action_timestamp = atime.strftime("%Y%m%d.%H%M%S%f")


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
        self.action_list.append(Action(inputdict=new_action_dict))


    def add_action_list(self, action_list: list):
        for action in action_list:
            self.action_list.append(action)
