""" schema.py
Standard classes for experiment queue objects.

"""

__all__ = ["Sequence", "Process", "Sequencer"]


import inspect
import types
from collections import defaultdict
from datetime import datetime

import helaocore.model.sample as hcms
from helaocore.helper import gen_uuid, print_message


# rename later to Sequence
class Sequence(object):
    "Sample-process grouping class."

    def __init__(
        self,
        inputdict: dict = {},
    ):
        imports = {}
        imports.update(inputdict)


        # main sequence parameters
        self.sequence_uuid = imports.get("sequence_uuid", None) #

        # main parametes for Process, need to put it into 
        # the new Basemodel in the future
        self.machine_name = imports.get("machine_name", None)
        

        # others parameter
        self.orch_name = imports.get("orch_name", "orchestrator")




        self.sequence_timestamp = imports.get("sequence_timestamp", None)
        self.sequence_label = imports.get("sequence_label", "noLabel")
        self.access = imports.get("access", "hte")
        self.sequence_name = imports.get("sequence_name", None)
        self.sequence_params = imports.get("sequence_params", {})
        # name of "instrument": sdc, anec, adss etc. defined in world config
        self.technique_name = imports.get("technique_name", None)

        # TODO: make the following attributes private
        self.result_dict = {}  # imports.get("result_dict", {})# this gets big really fast, bad for debugging
        self.global_params = {}  # TODO: reserved for internal use, do not write to .prg

    def as_dict(self):
        d = vars(self)
        attr_only = {k: v for k, v in d.items() if type(v) != types.FunctionType and not k.startswith("__")}
        return attr_only

    def fastdict(self):
        d = vars(self)
        params_dict = {
            k: int(v) if type(v) == bool else v
            for k, v in d.items()
            if type(v) != types.FunctionType
            and not k.startswith("__")
            and (v is not None)
            and (type(v) != dict)
            and (v != {})
        }
        json_dict = {
            k: v
            for k, v in d.items()
            if type(v) != types.FunctionType
            and not k.startswith("__")
            and (v is not None)
            and (type(v) == dict)
        }
        return params_dict, json_dict

    def gen_uuid_sequence(self, machine_name: str):
        "server_name can be any string used in generating random uuid"
        if self.sequence_uuid:
            print_message(
                {},
                "sequence",
                f"sequence_uuid: {self.sequence_uuid} already exists",
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

    def set_dtime(self, offset: float = 0):
        dtime = datetime.now()
        dtime = datetime.fromtimestamp(dtime.timestamp() + offset)
        self.sequence_timestamp = dtime.strftime("%Y%m%d.%H%M%S%f")


class Process(Sequence):
    "Sample-process identifier class."

    def __init__(
        self,
        inputdict: dict = {},
    ):
        super().__init__(inputdict)  # grab sequence keys
        imports = {}
        imports.update(inputdict)
        


        # machine_name
        
        # timestamp # execution time, not the queue time
        # ordering # enum
        #
        
        self.process_uuid = imports.get("process_uuid", None)



        self.process_queue_time = imports.get("process_queue_time", None)
        self.process_server = imports.get("process_server", None)
        self.process_name = imports.get("process_name", None)
        self.process_params = imports.get("process_params", {})
        self.process_enum = imports.get("process_enum", None)
        self.process_abbr = imports.get("process_abbr", None)
        self.start_condition = imports.get("start_condition", 3)

        # holds samples basemodel for parsing between processes etc
        self.samples_in: hcms.SampleList = []
        self.samples_out: hcms.SampleList = []

        # the following attributes are set during process dispatch but can be imported
        self.file_dict = defaultdict(lambda: defaultdict(dict))  # TODO: replace with model
        self.file_dict.update(imports.get("file_dict", {}))

        # TODO: make the following attributes private
        self.save_prc = imports.get("save_prc", True)
        self.save_data = imports.get("save_data", True)
        self.plate_id = imports.get("plate_id", None)
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

        check_args = {"server": self.process_server, "name": self.process_name}
        missing_args = [k for k, v in check_args.items() if v is None]
        if missing_args:
            print_message(
                {},
                "process",
                f'process {" and ".join(missing_args)} not specified. Placeholder processes will only affect the process queue enumeration.',
                info=True,
            )

    def gen_uuid_process(self, machine_name: str):
        if self.process_uuid:
            print_message(
                {},
                "process",
                f"process_uuid: {self.process_uuid} already exists",
                error=True,
            )
        else:
            self.process_uuid = gen_uuid(
                label=f"{machine_name}_{self.process_name}",
                timestamp=self.process_queue_time,
            )
            print_message({}, "process", f"process_uuid: {self.process_uuid} assigned", info=True)

    def set_atime(self, offset: float = 0.0):
        atime = datetime.now()
        if offset is not None:
            atime = datetime.fromtimestamp(atime.timestamp() + offset)
        self.process_queue_time = atime.strftime("%Y%m%d.%H%M%S%f")


class Sequencer(object):
    def __init__(
        self,
        pg: Sequence,
    ):
        frame = inspect.currentframe().f_back
        _args, _varargs, _keywords, _locals = inspect.getargvalues(frame)
        self._pg = pg
        self.process_list = []
        self.pars = self._C()
        for key, val in self._pg.sequence_params.items():
            setattr(self.pars, key, val)  # we could also add it direcly to the class root by just using self

        for key, val in _locals.items():
            if key != "pg_Obj" and key not in self._pg.sequence_params.keys():
                print_message(
                    {},
                    "sequencer",
                    f"local var '{key}' not found in pg_Obj, addding it to sq.pars",
                    error=True,
                )
                setattr(
                    self.pars, key, val
                )  # we could also add it direcly to the class root by just using self

    class _C:
        pass

    def add_process(self, process_dict: dict):
        new_process_dict = self._pg.as_dict()
        new_process_dict.update(process_dict)
        self.process_list.append(Process(inputdict=new_process_dict))
