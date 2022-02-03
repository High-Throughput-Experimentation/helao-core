""" schema.py
Standard classes for experiment queue objects.

"""

__all__ = [
           "Sequence", 
           "Experiment", 
           "Action", 
           "ActionPlanMaker",
           "ExperimentPlanMaker"
          ]


import inspect
import copy
from pathlib import Path
from typing import Optional
from datetime import datetime
from pydantic import BaseModel
from typing import List
from uuid import UUID
from collections import defaultdict


from .helper.print_message import print_message
from .helper.gen_uuid import gen_uuid
from .helper.set_time import set_time
from .helper.helaodict import HelaoDict
from .model.action import ActionModel, ShortActionModel
from .model.experiment import ExperimentModel, ShortExperimentModel
from .model.experiment_sequence import ExperimentSequenceModel
from .model.machine import MachineModel
from .version import get_hlo_version


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
        self.sequence_status = imports.get("sequence_status", [])
        self.sequence_name = imports.get("sequence_name", None)
        self.sequence_label = imports.get("sequence_label", "noLabel")
        self.sequence_params = imports.get("sequence_params", {})
        if self.sequence_params is None:
            self.sequence_params = {}
        self.experiment_plan_list = imports.get("experiment_plan_list", [])
        self.experimentmodel_list: List[ExperimentModel] = []
        self.sequence_output_dir = None


        def _to_time(Basemodel):
            time: datetime

    def __repr__(self):
        return f"<sequence_name:{self.sequence_name}>" 


    def __str__(self):
        return f"sequence_name:{self.sequence_name}" 


    def gen_uuid_sequence(self):
        "server_name can be any string used in generating random uuid"
        if self.sequence_uuid:
            print_message(
                {},
                "experiment",
                f"experiment_uuid: {self.experiment_uuid} already exists",
                info=True,
            )
        else:
            self.sequence_uuid = gen_uuid()
            print_message(
                {},
                "sequence",
                f"sequence_uuid: {self.sequence_uuid} assigned",
                info=True,
            )


    def set_sequence_time(self, offset: float = 0):
        self.sequence_timestamp = set_time(offset)


    def get_seq(self):
        return ExperimentSequenceModel(
            hlo_version=f"{get_hlo_version()}",
            sequence_name = self.sequence_name,
            sequence_params = self.sequence_params,
            sequence_label = self.sequence_label,
            sequence_uuid = self.sequence_uuid,
            sequence_timestamp = self.sequence_timestamp,
            sequence_status = self.sequence_status,
            experiment_plan_list = [experiment.experiment_name for experiment in self.experiment_plan_list],
            output_dir = Path(self.sequence_output_dir).as_posix() \
                            if self.sequence_output_dir is not None else None,
            experiment_list = [ShortExperimentModel(**prc.dict()) for prc in self.experimentmodel_list]

        )

    
    def init_seq(self, time_offset: float = 0):
        if self.sequence_timestamp is None:
            self.set_sequence_time(offset=time_offset)
        if self.sequence_uuid is None:
            self.gen_uuid_sequence()



class Experiment(Sequence):
    "Sample-action grouping class."

    def __init__(
        self,
        inputdict: dict = {},
    ):
        super().__init__(inputdict)  # grab sequence keys
        imports = {}
        imports.update(inputdict)


        # main experiment parameters
        self.experiment_uuid = imports.get("experiment_uuid", None) #
        self.experiment_status = imports.get("experiment_status", [])
        

        # others parameter
        self.orchestrator = MachineModel(**imports.get("orchestrator", MachineModel().dict()))
        
        self.experiment_timestamp = _to_datetime(time = imports.get("experiment_timestamp", None)).time
        self.access = imports.get("access", "hte")
        self.experiment_name = imports.get("experiment_name", None)
        self.experiment_params = imports.get("experiment_params", {})
        if self.experiment_params is None:
            self.experiment_params = {}

        # name of "instrument": sdc, anec, adss etc. defined in world config
        self.technique_name = imports.get("technique_name", None)

        # TODO: make the following attributes private
        self.result_dict = {}  # imports.get("result_dict", {})# this gets big really fast, bad for debugging
        self.global_params = {}  # TODO: reserved for internal use, do not write to .prg

        self.experiment_output_dir = None
        self.experiment_action_uuid_list: List[UUID] = []#imports.get("action_uuid_list", None)
        self.experiment_action_list: List[ActionModel] = []


    def __repr__(self):
        return f"<experiment_name:{self.experiment_name}>" 


    def __str__(self):
        return f"experiment_name:{self.experiment_name}" 


    def gen_uuid_experiment(self):
        "server_name can be any string used in generating random uuid"
        if self.experiment_uuid:
            print_message(
                {},
                "experiment",
                f"experiment_uuid: {self.experiment_uuid} already exists",
                info=True,
            )
        else:
            self.experiment_uuid = gen_uuid()
            print_message(
                {},
                "experiment",
                f"experiment_uuid: {self.experiment_uuid} assigned",
                info=True,
            )

    def set_dtime(self, offset: float = 0):
        self.experiment_timestamp = set_time(offset)


    def get_prc(self):
        
        prc = ExperimentModel(
            hlo_version=f"{get_hlo_version()}",
            orchestrator=self.orchestrator,
            sequence_uuid=self.sequence_uuid,
            access=self.access,
            experiment_uuid=self.experiment_uuid,
            experiment_timestamp=self.experiment_timestamp,
            experiment_status = self.experiment_status,
            technique_name=self.technique_name,
            experiment_name=self.experiment_name,
            experiment_params=self.experiment_params,
            output_dir = Path(self.experiment_output_dir).as_posix() \
                if self.experiment_output_dir is not None else None,
            # action_list = 
            # samples_in = 
            # samples_out = 
            # files = 
        )
        
        # now add all actions
        self._experiment_update_from_actlist(prc = prc)
        return prc


    def _experiment_update_from_actlist(self, prc: ExperimentModel):

        if self.experiment_action_list is None:
            self.experiment_action_list = []


        for actm in self.experiment_action_list:
            prc.action_list.append(ShortActionModel(**actm.dict()))

            for file in actm.files:
                if file.action_uuid is None:
                    file.action_uuid = actm.action_uuid
                prc.files.append(file)

            for _sample in actm.samples_in:
                identical = self._check_sample(
                                    new_sample = _sample,
                                    sample_list = prc.samples_in
                                    )
                if identical is None:
                    _sample.action_uuid = []
                    _sample.action_uuid.append(actm.action_uuid)
                    prc.samples_in.append(_sample)
                else:
                    prc.samples_in[identical].action_uuid.append(actm.action_uuid)

            for _sample in actm.samples_out:
                identical = self._check_sample(
                                    new_sample = _sample,
                                    sample_list = prc.samples_out
                                    )
                if identical is None:
                    _sample.action_uuid = []
                    _sample.action_uuid.append(actm.action_uuid)
                    prc.samples_out.append(_sample)
                else:
                    prc.samples_out[identical].action_uuid.append(actm.action_uuid)

        self._check_sample_duplicates(prc = prc)


    def _check_sample(self, new_sample, sample_list):
        for idx, sample in enumerate(sample_list):
            tmp_sample = copy.deepcopy(sample)
            tmp_sample.action_uuid = []
            identical = tmp_sample == new_sample
            if identical:
                return idx
        return None

    def _check_sample_duplicates(self, prc: ExperimentModel):
        out_labels = defaultdict(list)
        in_labels = defaultdict(list)
        for i, sample in enumerate(prc.samples_out):
            out_labels[sample.get_global_label()].append(i)
        for i, sample in enumerate(prc.samples_in):
            in_labels[sample.get_global_label()].append(i)

        isunique = True
        for key, locs in in_labels.items():
            if len(locs)>1:
                isunique = False

        if not isunique:
            print_message({}, "experiment", 
                          "\n----------------------------------"
                          "\nDuplicate but 'unique' samples."
                          "\nExperiment needs to be split."
                          "\n----------------------------------", error=True)
            print_message({}, "experiment", 
                          f"samples_in labels: {in_labels}", error = True)
            print_message({}, "experiment", 
                          f"samples_out labels: {out_labels}", error = True)


class Action(Experiment):
    "Sample-action identifier class."

    def __init__(
        self,
        inputdict: dict = {},
        act: ActionModel = None,
    ):
        super().__init__(inputdict)  # grab experiment keys
        imports = {}
        imports.update(inputdict)
        
        
        # overwrite some other params from seq, prc
        # usually also in dict (but we want to remove that in the future)
        self.technique_name = act.technique_name
        self.orchestrator = act.orchestrator
        self.experiment_uuid = act.experiment_uuid
        self.experiment_timestamp = act.experiment_timestamp
        self.access = act.access
        
        
        # main fixed parameters for Action
        self.action_uuid = act.action_uuid
        self.action_timestamp = act.action_timestamp
        self.action_status = act.action_status
        self.action_order = act.action_order
        self.action_retry = act.action_retry
        self.action_actual_order = act.action_actual_order
        self.action_split = act.action_split

        # name of the action server
        self.action_server = act.action_server

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
        # holds paramters for file connections for the data logger
        # self.file_conn_list: List[FileConn] = Field(default_factory=list)

        # TODO: make the following attributes private
        self.save_act = imports.get("save_act", True) # default should be true
        self.save_data = imports.get("save_data", True) # default should be true
        self.AUX_file_paths = imports.get("file_paths", [])


        # this hold all data for the action
        # can get really big
        self.data = imports.get("data", [])  # will be written to .hlo file

        # self.column_names = imports.get("column_names", None)  # deprecated in .hlo file format
        # self.header = imports.get("header", None)  # deprecated in .hlo file format
        # self.file_type = imports.get("file_type", None)
        # self.filename = imports.get("filename", None)
        # self.json_data_keys = imports.get("json_data_keys", None)
        # self.file_sample_label = imports.get("file_sample_label", None)
        # self.file_conn_keys = imports.get("file_conn_keys", None)
        # self.file_group = imports.get("file_group", None)
        self.error_code = imports.get("error_code", "0")

        self.from_global_params = imports.get("from_global_params", {})
        self.to_global_params = imports.get("to_global_params", [])


    def __repr__(self):
        return f"<action_name:{self.action_name}>" 


    def __str__(self):
        return f"action_name:{self.action_name}" 


    def gen_uuid_action(self):
        if self.action_uuid:
            print_message(
                {},
                "action",
                f"action_uuid: {self.action_uuid} already exists",
                error=True,
            )
        else:
            self.action_uuid = gen_uuid()
            print_message({}, "action", f"action_uuid: {self.action_uuid} assigned", info=True)

    def set_atime(self, offset: float = 0.0):
        self.action_timestamp = set_time(offset)


    def get_act(self):
        return ActionModel(
            hlo_version=f"{get_hlo_version()}",
            technique_name=self.technique_name,
            action_server=self.action_server,
            orchestrator=self.orchestrator,
            access=self.access,
            output_dir=Path(self.output_dir).as_posix() \
                if self.output_dir is not None else None,
            experiment_uuid=self.experiment_uuid,
            experiment_timestamp=self.experiment_timestamp,
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

    def init_act(self, time_offset: float = 0):
        if self.action_timestamp is None:
            self.set_atime(offset=time_offset)
        if self.action_uuid is None:
            self.gen_uuid_action()



class ActionPlanMaker(object):
    def __init__(
        self,
        pg: Experiment,
    ):
        frame = inspect.currentframe().f_back
        _args, _varargs, _keywords, _locals = inspect.getargvalues(frame)
        self._pg = copy.deepcopy(pg)
        self.action_list = []
        self.pars = self._C()
        if self._pg.experiment_params is not None:
            for key, val in self._pg.experiment_params.items():
                setattr(self.pars, key, val)  # we could also add it direcly to the class root by just using self

        for key, val in _locals.items():
            if key != "pg_Obj" and key not in self._pg.experiment_params.keys():
                print_message(
                    {},
                    "experimentr",
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


class ExperimentPlanMaker(object):
    def __init__(
        self,
    ):
        self.experiment_plan_list = []


    def add_experiment(self, selected_experiment, experiment_params):
        D = Experiment(inputdict={
            "experiment_name":selected_experiment,
            "experiment_params":experiment_params,
        })
        self.experiment_plan_list.append(D.as_dict())
