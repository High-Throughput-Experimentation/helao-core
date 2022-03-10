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

import os
import inspect
from copy import deepcopy
from pathlib import Path
from typing import Optional
from pydantic import Field
from typing import List
from collections import defaultdict
from uuid import UUID

from .helper.print_message import print_message
from .helper.gen_uuid import gen_uuid
from .helper.set_time import set_time
from .model.action import ActionModel, ShortActionModel
from .model.experiment import (
                               ExperimentModel, 
                               ShortExperimentModel, 
                               ExperimentTemplate
                              )
from .model.experiment_sequence import ExperimentSequenceModel
from .model.hlostatus import HloStatus
from .model.action_start_condition import ActionStartCondition
from .model.machine import MachineModel
# from .error import ErrorCodes

class Sequence(ExperimentSequenceModel):
    # not in ExperimentSequenceModel:

    #this holds experiments from an active sequence
    experimentmodel_list: List[ExperimentModel] = Field(default_factory=list)

    def __repr__(self):
        return f"<sequence_name:{self.sequence_name}>" 


    def __str__(self):
        return f"sequence_name:{self.sequence_name}" 


    def get_seq(self):
        seq = ExperimentSequenceModel(**self.dict())
        seq.experiment_list = [ShortExperimentModel(**prc.dict()) for prc in self.experimentmodel_list]
        # either we have a plan at the beginning or not
        # don't add it later from the experimentmodel_list
        # seq.experiment_plan_list = [ExperimentTemplate(**prc.dict()) for prc in self.experimentmodel_list]
        return seq


    def init_seq(
                 self, 
                 time_offset: float = 0,
                 force: Optional[bool] = None
                ):
        if force is None:
            force = False
        if force or self.sequence_timestamp is None:
            self.sequence_timestamp = set_time(offset = time_offset)
        if force or self.sequence_uuid is None:
            self.sequence_uuid = gen_uuid()
        if force or not self.sequence_status:
            self.sequence_status = [HloStatus.active]
        if force or self.sequence_output_dir is None:
            self.sequence_output_dir = self.get_sequence_dir()


    def get_sequence_dir(self):
        HMS = self.sequence_timestamp.strftime("%H%M%S")
        year_week = self.sequence_timestamp.strftime("%y.%U")
        sequence_date = self.sequence_timestamp.strftime("%Y%m%d")

        return os.path.join(
            year_week,
            sequence_date,
            f"{HMS}__{self.sequence_name}__{self.sequence_label}",
        )


class Experiment(Sequence, ExperimentModel):
    "Sample-action grouping class."

    # not in ExperimentModel:
    result_dict: Optional[dict] = Field(default_factory=dict)
    global_params: Optional[dict] = Field(default_factory=dict)
    experiment_action_list: List[ActionModel] = Field(default_factory=list)


    def __repr__(self):
        return f"<experiment_name:{self.experiment_name}>" 


    def __str__(self):
        return f"experiment_name:{self.experiment_name}" 


    def init_prc(
                 self, 
                 time_offset: float = 0,
                 force: Optional[bool] = None
                ):
        if force is None:
            force = False
        if force or self.experiment_timestamp is None:
            self.experiment_timestamp = set_time(offset = time_offset)
        if force or self.experiment_uuid is None:
            self.experiment_uuid = gen_uuid()
        if force or not self.experiment_status:
            self.experiment_status = [HloStatus.active]
        if force or self.experiment_output_dir is None:
            self.experiment_output_dir = self.get_experiment_dir()


    def get_experiment_dir(self):
        """accepts action or experiment object"""
        experiment_time = self.experiment_timestamp.strftime("%H%M%S%f")
        sequence_dir = self.get_sequence_dir()
        return os.path.join(
            sequence_dir,
            f"{experiment_time}__{self.experiment_name}",
        )


    def get_prc(self):
        prc = ExperimentModel(**self.dict())
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
            tmp_sample = deepcopy(sample)
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


class Action(Experiment, ActionModel):
    "Sample-action identifier class."
    # not in ActionModel:
    start_condition: Optional[ActionStartCondition] = ActionStartCondition.wait_for_all
    save_act: Optional[bool] = True # default should be true
    save_data: Optional[bool] = True # default should be true
    AUX_file_paths: Optional[List[Path]] = Field(default_factory=list)

    # moved to ActionModel
    # error_code: Optional[ErrorCodes] = ErrorCodes.none

    from_global_params: Optional[dict] = Field(default_factory=dict)
    to_global_params: Optional[list] = Field(default_factory=list)

    # internal
    file_conn_keys: Optional[List[UUID]] = Field(default_factory=list)

    def __repr__(self):
        return f"<action_name:{self.action_name}>" 


    def __str__(self):
        return f"action_name:{self.action_name}" 


    def get_act(self):
        return ActionModel(**self.dict())


    def init_act(
                 self, 
                 time_offset: float = 0,
                 force: Optional[bool] = None
                ):
        if self.sequence_timestamp is None \
        or self.experiment_timestamp is None:
            self.manual_action = True
            self.access = "manual"
            # -- (1) -- set missing sequence parameters
            self.sequence_name = "manual_seq"
            self.init_seq(time_offset=time_offset)
            # -- (2) -- set missing experiment parameters
            self.experiment_name = "MANUAL"
            self.init_prc(time_offset=time_offset)

        if force or self.action_timestamp is None:
            self.action_timestamp = set_time(offset=time_offset)
        if force or self.action_uuid is None:
            self.action_uuid = gen_uuid()
        if force or not self.action_status:
            self.action_status = [HloStatus.active]
        if force or self.action_output_dir is None:
            self.action_output_dir = self.get_action_dir()


    def get_action_dir(self):
        experiment_dir = self.get_experiment_dir()
        return os.path.join(
            experiment_dir,
            f"{self.orch_submit_order}__"
            f"{self.action_split}__"
            f"{self.action_timestamp.strftime('%Y%m%d.%H%M%S%f')}__"
            f"{self.action_server.server_name}__{self.action_name}",
        )


class ActionPlanMaker(object):
    def __init__(self):
        frame = inspect.currentframe().f_back
        _args, _varargs, _keywords, _locals = inspect.getargvalues(frame)
        self.expname = frame.f_code.co_name
        self._experiment = None
        self.action_list = []
        self.pars = self._C()

        exp_paramdict = dict()

        # find the Experiment Basemodel
        # and add all other params to a dict
        for arg in _args:
            argparam = _locals.get(arg, None)
            if isinstance(argparam, Experiment):
                if self._experiment is None:
                    print_message({}, "actionplanmaker", 
                                     f"{self.expname}: found Experiment BaseModel under "
                                     f"parameter '{arg}'",
                                     info = True)
                    self._experiment = deepcopy(argparam)
                else:
                    print_message({}, "actionplanmaker", 
                                  f"{self.expname}: critical error: "
                                  f"found another Experiment BaseModel"
                                  f" under parameter '{arg}',"
                                  f" skipping it",
                                  error=True,
                                  )
            else:
                exp_paramdict.update({arg:argparam})

        # check if an Experiment was detected
        if self._experiment is None:
            print_message({}, "actionplanmaker", 
               f"{self.expname}: critical error: "
               f"no Experiment BaseModel was found "
               f"by ActionPlanMaker, "
               f"using blank Experiment.",
               error=True,
            )
            self._experiment = Experiment()


        # add all experiment_params under self.pars
        if self._experiment.experiment_params is not None:
            for key, val in self._experiment.experiment_params.items():
                setattr(self.pars, key, val)

        # add all other params in exp_paramdict which were
        # not included in experiment_params to self.pars
        # for key, val in _locals.items():
        for key, val in exp_paramdict.items():
            if key not in self._experiment.experiment_params.keys():
                print_message(
                    {},
                    "ActionPlanMaker",
                    f"{self.expname}: local var '{key}'"
                    f" not found in Experiment, "
                    f"adding it to self.pars",
                    error=True,
                )
                setattr(
                    self.pars, key, val
                )

        print_message(
            {},
            "ActionPlanMaker",
            f"{self.expname}: params in self.pars are:"
            f" {vars(self.pars)}",
            info=True,
        )


    class _C:
        pass


    def add_action(self, action_dict: dict):
        new_action_dict = self._experiment.as_dict()
        new_action_dict.update(action_dict)
        self.action_list.append(Action(**new_action_dict))


    def add_action_list(self, action_list: list):
        for action in action_list:
            self.action_list.append(action)

    def add(
        self,
        action_server: dict,
        action_name: str,
        action_params: dict,
        start_condition: ActionStartCondition = ActionStartCondition.wait_for_all,
        **kwargs
    ):
        """Shorthand add_action()."""
        action_dict = self._experiment.as_dict()
        action_dict.update({
            "action_server": action_server,
            "action_name": action_name,
            "action_params": action_params,
            "start_condition": start_condition
        })
        action_dict.update(kwargs)
        self.action_list.append(Action(**action_dict))


class ExperimentPlanMaker(object):
    def __init__(
        self,
    ):
        self.experiment_plan_list = []


    def add_experiment(self, selected_experiment, experiment_params):
        self.experiment_plan_list.append(
            ExperimentTemplate(
                experiment_name = selected_experiment,
                experiment_params = experiment_params,
                )
            )
