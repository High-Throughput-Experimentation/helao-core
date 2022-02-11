__all__ = [
           "StatusModel",
           "ActionServerModel",
           "GlobalStatusModel",
           "MachineModel"
          ]

from typing import Dict, Optional, Tuple, List
from uuid import UUID
from pydantic import BaseModel, Field


from .orchstatus import OrchStatus
from .machine import MachineModel
from .action import ActionModel
from .hlostatus import HloStatus
from ..helper.helaodict import HelaoDict
from ..error import error_codes


# additional finished categories which contain one of these
# will be added to their own categories, sequence defines priority
# all of these need additional "finish" else the action is still "active"
main_finished_status = [HloStatus.estopped, HloStatus.errored]


class StatusModel(BaseModel, HelaoDict):
    # one status model per endpoint    
    act: ActionModel


class EndpointModel(BaseModel, HelaoDict):
    endpoint_name: str
    # status is a dict (keyed by action uuid) 
    # which hold a dict of active actions
    active_dict: Dict[UUID, StatusModel] = Field(default_factory=dict)

    # holds the finished uuids
    # keyed by either main_finished_status or "finished"
    finished_dict: Dict[HloStatus, Dict[UUID, StatusModel]] = Field(default_factory=dict)

    # none is infinite
    max_uuids: Optional[int] = None
    # todo: - add local queue and priority lists here?


    def sort_status(self):
        del_keys = []
        for uuid, status in self.active_dict.items():
            # check if action is finished
            if HloStatus.finished in status.act.action_status:
                del_keys.append(uuid)
                
                is_sub_status = False
                for hlostatus in main_finished_status:
                    if hlostatus in status.act.action_status:
                        if hlostatus not in self.finished_dict:
                            is_sub_status = True
                            self.finished_dict[hlostatus] = dict()
                            break
                        self.finished_dict[hlostatus].update({uuid:status})

                # no main substatus, add it under finished key
                if not is_sub_status:
                    self.finished_dict[HloStatus.finished].update({uuid:status})

        # delete all finished actions from active_dict
        for key in del_keys:
            del self.active_dict[key]


    def clear_finished(self):
        """clears all status dicts except active_dict"""
        self.finished_dict = dict()


class ActionServerModel(BaseModel, HelaoDict):
    action_server: MachineModel
    # endpoints keyed by the name of the endpoint (action_name)
    endpoints: Dict[str, EndpointModel] = Field(default_factory=dict)

    
    def get_fastapi_json(self, action_name: Optional[str] = None):
        json_dict = {}
        if action_name is None:
            # send all
            json_dict = self.json_dict()
        else:
            # send only selected endpoint status
            if action_name in self.endpoints:
                json_dict = ActionServerModel(
                        action_server = self.action_server,
                        # status_msg should be a StatusModel
                        endpoints = {action_name:self.endpoints[action_name]}
                    
                ).json_dict()

        return json_dict


class GlobalStatusModel(BaseModel, HelaoDict):
    orchestrator: MachineModel
    # a dict of actionserversmodels keyed by the server name
    # use MachineModel.as_key() for the dict key
    server_dict: Dict[Tuple,ActionServerModel] = Field(default_factory=dict)

    # a dict of all active actions for this orch
    active_acts: Dict[UUID, StatusModel] = Field(default_factory=dict)
    # a dict of all finished actions 
    # keyed by either main_finished_status or "finished"
    finished_acts: Dict[HloStatus, Dict[UUID, StatusModel]] = Field(default_factory=dict)


    # some control parameters for the orch
    
    # new intented state for the dispatch loop
    loop_intent: OrchStatus = OrchStatus.none
    # the dispatch loop state
    loop_state: OrchStatus = OrchStatus.stopped
    # the state of the orch
    orch_state: OrchStatus = OrchStatus.none
    # counter for dispatched actions
    counter_dispatched_actions = 0


    def server_free(
                    self, 
                    action_server: MachineModel, 
                   ) -> bool:
        free = True
        if action_server.as_key() in self.server_dict:
            actionservermodel = self.server_dict[action_server.as_key()]
            for endpoint_name, endpointmodel in actionservermodel.endpoints:
                # loop through all of its active uuids
                for uuid, statusmodel in endpointmodel.active_dict:
                    if statusmodel.act.orchestrator == self.orchestrator:
                        # found an acive action for this orch
                        # endpoint is not yet free for this orch
                        free = False
                        break
        return free


    def endpoint_free(
                      self, 
                      action_server: MachineModel, 
                      endpoint_name: str
                     ) -> bool:
        """checks if an action server endpoint is available 
           for this orch"""
        free = True
        # check if the actio server is registered for this orch
        # if action_server.server_name in self.server_dict:
        if action_server.as_key() in self.server_dict:
            actionservermodel = self.server_dict[action_server.as_key()]
            # check if the action server has the requested endpoint
            if endpoint_name in actionservermodel.endpoints:
                endpointmodel = actionservermodel.endpoints[endpoint_name]
                # loop through all of its active uuids
                for uuid, statusmodel in endpointmodel.active_dict:
                    if statusmodel.act.orchestrator == self.orchestrator:
                        # found an acive action for this orch
                        # endpoint is not yet free for this orch
                        free = False
                        break

        return free


    def _sort_status(self):
        """sorts actions from server_dict
           into orch specific separate dicts"""

        # loop through all servers
        for action_server, actionservermodel \
        in self.server_dict.items():
            # loop through all endpoints on this server
            for action_name, endpointmodel \
            in actionservermodel.endpoints.items():
                # loop through all active uuids on this endpoint
                for uuid, statusmodel in endpointmodel.active_dict.items():
                    if statusmodel.act.orchestrator == self.orchestrator:
                        self.active_dict.update({uuid:statusmodel})
                # loop through all finished uuids on this endpoint
                for hlostatus, status_dict in endpointmodel.finished_dict.items():
                    for uuid, statusmodel in status_dict.items():
                        if statusmodel.act.orchestrator == self.orchestrator:
                            if hlostatus not in self.finished_dict:
                                self.finished_dict[hlostatus] = dict()
                            self.finished_dict[hlostatus].update({uuid:statusmodel})


    def update_global_with_acts(self, actionserver: ActionServerModel):
        if actionserver.action_server.as_key() not in self.server_dict:
            # add it for the first time
            self.server_dict.update(
                {actionserver.action_server.as_key():actionserver}
            )
        else:
            # cannot update the full active_dict
            # we need to update each endpoint individually
            for endpoint_name, endpointmodel in actionserver.endpoints.items():
                self.server_dict[actionserver.action_server.as_key()].endpoints.update(
                    {endpoint_name:endpointmodel}
                )

        # sort it into active and finished
        self._sort_status()


    def find_hlostatus_in_finished(
                        self, 
                        hlostatus: HloStatus
                       ) -> Dict[UUID, StatusModel]:
        """returns a dict of uuids for actions which contain hlostatus"""
        uuid_dict = dict()
        
        if hlostatus in self.finished_acts:
            # all of them have this status
            for uuid, statusmodel in self.finished_acts[hlostatus].items():
                uuid_dict.append({uuid:statusmodel})
        elif HloStatus.finished in self.finished_acts:
            # can only be in finsihed, but need to look for substatus
            for uuid, statusmodel in self.finished_acts[HloStatus.finished].items():
                if hlostatus in statusmodel.act.action_status:
                    uuid_dict.append({uuid:statusmodel})
            
        
        return uuid_dict


    def clear_in_finished(self, hlostatus: HloStatus):
        if hlostatus in self.finished_acts:
            self.finished_acts[hlostatus] = dict()
        elif HloStatus.finished in self.finished_acts:
            # can only be in finsihed, but need to look for substatus
            del_keys = []
            for uuid, statusmodel in self.finished_acts[HloStatus.finished].items():
                if hlostatus in statusmodel.act.action_status:
                    del_keys.append(uuid)

            # delete uuids
            for key in del_keys:
                del self.finished_acts[HloStatus.finished][key]


    def finish_experiment(self, exp_uuid: UUID) -> List[ActionModel]:
        """ returns all finished experiments"""
        # we don't filter by orch as this should have happened already when they 
        # were added to the finished_dict
        finished_acts = []
        for hlostatus, status_dict in self.finished_acts.items():
            for uuid, statusmodel in status_dict.items():
                # TODO all acts should contain "finished", else 
                # something went wrong
                # if HloStatus.finished not in statusmodel.act.action_status:
                #     ERROR
               finished_acts.append(statusmodel.act)

        # if self.active_acts:
        #     ERROR

        # clear finished
        self.finished_acts = dict()
        self.counter_dispatched_actions = 0

        return finished_acts
