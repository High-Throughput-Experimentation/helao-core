from __future__ import annotations
""" sample.py
Liquid, Gas, Assembly, and Solid sample type models.

"""
__all__ = [
           "NoneSample",
           "SampleModel",
           "LiquidSample", 
           "GasSample", 
           "SolidSample", 
           "AssemblySample", 
           "SampleList",
           "SampleUnion",
           "object_to_sample",
           "SampleInheritance",
           "SampleStatus"
          ]

from socket import gethostname
from uuid import UUID
from enum import Enum

from pydantic import BaseModel, validator, root_validator, Field
from pydantic.tools import parse_obj_as

from typing import List, Optional, Union, Literal
from typing import ForwardRef

from ..helper.print_message import print_message
from ..version import get_hlo_version
from ..helper.helaodict import HelaoDict


SampleUnion = ForwardRef('SampleUnion')
SamplePartUnion = ForwardRef('SamplePartUnion')


class SampleInheritance(str, Enum):
    none = "none"
    give_only = "give_only"
    receive_only = "receive_only"
    allow_both = "allow_both"
    block_both = "block_both"


class SampleStatus(str, Enum):
    none = "none" 
    # pretty self-explanatory; the sample was created during the action.
    created = "created"
    # also self-explanatory
    destroyed = "destroyed"
    # the sample exists before and after the action. e.g. an echem experiment
    preserved = "preserved"
    # the sample was combined with others in the action. E.g. the creation of an electrode assembly from electrodes and electrolytes
    incorporated = "incorporated"
    # the opposite of incorporated. E.g. an electrode assembly is taken apart, and the original electrodes are recovered, and further experiments may be done on those electrodes
    recovered = "recovered"
    loaded = "loaded"
    unloaded = "unloaded"


class SampleModel(BaseModel, HelaoDict):
    """Bare bones sample with only the key identifying information of a sample in the database."""

    _hashinclude_ = {"global_label", "sample_type"}

    hlo_version: Optional[str] = get_hlo_version()
    global_label: Optional[str]  # is None for a ref sample
    sample_type: Optional[str]
  

class _BaseSample(SampleModel):
    """Full Sample with all helao-async relevant attributes."""

    # time related fields
    sample_creation_timecode: Optional[int]  # epoch in ns
    last_update: Optional[int]  # epoch in ns
    # action_timestamp: Optional[str]  # "%Y%m%d.%H%M%S%f"

    # labels
    sample_no: Optional[int]
    machine_name: Optional[str]
    sample_hash: Optional[str]
    server_name: Optional[str]

    # action related
    action_uuid: List[UUID] = Field(default_factory=list)
    sample_creation_action_uuid: Optional[UUID]
    sample_creation_experiment_uuid: Optional[UUID]

    # metadata
    sample_position: Optional[str]
    inheritance: Optional[SampleInheritance]  # only for internal use
    status: List[SampleStatus] = Field(default_factory=list)  # only for internal use
    chemical: List[str] = Field(default_factory=list)
    mass: List[str] = Field(default_factory=list)
    supplier: List[str] = Field(default_factory=list)
    lot_number: List[str] = Field(default_factory=list)
    source: List[str] = Field(default_factory=list)
    comment: Optional[str]


    def create_initial_prc_dict(self):
        if not isinstance(self.status, list):
            self.status = [self.status]

        return {
            "global_label": self.get_global_label(),
            "sample_type": self.sample_type,
            "sample_no": self.sample_no,
            "machine_name": self.machine_name if self.machine_name is not None else gethostname(),
            "sample_creation_timecode": self.sample_creation_timecode,
            "last_update": self.last_update,
            "sample_position":self.sample_position,
            "inheritance":self.inheritance,
            "status":self.status
        }


    def get_global_label(self):
         pass

        
    def update_vol(self, delta_vol_ml: float, dilute: bool):
        if hasattr(self, "volume_ml"):
            old_vol = self.volume_ml
            tot_vol = old_vol+delta_vol_ml
            if tot_vol < 0:
                print_message({}, "model", "new volume is < 0, setting it to zero.", error=True)
                tot_vol = 0
            self.volume_ml = tot_vol
            if dilute:
                if hasattr(self, "dilution_factor"):
                    old_df = self.dilution_factor
                    if old_vol <= 0:
                        print_message({}, "model", "previous volume is <= 0, setting new df to 0.", error=True)
                        new_df = -1
                    else:
                        new_df = tot_vol/(old_vol/old_df)
                    self.dilution_factor = new_df
                    print_message({}, "model", f"updated sample dilution-factor: {self.dilution_factor}", error=True)


    def get_vol_ml(self) -> float:
        if hasattr(self, "volume_ml"):
            return self.volume_ml
        else:
            return 0.0

        
    def get_dilution_factor(self) -> float:
        if hasattr(self, "dilution_factor"):
            return self.dilution_factor
        else:
            return 1.0
    
class NoneSample(SampleModel):
    sample_type: Literal[None] = None
    global_label: Literal[None] = None
    inheritance: Optional[SampleInheritance]  # only for internal use
    status: List[SampleStatus] = Field(default_factory=list)  # only for internal use

    def get_global_label(self):
        return None

    def get_vol_ml(self):
        return None


class LiquidSample(_BaseSample):
    """base class for liquid samples"""

    sample_type: Literal["liquid"] = "liquid"
    volume_ml: Optional[float] = 0.0
    ph: Optional[float] = None
    dilution_factor: Optional[float] = 1.0

    def prc_dict(self):
        prc_dict = self.create_initial_prc_dict()
        prc_dict.update({"volume_ml": self.volume_ml})
        prc_dict.update({"ph": self.ph})
        prc_dict.update({"dilution_factor": self.dilution_factor})
        return prc_dict

    def get_global_label(self):
        if self.global_label is None:
            label = None
            machine_name = self.machine_name if self.machine_name is not None else gethostname()
            label = f"{machine_name}__liquid__{self.sample_no}"
            return label
        else:
            return self.global_label


class SolidSample(_BaseSample):
    """base class for solid samples"""

    sample_type: Literal["solid"] = "solid"
    machine_name: Optional[str] = "legacy"
    plate_id: Optional[int] = None

    def prc_dict(self):
        prc_dict = self.create_initial_prc_dict()
        prc_dict.update({"plate_id": self.plate_id})
        return prc_dict

    def get_global_label(self):
        if self.global_label is None:
            label = None
            machine_name = self.machine_name if self.machine_name is not None else "legacy"
            label = f"{machine_name}__solid__{self.plate_id}_{self.sample_no}"
            return label
        else:
            return self.global_label


    @root_validator(pre=False, skip_on_failure=True)
    def validate_global_label(cls, values):
        machine_name = values.get("machine_name")
        plate_id = values.get("plate_id")
        sample_no = values.get("sample_no")
        if machine_name == "legacy":
            values["global_label"] = f"{machine_name}__solid__{plate_id}_{sample_no}"
            return values
        else:
            raise ValueError("Only legacy solid sample supported for now.")


class GasSample(_BaseSample):
    """base class for gas samples"""

    sample_type: Literal["gas"] = "gas"
    volume_ml: Optional[float] = 0.0
    dilution_factor: Optional[float] = 1.0

    def prc_dict(self):
        prc_dict = self.create_initial_prc_dict()
        prc_dict.update({"volume_ml": self.volume_ml})
        prc_dict.update({"dilution_factor": self.dilution_factor})
        return prc_dict

    def get_global_label(self):
        if self.global_label is None:
            label = None
            machine_name = self.machine_name if self.machine_name is not None else gethostname()
            label = f"{machine_name}__gas__{self.sample_no}"
            return label
        else:
            return self.global_label


class AssemblySample(_BaseSample):
    sample_type: Literal['assembly'] = "assembly"
    # parts: List[SampleUnion] = Field(default_factory=list)
    parts: List[SamplePartUnion] = Field(default_factory=list)
    sample_position: Optional[str] = "cell1_we"  # usual default assembly position

    def get_global_label(self):
        if self.global_label is None:
            label = None
            machine_name = self.machine_name if self.machine_name is not None else gethostname()
            label = f"{machine_name}__assembly__{self.sample_position}__{self.sample_creation_timecode}"
            return label
        else:
            return self.global_label


    @validator("parts", pre=True)
    def validate_parts(cls, value):
        if value is None:
            return []
        return value


    def prc_dict(self):
        prc_dict = self.create_initial_prc_dict()
        prc_dict.update({"assembly_parts": self.get_assembly_parts_prc_dict()})
        return prc_dict


    def get_assembly_parts_prc_dict(self):
        part_dict_list = []
        for part in self.parts:
            if part is not None:
                # return full dict
                # part_dict_list.append(part.prc_dict())
                # return only the label (preferred)
                part_dict_list.append(part.get_global_label())
            else:
                pass
        return part_dict_list


# TODO: this needs to be removed in the near future
# and all calls to SampleList replaced by SampleUnion
class SampleList(BaseModel, HelaoDict):
    """a combi basemodel which can contain all possible samples
    Its also a list and we should enforce samples as being a list"""

    samples: Optional[List[SampleUnion]] = Field(default_factory=list)


SampleUnion = Union[
                    AssemblySample,
                    NoneSample,
                    LiquidSample,
                    GasSample,
                    SolidSample,
                    ]


SamplePartUnion = Union[
                    # AssemblySample,
                    NoneSample,
                    LiquidSample,
                    GasSample,
                    SolidSample,
                    ]


def object_to_sample(data):
    return parse_obj_as(SampleUnion, data)


AssemblySample.update_forward_refs()
SampleList.update_forward_refs()
