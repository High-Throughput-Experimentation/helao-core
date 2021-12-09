""" sample.py
Liquid, Gas, Assembly, and Solid sample type models.

"""
__all__ = [
           "SampleModel",
           "LiquidSample", 
           "GasSample", 
           "SolidSample", 
           "AssemblySample", 
           "SampleList",
           "SampleUnion"
          ]

from datetime import datetime
from socket import gethostname
from typing import List, Optional, Union

from pydantic import BaseModel, validator, root_validator

from helaocore.helper import print_message
from helaocore.server import version


def _sample_model_list_validator(model_list, values, **kwargs):
    """validates samples models in a list"""

    def dict_to_model(model_dict):
        sample_type = model_dict.get("sample_type", None)
        if sample_type is None:
            return None
        elif sample_type == "liquid":
            return LiquidSample(**model_dict)
        elif sample_type == "gas":
            return GasSample(**model_dict)
        elif sample_type == "solid":
            return SolidSample(**model_dict)
        elif sample_type == "assembly":
            return AssemblySample(**model_dict)
        else:
            print_message({}, "model", f"unsupported sample_type '{sample_type}'", error=True)
            raise ValueError("model", f"unsupported sample_type '{sample_type}'")

    if model_list is None or not isinstance(model_list, list):
        print_message(
            {},
            "model",
            f"validation error, type '{type(model_list)}' is not a valid sample model list",
            error=True,
        )
        raise ValueError("must be valid sample model list")

    for i, model in enumerate(model_list):
        if isinstance(model, dict):
            model_list[i] = dict_to_model(model)
        elif isinstance(model, LiquidSample):
            continue
        elif isinstance(model, SolidSample):
            continue
        elif isinstance(model, GasSample):
            continue
        elif isinstance(model, AssemblySample):
            continue
        elif model is None:
            continue
        else:
            print_message(
                {},
                "model",
                f"validation error, type '{type(model)}' is not a valid sample model",
                error=True,
            )
            raise ValueError("must be valid sample model")

    return model_list



class SampleModel(BaseModel):
    hlo_version: Optional[str] = version.hlo_version
    global_label: Optional[str]  # is None for a ref sample
    sample_type: Optional[str]


class Sample(BaseModel):
    # Main base parameter which are fixed.
    # A Sample ref would have no global label 
    # only a sample_type.
    # 
    hlo_version: Union[str, None] = version.hlo_version
    global_label: Optional[str] = None # is None for a ref sample
    sample_type: Optional[str] = None


class _BaseSample(SampleModel):
    # additional parameters
    sample_no: Optional[int] = None
    machine_name: Optional[str] = None
    sample_creation_timecode: Optional[int] = None  # epoch in ns
    sample_position: Optional[str] = None
    sample_hash: Optional[str] = None
    last_update: Optional[int] = None  # epoch in ns
    inheritance: Optional[str] = None  # only for internal use
    status: Union[List[str], str] = None  # only for internal use
    process_uuid: Optional[str] = None
    action_uuid: Optional[str] = None
    action_timestamp: Optional[str] = None  # "%Y%m%d.%H%M%S%f"
    server_name: Optional[str] = None
    chemical: Optional[List[str]] = []
    mass: Optional[List[str]] = []
    supplier: Optional[List[str]] = []
    lot_number: Optional[List[str]] = []
    source: Union[List[str], str] = None
    comment: Optional[str] = None


    @validator("action_timestamp")
    def validate_action_timestamp(cls, v):
        if v is not None:
            if v != "00000000.000000000000":
                try:
                    atime = datetime.strptime(v, "%Y%m%d.%H%M%S%f")
                except ValueError:
                    print_message({}, "model", f"invalid 'action_timestamp': {v}", error=True)
                    raise ValueError("invalid 'action_timestamp'")
                return atime.strftime("%Y%m%d.%H%M%S%f")
            else:
                return v
        else:
            return None

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


    def get_vol_ml(self):
        if hasattr(self, "volume_ml"):
            return self.volume_ml
        else:
            return 0.0

        
    def get_dilution_factor(self):
        if hasattr(self, "dilution_factor"):
            return self.dilution_factor
        else:
            return 1.0
    


class LiquidSample(_BaseSample):
    """base class for liquid samples"""

    sample_type: Optional[str] = "liquid"
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

    @validator("sample_type")
    def validate_sample_type(cls, v):
        if v != "liquid":
            print_message(
                {},
                "model",
                f"validation liquid in solid_sample, got type '{v}'",
                error=True,
            )
            # return "liquid"
            raise ValueError("must be liquid")
        return "liquid"


class SolidSample(_BaseSample):
    """base class for solid samples"""

    sample_type: Optional[str] = "solid"
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

    @validator("sample_type")
    def validate_sample_type(cls, v):
        if v != "solid":
            print_message(
                {},
                "model",
                f"validation error in solid_sample, got type '{v}'",
                error=True,
            )
            # return "solid"
            raise ValueError("must be solid")
        return "solid"

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

    sample_type: Optional[str] = "gas"
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

    @validator("sample_type")
    def validate_sample_type(cls, v):
        if v != "gas":
            print_message(
                {},
                "model",
                f"validation error in gas_sample, got type '{v}'",
                error=True,
            )
            # return "gas"
            raise ValueError("must be gas")
        return "gas"


class AssemblySample(_BaseSample):
    sample_type: Optional[str] = "assembly"
    parts: Optional[Union[list, None]] = []
    sample_position: Optional[str] = "cell1_we"  # usual default assembly position

    def get_global_label(self):
        if self.global_label is None:
            label = None
            machine_name = self.machine_name if self.machine_name is not None else gethostname()
            label = f"{machine_name}__assembly__{self.sample_position}__{self.sample_creation_timecode}"
            return label
        else:
            return self.global_label

    @validator("parts")
    def validate_parts(cls, value, values, **kwargs):
        if value is None:
            return []
        else:
            return _sample_model_list_validator(value, values, **kwargs)

    @validator("sample_type")
    def validate_sample_type(cls, v):
        if v != "assembly":
            print_message({}, "model", f"validation error in assembly, got type '{v}'", error=True)
            # return "assembly"
            raise ValueError("must be assembly")
        return "assembly"

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
                # part_dict_list.append(None)
                pass
        return part_dict_list


class SampleList(BaseModel):
    """a combi basemodel which can contain all possible samples
    Its also a list and we should enforce samples as being a list"""

    samples: Optional[list] = []  # don't use union of models, that does not work here

    @validator("samples")
    def validate_samples(cls, value, values, **kwargs):
        return _sample_model_list_validator(value, values, **kwargs)


SampleUnion = Union[AssemblySample, SolidSample, LiquidSample, GasSample]
