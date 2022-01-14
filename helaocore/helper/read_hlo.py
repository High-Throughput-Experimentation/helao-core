__all__ = ["read_hlo"]

import json
import yaml
from pathlib import Path
from typing import Tuple
from collections import defaultdict

def read_hlo(path: str) -> Tuple[dict, dict]:
    "Parse .hlo file into tuple of dictionaries containing metadata and data."
    path_to_hlo = Path(path)
    with path_to_hlo.open() as f:
        lines = f.readlines()
        
    sep_index = lines.index('%%\n')
    
    meta = yaml.load("".join(lines[:sep_index]), Loader=yaml.CLoader)

    data = defaultdict(list)
    for line in lines[sep_index+1:]:
        line_dict = json.loads(line)
        for k,v in line_dict.items():
            data[k] += v
    
    return meta, data
