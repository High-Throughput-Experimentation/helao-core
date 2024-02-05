# HELAO-core

HELAO-core encompasses the API data structures used by Caltech HTE group's instrument control software following [HELAO](https://doi.org/10.26434/chemrxiv-2021-kr87t) design principles. This package is **independent** of [HELAO-async](https://github.com/High-Throughput-Experimentation/helao-async). See the instrument control software contained in the HELAO-async repo is the working application of these structures.


## Requirements

- [miniconda](https://docs.conda.io/en/latest/miniconda.html)
- Python 3 (tested with Python 3.11)
- [pydantic](https://github.com/pydantic/pydantic) 2.5.2


## Installation

As HELAO-core is a dependency of HELAO-async, the executig the environment setup scripts in the HELAO-async repo will automatically install HELAO-core.

To manually install HELAO-core, open a miniconda prompt or PowerShell with an active conda profile and run the following commands.

    git clone https://github.com/High-Throughput-Experimentation/helao-core.git
    conda install -c conda-forge pydantic=2.5.2 pydantic-core=2.14.5 pydantic-settings=2.1.0
