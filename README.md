[![PyPI version fury.io](https://badge.fury.io/py/meteostations-geopy.svg)](https://pypi.python.org/pypi/meteostations-geopy)
[![Documentation Status](https://readthedocs.org/projects/meteostations-geopy/badge/?version=latest)](https://meteostations-geopy.readthedocs.io/en/latest/?badge=latest)
[![CI/CD](https://github.com/martibosch/meteostations-geopy/actions/workflows/dev.yml/badge.svg)](https://github.com/martibosch/meteostations-geopy/blob/main/.github/workflows/dev.yml)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/martibosch/meteostations-geopy/main.svg)](https://results.pre-commit.ci/latest/github/martibosch/meteostations-geopy/main)
[![codecov](https://codecov.io/gh/martibosch/meteostations-geopy/branch/main/graph/badge.svg?token=hKoSSRn58a)](https://codecov.io/gh/martibosch/meteostations-geopy)
[![GitHub license](https://img.shields.io/github/license/martibosch/meteostations-geopy.svg)](https://github.com/martibosch/meteostations-geopy/blob/main/LICENSE)

# Meteostations geopy

Pythonic interface to access data from meteorological stations

## Installation

Although meteostations-geopy is not available in PyPI and conda-forge yet (hopefully will be soon), it can be installed using conda/mamba and pip as follows:

```bash
# install GDAL-based requirements
conda install -c conda-forge contextily geopandas osmnx
# install meteostations-geopy from GitHub
pip install https://github.com/martibosch/meteostations-geopy/archive/main.zip
```

## Usage

## See also

This library intends to provide a unified way to access data from meteorological stations from multiple providers. The following libraries provide access to data from a specific provider:

- [martibosch/agrometeo-geopy](https://github.com/martibosch/agrometeo-geopy)
- [martibosch/netatmo-geopy](https://github.com/martibosch/netatmo-geopy)

Eventually these packages will be fully integrated into meteostations-geopy.

## Acknowledgements

- Many utils such as the requests cache mechanism or the logging system are based on code from [gboeing/osmnx](https://github.com/gboeing/osmnx).
- This package was created with the [martibosch/cookiecutter-geopy-package](https://github.com/martibosch/cookiecutter-geopy-package) project template.
- With the support of the École Polytechnique Fédérale de Lausanne (EPFL).
