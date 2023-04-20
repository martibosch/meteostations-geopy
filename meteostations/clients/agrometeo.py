"""Agrometeo client."""
from typing import Any, Mapping, Union

import pandas as pd

from meteostations.base import BaseClient
from meteostations.mixins.region import RegionMixin, RegionType
from meteostations.mixins.stations import AllStationsEndpointMixin
from meteostations.mixins.variables import VariablesEndpointMixin

# API endpoints
BASE_URL = "https://www.agrometeo.ch/backend/api"
STATIONS_ENDPOINT = f"{BASE_URL}/stations"
VARIABLES_ENDPOINT = f"{BASE_URL}/sensors"

# useful constants
LONLAT_CRS = "epsg:4326"
LV03_CRS = "epsg:21781"
# ACHTUNG: for some reason, the API mixes up the longitude and latitude columns ONLY in
# the CH1903/LV03 projection. This is why we need to swap the columns in the dict below.
GEOM_COL_DICT = {LONLAT_CRS: ["long_dec", "lat_dec"], LV03_CRS: ["lat_ch", "long_ch"]}
DEFAULT_CRS = LV03_CRS
# variables name column
VARIABLES_NAME_COL = "name.en"
# API_DT_FMT = "%Y-%m-%d"
SCALE = "none"
MEASUREMENT = "avg"


class AgrometeoClient(
    RegionMixin, AllStationsEndpointMixin, VariablesEndpointMixin, BaseClient
):
    """Agrometeo client."""

    _stations_endpoint = STATIONS_ENDPOINT
    _variables_endpoint = VARIABLES_ENDPOINT

    def __init__(
        self,
        region: RegionType,
        crs: Any = None,
        variables_name_col: Union[str, None] = None,
        sjoin_kws: Union[Mapping, None] = None,
    ) -> None:
        """Initialize Agrometeo client."""
        # ACHTUNG: CRS must be set before region
        self.CRS = crs or DEFAULT_CRS
        self._variables_name_col = variables_name_col or VARIABLES_NAME_COL
        self.X_COL, self.Y_COL = GEOM_COL_DICT[self.CRS]
        self.region = region
        if sjoin_kws is None:
            sjoin_kws = {}
        self.SJOIN_KWS = sjoin_kws

    def _stations_df_from_json(self, response_json: dict) -> pd.DataFrame:
        return pd.DataFrame(response_json["data"])

    def _variables_df_from_json(self, response_json: dict) -> pd.DataFrame:
        variables_df = pd.json_normalize(response_json["data"])
        # ACHTUNG: need to strip strings, at least in variables name column. Note
        # that *it seems* that the integer type of variable code column is inferred
        # correctly
        variables_df[self._variables_name_col] = variables_df[
            self._variables_name_col
        ].str.strip()
        return variables_df
