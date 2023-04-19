"""Agrometeo client."""
from typing import Any, Mapping, Union

import pandas as pd

from meteostations.base import BaseClient
from meteostations.mixins.region import RegionMixin, RegionType
from meteostations.mixins.stations import AllStationsEndpointMixin

# API endpoints
BASE_URL = "https://www.agrometeo.ch/backend/api"
STATIONS_ENDPOINT = f"{BASE_URL}/stations"

# useful constants
LONLAT_CRS = "epsg:4326"
LV03_CRS = "epsg:21781"
# ACHTUNG: for some reason, the API mixes up the longitude and latitude columns ONLY in
# the CH1903/LV03 projection. This is why we need to swap the columns in the dict below.
GEOM_COL_DICT = {LONLAT_CRS: ["long_dec", "lat_dec"], LV03_CRS: ["lat_ch", "long_ch"]}
DEFAULT_CRS = LV03_CRS
# API_DT_FMT = "%Y-%m-%d"
SCALE = "none"
MEASUREMENT = "avg"


class AgrometeoClient(RegionMixin, AllStationsEndpointMixin, BaseClient):
    """Agrometeo client."""

    _stations_endpoint = STATIONS_ENDPOINT

    def __init__(
        self,
        region: RegionType,
        crs: Any = None,
        sjoin_kws: Union[Mapping, None] = None,
    ) -> None:
        """Initialize MetOffice client."""
        # ACHTUNG: CRS must be set before region
        self.CRS = crs or DEFAULT_CRS
        self.X_COL, self.Y_COL = GEOM_COL_DICT[self.CRS]
        self.region = region
        if sjoin_kws is None:
            sjoin_kws = {}
        self.SJOIN_KWS = sjoin_kws

    def _stations_df_from_json(self, response_json: dict) -> pd.DataFrame:
        return pd.DataFrame(response_json["data"])
