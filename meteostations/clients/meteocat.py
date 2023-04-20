"""MetOffice client."""
from typing import Mapping, Union

import pandas as pd

from meteostations.base import BaseClient
from meteostations.mixins.auth import APIKeyHeaderMixin
from meteostations.mixins.region import RegionMixin, RegionType
from meteostations.mixins.stations import AllStationsEndpointMixin
from meteostations.mixins.variables import VariablesEndpointMixin

# API endpoints
BASE_URL = "https://api.meteo.cat/xema/v1"
STATIONS_ENDPOINT = f"{BASE_URL}/estacions/metadades"
VARIABLES_ENDPOINT = f"{BASE_URL}/variables/mesurades/metadades"

# useful constants
VARIABLES_NAME_COL = "name"


class MeteocatClient(
    APIKeyHeaderMixin,
    RegionMixin,
    AllStationsEndpointMixin,
    VariablesEndpointMixin,
    BaseClient,
):
    """MetOffice client."""

    X_COL = "coordenades.longitud"
    Y_COL = "coordenades.latitud"
    CRS = "epsg:4326"
    _variables_name_col = VARIABLES_NAME_COL
    _stations_endpoint = STATIONS_ENDPOINT
    _variables_endpoint = VARIABLES_ENDPOINT

    def __init__(
        self, region: RegionType, api_key: str, sjoin_kws: Union[Mapping, None] = None
    ) -> None:
        """Initialize Meteocat client."""
        self.region = region
        self._api_key = api_key
        if sjoin_kws is None:
            sjoin_kws = {}
        self.SJOIN_KWS = sjoin_kws

    def _stations_df_from_json(self, response_json: dict) -> pd.DataFrame:
        return pd.json_normalize(response_json)

    def _variables_df_from_json(self, response_json: dict) -> pd.DataFrame:
        return pd.json_normalize(response_json)
