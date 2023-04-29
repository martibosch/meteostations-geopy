"""MetOffice client."""
from typing import Mapping, Union

import pandas as pd

from meteostations.clients.base import BaseClient, RegionType
from meteostations.mixins import AllStationsEndpointMixin, APIKeyParamMixin

# API endpoints
BASE_URL = "http://datapoint.metoffice.gov.uk/public/data"
STATIONS_ENDPOINT = f"{BASE_URL}/val/wxobs/all/json/sitelist"


class MetOfficeClient(APIKeyParamMixin, AllStationsEndpointMixin, BaseClient):
    """MetOffice client."""

    X_COL = "longitude"
    Y_COL = "latitude"
    CRS = "epsg:4326"
    _stations_endpoint = STATIONS_ENDPOINT
    _api_key_param_name = "key"

    def __init__(
        self, region: RegionType, api_key: str, sjoin_kws: Union[Mapping, None] = None
    ) -> None:
        """Initialize MetOffice client."""
        self.region = region
        self._api_key = api_key
        if sjoin_kws is None:
            sjoin_kws = {}
        self.SJOIN_KWS = sjoin_kws

    def _stations_df_from_json(self, response_json: dict) -> pd.DataFrame:
        return pd.DataFrame(response_json["Locations"]["Location"])
