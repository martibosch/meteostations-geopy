"""MetOffice client."""
from typing import Mapping, Union

import pandas as pd

from meteostations import utils
from meteostations.base import BaseClient
from meteostations.mixins.auth import APIKeyParamMixin
from meteostations.mixins.region import RegionMixin, RegionType
from meteostations.mixins.stations import AllStationsEndpointMixin

# API endpoints
BASE_URL = "https://opendata.aemet.es/opendata/api"
STATIONS_ENDPOINT = (
    f"{BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
)


class AemetClient(APIKeyParamMixin, RegionMixin, AllStationsEndpointMixin, BaseClient):
    """MetOffice client."""

    X_COL = "longitud"
    Y_COL = "latitud"
    CRS = "epsg:4326"
    _stations_endpoint = STATIONS_ENDPOINT
    _api_key_param_name = "api_key"
    request_headers = {"cache-control": "no-cache"}

    # @property
    # def request_headers(self):
    #     """Request headers."""
    #     return {"cache-control": "no-cache"}

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
        # response_json returns a dict with urls, where the one under the "datos" key
        # contains the JSON data
        stations_df = pd.read_json(response_json["datos"], encoding="latin1")
        for col in [self.X_COL, self.Y_COL]:
            stations_df[col] = utils.dms_to_decimal(stations_df[col])
        return stations_df

    def _get_json_from_url(
        self,
        url: str,
        *,
        params: Union[Mapping, None] = None,
        headers: Union[Mapping, None] = None,
        request_kws: Union[Mapping, None] = None,
        pause: Union[int, None] = None,
        error_pause: Union[int, None] = None,
    ) -> dict:
        """Get JSON response for the url.

        Parameters
        ----------
        url : str
            URL to request.
        params : dict, optional
            Parameters to pass to the request. They will be added to the default params
            set in the `request_params` property.
        headers : dict, optional
            Headers to pass to the request. They will be added to the default headers
            set in the `request_headers` property.
        request_kws : dict, optional
            Additional keyword arguments to pass to `requests.get`. If None, the value
            from `settings.REQUEST_KWS` will be used.
        pause : int, optional
            How long to pause before request, in seconds. If None, the value from
            `settings.PAUSE` will be used.
        error_pause : int, optional
            How long to pause in seconds before re-trying request if error. If None, the
            value from `settings.ERROR_PAUSE` will be used.

        Returns
        -------
        response_json : dict
            JSON-encoded response content.
        """
        response_json, _ = self._perform_request(
            url,
            params=params,
            headers=headers,
            request_kws=request_kws,
            pause=pause,
            error_pause=error_pause,
        )
        return response_json
