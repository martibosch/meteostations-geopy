"""MetOffice client."""

from typing import Mapping, Union

import pandas as pd

from meteostations import settings, utils
from meteostations.base import BaseClient
from meteostations.mixins.auth import APIKeyParamMixin
from meteostations.mixins.region import RegionMixin, RegionType
from meteostations.mixins.stations import AllStationsEndpointMixin
from meteostations.mixins.variables import VariablesEndpointMixin

# API endpoints
BASE_URL = "https://opendata.aemet.es/opendata/api"
STATIONS_ENDPOINT = (
    f"{BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
)
VARIABLES_ENDPOINT = DATA_ENDPOINT = f"{BASE_URL}/observacion/convencional/todas"

# useful constants
STATIONS_ID_COL = "idema"
VARIABLES_NAME_COL = VARIABLES_CODE_COL = "id"
ECV_DICT = {
    "precipitation": "prec",
    "pressure": "pres",
    "surface_wind_speed": "vv",
    "surface_wind_direction": "dv",
    "temperature": "ta",
    "water_vapour": "hr",
}


class AemetClient(
    APIKeyParamMixin,
    RegionMixin,
    AllStationsEndpointMixin,
    VariablesEndpointMixin,
    BaseClient,
):
    """MetOffice client."""

    X_COL = "longitud"
    Y_COL = "latitud"
    CRS = "epsg:4326"
    _stations_endpoint = STATIONS_ENDPOINT
    _stations_id_col = STATIONS_ID_COL
    _variables_endpoint = VARIABLES_ENDPOINT
    _variables_name_col = VARIABLES_NAME_COL
    _variables_code_col = VARIABLES_CODE_COL
    _ecv_dict = ECV_DICT
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

    def _variables_df_from_json(self, response_json) -> pd.DataFrame:
        return pd.json_normalize(
            pd.read_json(response_json["metadatos"], encoding="latin1")["campos"]
        )

    def get_ts_df(
        self,
        variable: Union[str, int],
    ) -> pd.DataFrame:
        """Get time series data frame for the last 24h.

        Parameters
        ----------
        variable : str or int
            Target variable, which can be either an Aemet variable code (integer or
            string) or an essential climate variable (ECV) following the
            meteostations-geopy nomenclature (string).

        Returns
        -------
        ts_df : pd.DataFrame
            Data frame with a time series of meaurements (rows) at each station
            (columns).

        """
        response_json = self._get_json_from_url(DATA_ENDPOINT)
        # response_json returns a dict with urls, where the one under the "datos" key
        # contains the JSON data
        long_df = pd.read_json(response_json["datos"], encoding="latin1")
        # filter only stations from the region
        # TODO: how to handle better the "indicativo" column name? i.e., the stations id
        # column is "idema" in the observation data frame but "indicativo" in the
        # stations data frame.
        long_df = long_df[
            long_df[self._stations_id_col].isin(self.stations_gdf["indicativo"])
        ]
        # TODO: time_col as class-level constant?
        time_col = "fint"
        # process the variable arg
        # TODO: in this case, there is no variable name, only variable code
        variable_code = self._process_variable_arg(variable)
        # convert to wide_df
        # TODO: allow returning long_df?
        ts_df = long_df.pivot_table(
            index=time_col, columns=self._stations_id_col, values=variable_code
        )
        # set the index name
        ts_df.index.name = settings.TIME_NAME
        # return the sorted data frame
        return ts_df.sort_index()
