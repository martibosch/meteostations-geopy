"""Base abstract classes for meteo station datasets."""

import logging as lg
import re
import time
from abc import ABC
from typing import Mapping, Tuple, Union

import geopandas as gpd
import requests
from better_abc import abstract_attribute

from meteostations import settings, utils

__all__ = ["BaseClient"]


# def _long_ts_df(ts_df, station_id_name, time_name, value_name):
#     """Transform time series data frame from wide (default) to long format."""
#     return pd.melt(
#         ts_df.reset_index(),
#         id_vars=time_name,
#         var_name=station_id_name,
#         value_name=value_name,
#     )


class BaseClient(ABC):
    """Meteo station base client."""

    # def __init__(
    #     self,
    #     *,
    #     crs=None,
    #     stations_id_name=None,
    #     time_name=None,
    #     geocode_to_gdf_kws=None,
    # ):
    #     """
    #     Initialize an meteo station dataset.
    #     """
    #     if stations_id_name is None:
    #         stations_id_name = settings.STATIONS_ID_NAME
    #     self.stations_id_name = stations_id_name
    #     if time_name is None:
    #         time_name = settings.TIME_NAME
    #     self.time_name = time_name

    @abstract_attribute
    def X_COL(self):  # pylint: disable=invalid-name
        """Name of the column with longitude coordinates."""
        pass

    @abstract_attribute
    def Y_COL(self):  # pylint: disable=invalid-name
        """Name of the column with latitude coordinates."""
        pass

    @abstract_attribute
    def CRS(self):  # pylint: disable=invalid-name
        """CRS of the data source."""
        pass

    @property
    def stations_gdf(self) -> gpd.GeoDataFrame:
        """Geo-data frame with stations data."""
        try:
            return self._stations_gdf
        except AttributeError:
            self._stations_gdf = self._get_stations_gdf()
            return self._stations_gdf

    # @abc.abstractmethod
    # def get_ts_df(self, *args, **kwargs):
    #     """
    #     Get time series data frame.

    #     Returns
    #     -------
    #     ts_df : pd.DataFrame
    #         Data frame with a time series of meaurements (rows) at each station
    #         (columns).
    #     """
    #     pass

    # @abc.abstractmethod
    # def get_ts_gdf(self, *args, **kwargs):
    #     """
    #     Get time series geo-data frame.

    #     Returns
    #     -------
    #     ts_gdf : gpd.GeoDataFrame
    #         Geo-data frame with a time series of meaurements (columns) at each station
    #         (rows), with an additional geometry column with the stations' locations.
    #     """
    #     pass

    @property
    def request_headers(self):
        """Request headers."""
        return {}

    @property
    def request_params(self):
        """Request parameters."""
        return {}

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
        """Get JSON response for the url (from the cache or from the API).

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
        cached_response_json = utils._retrieve_from_cache(url)

        if cached_response_json is not None:
            # found response in the cache, return it instead of calling server
            return cached_response_json
        else:
            response_json, sc = self._perform_request(
                url,
                params=params,
                headers=headers,
                request_kws=request_kws,
                pause=pause,
                error_pause=error_pause,
            )
            utils._save_to_cache(url, response_json, sc)
            return response_json

    def _perform_request(
        self,
        url: str,
        *,
        params: Union[Mapping, None] = None,
        headers: Union[Mapping, None] = None,
        request_kws: Union[Mapping, None] = None,
        pause: Union[int, None] = None,
        error_pause: Union[int, None] = None,
    ) -> Tuple[dict, int]:
        """Send GET request to the API and return JSON response and status code.

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
        status_code : int
            Status code of the response.

        """
        # if this URL is not already in the cache, pause, then request it
        if pause is None:
            pause = settings.PAUSE
        utils.log(f"Pausing {pause} seconds before making HTTP GET request")
        time.sleep(pause)

        # transmit the HTTP GET request
        utils.log(f"Get {url} with timeout={settings.TIMEOUT}")
        # headers = _get_http_headers()

        _params = self.request_params.copy()
        _headers = self.request_headers.copy()
        if params is not None:
            _params.update(params)
        if headers is not None:
            _headers.update(headers)
        if request_kws is None:
            request_kws = settings.REQUEST_KWS.copy()
        response = requests.get(
            url,
            params=_params,
            timeout=settings.TIMEOUT,
            headers=_headers,
            **request_kws,
        )
        sc = response.status_code

        # log the response size and domain
        size_kb = len(response.content) / 1000
        domain = re.findall(r"(?s)//(.*?)/", url)[0]
        utils.log(f"Downloaded {size_kb:,.1f}kB from {domain}")

        try:
            response_json = response.json()

        except Exception:  # pragma: no cover
            if sc in {429, 504}:
                # 429 is 'too many requests' and 504 is 'gateway timeout' from
                # server overload: handle these by pausing then recursively
                # re-trying until we get a valid response from the server
                if error_pause is None:
                    error_pause = settings.ERROR_PAUSE
                utils.log(
                    f"{domain} returned {sc}: retry in {error_pause} secs",
                    level=lg.WARNING,
                )
                time.sleep(error_pause)
                response_json = self._perform_request(
                    url,
                    params=params,
                    headers=headers,
                    request_kws=request_kws,
                    pause=pause,
                    error_pause=error_pause,
                )

            else:
                # else, this was an unhandled status code, throw an exception
                utils.log(f"{domain} returned {sc}", level=lg.ERROR)
                raise Exception(
                    "Server returned:\n"
                    f"{response} {response.reason}\n{response.text}"
                )

        return response_json, sc
