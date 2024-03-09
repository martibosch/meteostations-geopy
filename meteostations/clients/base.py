"""Base abstract classes for meteo station datasets."""

import datetime
import logging as lg
import os
import re
import time
from abc import ABC
from typing import IO, Mapping, Sequence, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
import requests
from better_abc import abstract_attribute
from fiona.errors import DriverError
from shapely import geometry
from shapely.geometry.base import BaseGeometry

from meteostations import settings, utils

try:
    import osmnx as ox
except ImportError:
    ox = None


__all__ = ["BaseClient", "RegionType", "DateTimeType"]


# def _long_ts_df(ts_df, station_id_name, time_name, value_name):
#     """Transform time series data frame from wide (default) to long format."""
#     return pd.melt(
#         ts_df.reset_index(),
#         id_vars=time_name,
#         var_name=station_id_name,
#         value_name=value_name,
#     )

RegionType = Union[str, Sequence, gpd.GeoSeries, gpd.GeoDataFrame, os.PathLike, IO]
DateTimeType = Union[
    datetime.date, datetime.datetime, np.datetime64, pd.Timestamp, str, int, float
]


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
    def CRS(self) -> pyproj.CRS:  # pylint: disable=invalid-name
        """CRS of the data source."""
        pass

    @property
    def region(self) -> Union[gpd.GeoDataFrame, None]:
        """The region as a GeoDataFrame."""
        return self._region

    @region.setter
    def region(
        self,
        region: Union[str, Sequence, gpd.GeoSeries, gpd.GeoDataFrame, os.PathLike, IO],
    ):
        self._region = self._process_region_arg(region)

    def _process_region_arg(
        self,
        region: Union[str, Sequence, gpd.GeoSeries, gpd.GeoDataFrame, os.PathLike, IO],
        *,
        geocode_to_gdf_kws: Union[dict, None] = None,
    ) -> Union[gpd.GeoDataFrame, None]:
        """Process the region argument.

        Parameters
        ----------
        region : str, Sequence, GeoSeries, GeoDataFrame, PathLike, or IO
            The region to process. This can be either:
            -  A string with a place name (Nominatim query) to geocode.
            -  A sequence with the west, south, east and north bounds.
            -  A geometric object, e.g., shapely geometry, or a sequence of geometric
               objects. In such a case, the value will be passed as the `data` argument
               of the GeoSeries constructor, and needs to be in the same CRS as the one
               used by the client's class (i.e., the `CRS` class attribute).
            -  A geopandas geo-series or geo-data frame.
            -  A filename or URL, a file-like object opened in binary ('rb') mode, or a
               Path object that will be passed to `geopandas.read_file`.
        geocode_to_gdf_kws : dict or None, optional
            Keyword arguments to pass to `geocode_to_gdf` if `region` is a string
            corresponding to a place name (Nominatim query).

        Returns
        -------
        gdf : GeoDataFrame
            The processed region as a GeoDataFrame, in the CRS used by the client's
            class. A value of None is returned when passing a place name (Nominatim
            query) but osmnx is not installed.

        """
        # crs : Any, optional
        # Coordinate Reference System of the provided `region`. Ignored if `region` is a
        # string corresponding to a place name, a geopandas geo-series or geo-data frame
        # with its CRS attribute set or a filename, URL or file-like object. Can be
        # anything accepted by `pyproj.CRS.from_user_input()`, such as an authority
        # string (eg “EPSG:4326”) or a WKT string.

        if not isinstance(region, gpd.GeoDataFrame):
            # naive geometries
            if not isinstance(region, gpd.GeoSeries) and (
                hasattr(region, "__iter__")
                and not isinstance(region, str)
                or isinstance(region, BaseGeometry)
            ):
                # if region is a sequence (other than a string)
                # use the hasattr to avoid AttributeError when region is a BaseGeometry
                if hasattr(region, "__len__"):
                    if len(region) == 4 and isinstance(region[0], (int, float)):
                        # if region is a sequence of 4 numbers, assume it's a bounding
                        # box
                        region = geometry.box(*region)
                # otherwise, assume it's a geometry or sequence of geometries that can
                # be passed as the `data` argument of the GeoSeries constructor
                region = gpd.GeoSeries(region, crs=self.CRS)
            if isinstance(region, gpd.GeoSeries):
                # if we have a GeoSeries, convert it to a GeoDataFrame so that we can
                # use the same code
                region = gpd.GeoDataFrame(
                    geometry=region, crs=getattr(region, "crs", self.CRS)
                )
            else:
                # at this point, we assume that this is either file-like or a Nominatim
                # query
                try:
                    region = gpd.read_file(region)
                except (DriverError, AttributeError):
                    #             if ox is None:
                    #                 lg.warning(
                    #                     """
                    # Using a Nominatim query as `region` argument requires osmnx.
                    # You can install it using conda or pip.
                    # """
                    #                 )
                    #                 return

                    if geocode_to_gdf_kws is None:
                        geocode_to_gdf_kws = {}
                    region = ox.geocode_to_gdf(region, **geocode_to_gdf_kws).iloc[:1]

        return region.to_crs(self.CRS)

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
        # use the Python requests library to prepare the url here so that the cache
        # mechanism takes into account the params
        # TODO: DRY together with `self._perform_request`
        # - the `params` arg is processed here and in `self._perform_request`
        # - the url is prepared here and in `self._perform_request`
        _params = self.request_params.copy()
        if params is not None:
            _params.update(params)
        prepared_url = requests.Request("GET", url, params=_params).prepare().url
        cached_response_json = utils._retrieve_from_cache(prepared_url)

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
            utils._save_to_cache(prepared_url, response_json, sc)
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
