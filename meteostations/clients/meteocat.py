"""Meteocat client."""
import datetime
from typing import Mapping, Union

import geopandas as gpd
import pandas as pd

from meteostations import settings
from meteostations.clients.base import BaseClient, RegionType
from meteostations.mixins import (
    AllStationsEndpointMixin,
    APIKeyHeaderMixin,
    VariablesEndpointMixin,
)

# API endpoints
BASE_URL = "https://api.meteo.cat/xema/v1"
STATIONS_ENDPOINT = f"{BASE_URL}/estacions/metadades"
VARIABLES_ENDPOINT = f"{BASE_URL}/variables/mesurades/metadades"
DATA_ENDPOINT = f"{BASE_URL}/variables/mesurades"

# useful constants
STATIONS_ID_COL = "codi"
VARIABLES_NAME_COL = "nom"
VARIABLES_CODE_COL = "codi"
ECV_DICT = {
    "precipitation": "Precipitació",
    "pressure": "Pressió atmosfèrica",
    "surface_radiation_shortwave": "Radiació UV",
    "surface_wind_speed": "Velocitat del vent a 10 m (esc.)",
    "surface_wind_direction": "Direcció de vent 10 m (m. 1)",
    "temperature": "Temperatura",
    "water_vapour": "Humitat relativa",
}
TIME_COL = "data"


class MeteocatClient(
    APIKeyHeaderMixin,
    AllStationsEndpointMixin,
    VariablesEndpointMixin,
    BaseClient,
):
    """Meteocat client."""

    X_COL = "coordenades.longitud"
    Y_COL = "coordenades.latitud"
    CRS = "epsg:4326"
    _stations_endpoint = STATIONS_ENDPOINT
    _stations_id_col = STATIONS_ID_COL
    _variables_endpoint = VARIABLES_ENDPOINT
    _variables_name_col = VARIABLES_NAME_COL
    _variables_code_col = VARIABLES_CODE_COL
    _ecv_dict = ECV_DICT
    _data_endpoint = DATA_ENDPOINT
    _time_col = TIME_COL

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

    def _get_ts_df(
        self,
        variable: Union[str, int],
        date: Union[str, datetime.date],
    ) -> pd.DataFrame:
        """Get time series data frame for a given day.

        Parameters
        ----------
        variable : str or int
            Target variable, which can be either a Meteocat variable code (integer or
            string), an essential climate variable (ECV) following the
            meteostations-geopy nomenclature (string), or a Meteocat variable name
            (string).
        date : str or datetime.date
            String in the "YYYY-MM-DD" format or datetime.date instance,
            representing the start and end days of the requested data period.

        Returns
        -------
        ts_df : pd.DataFrame
            Data frame with a time series of meaurements (rows) at each station
            (columns).
        """
        variable_code = self._process_variable_arg(variable)
        # process date arg
        if isinstance(date, str):
            date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        # request url
        request_url = (
            f"{self._data_endpoint}"
            f"/{variable_code}/{date.year}/{date.month:02}/{date.day:02}"
        )
        response_json = self._get_json_from_url(request_url)
        # process response
        response_df = pd.json_normalize(response_json)
        # filter stations
        response_df = response_df[
            response_df["codi"].isin(self.stations_gdf[self._stations_id_col])
        ]
        # extract json observed data, i.e.,  the "variables" column into a list of data
        # frames and concatenate them into a single data frame
        long_df = pd.concat(
            response_df.apply(
                lambda row: pd.DataFrame(row["variables"][0]["lectures"]), axis=1
            ).tolist()
        )
        # add the station id column matching the observations
        long_df[self._stations_id_col] = (
            response_df[self._stations_id_col]
            .repeat(
                response_df.apply(
                    lambda row: len(row["variables"][0]["lectures"]), axis=1
                )
            )
            .values
        )
        # TODO: values_col as class-level constant?
        values_col = "valor"
        # convert to a wide data frame
        ts_df = long_df.pivot_table(
            index=self._time_col, columns=self._stations_id_col, values=values_col
        )
        # set the index name
        ts_df.index.name = settings.TIME_NAME
        # convert the index from string to datetime
        ts_df.index = pd.to_datetime(ts_df.index)
        # return the sorted data frame
        return ts_df.sort_index()

    def get_ts_df(
        self,
        variable: Union[str, int],
        start_date: Union[str, datetime.date],
        end_date: Union[str, datetime.date],
    ) -> pd.DataFrame:
        """Get time series data frame.

        Parameters
        ----------
        variable : str or int
            Target variable, which can be either a Meteocat variable code (integer or
            string), an essential climate variable (ECV) following the
            meteostations-geopy nomenclature (string), or a Meteocat variable name
            (string).
        start_date, end_date : str or datetime.date
            String in the "YYYY-MM-DD" format or datetime.date instance, respectively
            representing the start and end days of the requested data period.

        Returns
        -------
        ts_df : pd.DataFrame
            Data frame with a time series of meaurements (rows) at each station
            (columns).
        """
        # return self._get_ts_df(variable, date)
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        return pd.concat(self._get_ts_df(variable, date) for date in date_range)

    def get_ts_gdf(
        self,
        variable: Union[str, int],
        start_date: Union[str, datetime.date],
        end_date: Union[str, datetime.date],
    ) -> gpd.GeoDataFrame:
        """Get time series geo-data frame.

        Parameters
        ----------
        variable : str or int
            Target variable, which can be either an agrometeo variable code (integer or
            string), an essential climate variable (ECV) following the
            meteostations-geopy nomenclature (string), or an agrometeo variable name
            (string).
        start_date, end_date : str or datetime
            String in the "YYYY-MM-DD" format or datetime instance, respectively
            representing the start and end of the  requested data period.

        Returns
        -------
        ts_gdf : gpd.GeoDataFrame
            Geo-data frame with a time series of meaurements (columns) at each station
            (rows), with an additional geometry column with the stations' locations.
        """
        ts_gdf = gpd.GeoDataFrame(
            self.get_ts_df(
                variable,
                start_date,
                end_date,
            ).T
        )
        # get the geometry from stations_gdf
        ts_gdf["geometry"] = self.stations_gdf.set_index(ts_gdf.index.name).loc[
            ts_gdf.index
        ]["geometry"]
        # sort the timestamp columns
        ts_columns = ts_gdf.columns.drop("geometry")
        ts_gdf = ts_gdf[sorted(ts_columns) + ["geometry"]]

        return ts_gdf
