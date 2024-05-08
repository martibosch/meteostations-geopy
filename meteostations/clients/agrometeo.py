"""Agrometeo client."""

import datetime
from typing import Any, List, Mapping, Union

import geopandas as gpd
import pandas as pd
import pyproj

from meteostations import settings
from meteostations.clients.base import BaseClient, RegionType
from meteostations.mixins import AllStationsEndpointMixin, VariablesEndpointMixin

# API endpoints
BASE_URL = "https://agrometeo.ch/backend/api"
STATIONS_ENDPOINT = f"{BASE_URL}/stations"
VARIABLES_ENDPOINT = f"{BASE_URL}/sensors"
DATA_ENDPOINT = f"{BASE_URL}/meteo/data"

# useful constants
LONLAT_CRS = pyproj.CRS("epsg:4326")
LV03_CRS = pyproj.CRS("epsg:21781")
# ACHTUNG: for some reason, the API mixes up the longitude and latitude columns ONLY in
# the CH1903/LV03 projection. This is why we need to swap the columns in the dict below.
GEOM_COL_DICT = {LONLAT_CRS: ["long_dec", "lat_dec"], LV03_CRS: ["lat_ch", "long_ch"]}
DEFAULT_CRS = LV03_CRS
# stations column used by the Agrometeo API (do not change)
STATIONS_API_ID_COL = "id"
# stations column used to index the data (e.g., time-series dataframe) by the client's
# class (can be any column that is unique to each station, e.g., name or id).
# The docstring would read as:
# stations_id_col : str, optional
#     Column of `stations_gdf` that will be used in the returned data frame to identify
#     the stations. If None, the value from `STATIONS_ID_COL` will be used.
STATIONS_ID_COL = "name"
# variables name column
VARIABLES_NAME_COL = "name.en"
# variables code column
VARIABLES_CODE_COL = "id"
# agrometeo sensors
# 42                       Leaf moisture III
# 43     Voltage of internal lithium battery
# 1              Temperature 2m above ground
# 4                       Relative humidity
# 6                           Precipitation
# 15              Intensity of precipitation
# 7                            Leaf moisture
# 11                         Solar radiation
# 41                           Solar Energie
# 9                          Avg. wind speed
# 14                         Max. wind speed
# 8                           Wind direction
# 22                       Temperature +10cm
# 12                    Luxmeter after Lufft
# 10                                ETP-Turc
# 24                              ETo-PenMon
# 13                               Dew point
# 18                       Real air pressure
# 2                    Soil temperature +5cm
# 19                  Soil temperature -20cm
# 3                   Soil temperature -10cm
# 5                       Soil moisture -5cm
# 20                   Pressure on sea level
# 17                        Leaf moisture II
# 25                     Soil moisture -30cm
# 26                     Soil moisture -50cm
# 39                                  unused
# 33                 Temperature in leafzone
# 32                         battery voltage
# 21                         min. wind speed
# 23                        Temperatur +20cm
# 27                  Temperatur in Pflanze1
# 28                  Temperatur in Pflanze1
# 29                                    UVAB
# 30                                     UVA
# 31                                     UAB
# 34                Air humidity in leafzone
# 35             Photosyth. active radiation
# 36                  Soil temperature -10cm
# 37                Temperatur 2m unbelüftet
# 38           elative Luftfeuchtigkeit +5cm
# 40                     Precip. Radolan Day
# 100                                   Hour
# 101                                   Year
# 102                            Day of year
# 103                           Degree hours
# 104                 Density of sporulation
# 105                           Leaf surface
ECV_DICT = {
    "precipitation": "Precipitation",
    "pressure": "Real air pressure",
    "surface_radiation_shortwave": "Solar radiation",
    "surface_wind_speed": "Avg. wind speed",
    "surface_wind_direction": "Wind direction",
    "temperature": "Temperature 2m above ground",
    "water_vapour": "Relative humidity",
}
TIME_COL = "date"
API_DT_FMT = "%Y-%m-%d"
SCALE = "none"
MEASUREMENT = "avg"


class AgrometeoClient(AllStationsEndpointMixin, VariablesEndpointMixin, BaseClient):
    """Agrometeo client."""

    _stations_endpoint = STATIONS_ENDPOINT
    _stations_id_col = STATIONS_ID_COL
    _variables_endpoint = VARIABLES_ENDPOINT
    _variables_code_col = VARIABLES_CODE_COL
    _variables_name_col = VARIABLES_NAME_COL
    _data_endpoint = DATA_ENDPOINT
    _ecv_dict = ECV_DICT
    _time_col = TIME_COL

    def __init__(
        self,
        region: RegionType,
        crs: Any = None,
        variables_name_col: Union[str, None] = None,
        sjoin_kws: Union[Mapping, None] = None,
    ) -> None:
        """Initialize Agrometeo client."""
        # ACHTUNG: CRS must be either EPSG:4326 or EPSG:21781
        # ACHTUNG: CRS must be set before region
        if crs is not None:
            crs = pyproj.CRS(crs)
        else:
            crs = DEFAULT_CRS
        self.CRS = crs
        self._variables_name_col = variables_name_col or VARIABLES_NAME_COL
        try:
            self.X_COL, self.Y_COL = GEOM_COL_DICT[self.CRS]
        except KeyError:
            raise ValueError(
                f"CRS must be among {list(GEOM_COL_DICT.keys())}, got {self.CRS}"
            )

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

    def get_ts_df(
        self,
        variables: Union[str, int, List[str], List[int]],
        start_date: Union[str, datetime.date],
        end_date: Union[str, datetime.date],
        *,
        scale: Union[str, None] = None,
        measurement: Union[str, None] = None,
    ) -> pd.DataFrame:
        """Get time series data frame.

        Parameters
        ----------
        variables : str, int or list-like of str or int
            Target variables, which can be either an agrometeo variable code (integer or
            string), an essential climate variable (ECV) following the
            meteostations-geopy nomenclature (string), or an agrometeo variable name
            (string).
        start_date, end_date : str or datetime.date
            String in the "YYYY-MM-DD" format or datetime.date instance, respectively
            representing the start and end days of the requested data period.
        scale : None or {"hour", "day", "month", "year"}, default None
            Temporal scale of the measurements. The default value of None returns the
            finest scale, i.e., 10 minutes.
        measurement : None or {"min", "avg", "max"}, default None
            Whether the measurement values correspond to the minimum, average or maximum
            value for the required temporal scale. Ignored if `scale` is None.

        Returns
        -------
        ts_df : pd.DataFrame
            Data frame with a time series of meaurements (rows) at each station
            (columns).

        """
        # process variables
        if not pd.api.types.is_list_like(variables):
            variables = [variables]
        variable_codes = [
            self._process_variable_arg(variable) for variable in variables
        ]

        # process date args
        if isinstance(start_date, datetime.date):
            start_date = start_date.strftime(API_DT_FMT)
        if isinstance(end_date, datetime.date):
            end_date = end_date.strftime(API_DT_FMT)
        # process scale and measurement args
        if scale is None:
            # the API needs it to be lowercase
            scale = SCALE
        if measurement is None:
            measurement = MEASUREMENT
        # # process the stations_id_col arg
        # if stations_id_col is None:
        #     stations_id_col = self._stations_id_col

        _stations_ids = self.stations_gdf[STATIONS_API_ID_COL].astype(str)
        request_url = f"{self._data_endpoint}?" + "&".join(
            [
                f"from={start_date}",
                f"to={end_date}",
                f"scale={scale}",
                "sensors="
                + "%2C".join(
                    [
                        f"{variable_code}%3A{measurement}"
                        for variable_code in variable_codes
                    ]
                ),
                f"stations={'%2C'.join(_stations_ids)}",
            ]
        )

        response_json = self._get_json_from_url(request_url)

        # parse the response as a data frame
        ts_df = pd.json_normalize(response_json["data"]).set_index(self._time_col)
        ts_df.index = pd.to_datetime(ts_df.index)
        ts_df.index.name = settings.TIME_NAME
        # ts_df.columns = self.stations_gdf[STATIONS_ID_COL]
        # ACHTUNG: note that agrometeo returns the data indexed by keys of the form
        # "{station_id}_{variable_code}_{measurement}", so to properly set the columns
        # as the desired station identifier (e.g., "id" or "name") we need to first get
        # the ids and then get (loc) the station data from the stations_gdf.
        ts_df.columns = self.stations_gdf.set_index(STATIONS_API_ID_COL).loc[
            ts_df.columns.str.replace(
                [f"_{variable_code}_{measurement}" for variable_code in variable_codes],
                "",
            ).astype(self.stations_gdf[STATIONS_API_ID_COL].dtype)
        ][self._stations_id_col]
        ts_df = ts_df.apply(pd.to_numeric, axis=1)

        return ts_df.sort_index()

    def get_ts_gdf(
        self,
        variable: Union[str, int],
        start_date: Union[str, datetime.date],
        end_date: Union[str, datetime.date],
        *,
        scale: Union[str, None] = None,
        measurement: Union[str, None] = None,
    ):
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
        scale : None or {"hour", "day", "month", "year"}, default None
            Temporal scale of the measurements. The default value of None returns the
            finest scale, i.e., 10 minutes.
        measurement : None or {"min", "avg", "max"}, default None
            Whether the measurement values correspond to the minimum, average or maximum
            value for the required temporal scale. Ignored if `scale` is None.

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
                scale=scale,
                measurement=measurement,
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
