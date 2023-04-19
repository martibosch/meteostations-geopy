"""Tests for Meteostations geopy."""
import json
import logging as lg
import tempfile
import unittest
from os import path

import osmnx as ox
import pandas as pd
import requests_mock
from pandas.api.types import is_numeric_dtype

from meteostations import utils
from meteostations.clients import (
    AemetClient,
    AgrometeoClient,
    MeteocatClient,
    MetOfficeClient,
)


def test_utils():
    # dms to dd
    dms_ser = pd.Series(["413120N"])
    dd_ser = utils.dms_to_decimal(dms_ser)
    assert is_numeric_dtype(dd_ser)

    # logger
    utils.log("test a fake default message")
    utils.log("test a fake debug", level=lg.DEBUG)
    utils.log("test a fake info", level=lg.INFO)
    utils.log("test a fake warning", level=lg.WARNING)
    utils.log("test a fake error", level=lg.ERROR)

    # timestamps
    utils.ts(style="date")
    utils.ts(style="time")


def test_region_arg():
    # we will use Agrometeo (since it does not require API keys) to test the region arg
    nominatim_query = "Pully, Switzerland"
    gdf = ox.geocode_to_gdf(nominatim_query)
    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = path.join(tmp_dir, "foo.gpkg")
        gdf.to_file(filepath)
        for region in [nominatim_query, gdf, filepath]:
            client = AgrometeoClient(region=region)
            stations_gdf = client.stations_gdf
            assert len(stations_gdf) >= 1
    # now test naive geometries without providing CRS, so first ensure that we have them
    # in the same CRS as the client
    gdf = gdf.to_crs(client.CRS)
    for region in [gdf.total_bounds, gdf["geometry"].iloc[0]]:
        client = AgrometeoClient(region=region)
        stations_gdf = client.stations_gdf
        assert len(stations_gdf) >= 1


class BaseClientTest:
    client_cls = None
    region = None

    def setUp(self):
        self.client = self.client_cls(region=self.region)

    def test_attributes(self):
        for attr in ["X_COL", "Y_COL", "CRS"]:
            self.assertTrue(hasattr(self.client, attr))
            self.assertIsNotNone(getattr(self.client, attr))

    def test_stations(self):
        stations_gdf = self.client.stations_gdf
        assert len(stations_gdf) >= 1


class APIKeyClientTest(BaseClientTest):
    stations_response_file = None

    def setUp(self):
        self.client = self.client_cls(region=self.region, api_key="fake_key")

    def test_attributes(self):
        super().test_attributes()
        self.assertTrue(hasattr(self.client, "_api_key"))
        self.assertIsNotNone(self.client._api_key)

    def test_stations(self):
        with requests_mock.Mocker() as m:
            with open(self.stations_response_file) as f:
                m.get(self.client._stations_endpoint, json=json.load(f))
            stations_gdf = self.client.stations_gdf
        assert len(stations_gdf) >= 1


class APIKeyHeaderClientTest(APIKeyClientTest):
    def test_attributes(self):
        super().test_attributes()
        self.assertTrue("X-API-KEY" in self.client.request_headers)
        self.assertIsNotNone(self.client.request_headers["X-API-KEY"])


class APIKeyParamClientTest(APIKeyClientTest):
    def test_attributes(self):
        super().test_attributes()
        self.assertTrue(hasattr(self.client, "_api_key_param_name"))
        api_key_param_name = self.client._api_key_param_name
        self.assertTrue(api_key_param_name in self.client.request_params)
        self.assertIsNotNone(self.client.request_params[api_key_param_name])


class AemetClientTest(APIKeyParamClientTest, unittest.TestCase):
    client_cls = AemetClient
    region = "Barcelona"
    api_key = "fake_key"
    stations_interim_file = "tests/data/stations/aemet-interim.json"
    stations_response_file = "tests/data/stations/aemet.json"

    def test_stations(self):
        with requests_mock.Mocker() as m:
            with open(self.stations_interim_file) as f:
                interim_response = json.load(f)
            m.get(self.client._stations_endpoint, json=interim_response)
            # TODO: find a way to mock requests for pandas URL reader (uses urlopen)
            # interim_url = interim_response["datos"]
            # with open(self.stations_response_file) as f:
            #     stations_response = json.load(f)
            # m.get(interim_url, json=stations_response)
            # stations_gdf = self.client.stations_gdf
            # self.assertTrue(len(stations_gdf) >= 1)


class AgrometeoClientTest(BaseClientTest, unittest.TestCase):
    client_cls = AgrometeoClient
    region = "Pully, Switzerland"


class MeteocatClientTest(APIKeyHeaderClientTest, unittest.TestCase):
    client_cls = MeteocatClient
    region = "Barcelona"
    api_key = "fake_key"
    stations_response_file = "tests/data/stations/meteocat.json"


class MetOfficeClientTest(APIKeyParamClientTest, unittest.TestCase):
    client_cls = MetOfficeClient
    region = "Edinburgh"
    api_key = "fake_key"
    stations_response_file = "tests/data/stations/metoffice.json"
