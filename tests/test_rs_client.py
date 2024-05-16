# Copyright 2024 CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for RsClient, AuxipClient, CadipClient."""

import urllib.parse
from datetime import datetime

import pytest
import responses

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_client.rs_client import RsClient
from rs_client.stac_client import StacClient
from rs_common.config import DATETIME_FORMAT, ECadipStation, EPlatform

# Use dummy values
RS_SERVER_HREF = "http://rs_server_href"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = "OWNER_ID"
CADIP_STATION = ECadipStation.CADIP
PLATFORMS = [EPlatform.S1A, EPlatform.S2A]
TIMEOUT = 3  # seconds

RS_CLIENT = RsClient(RS_SERVER_HREF, RS_SERVER_API_KEY, OWNER_ID)
AUXIP_CLIENT = RS_CLIENT.get_auxip_client()
CADIP_CLIENT = RS_CLIENT.get_cadip_client(CADIP_STATION)


def test_get_child_client():
    """Test get_auxip_client, get_cadip_client, get_stac_client"""
    assert isinstance(RS_CLIENT.get_auxip_client(), AuxipClient)
    assert isinstance(RS_CLIENT.get_cadip_client(CADIP_STATION), CadipClient)
    assert isinstance(RS_CLIENT.get_stac_client(), StacClient)


def test_station_names():
    """Test the station name returned by the AuxipClient and CadipClient"""
    assert "ADGS" in AUXIP_CLIENT.station_name
    assert "CADIP" in CADIP_CLIENT.station_name


def test_server_href(monkeypatch):
    """Test that the Auxip, Cadip, Catalog service URLs can be passed by environment variable."""

    rs_client = RsClient("", RS_SERVER_API_KEY, OWNER_ID)  # no global href

    for env_var, client, get_href in [
        ["RSPY_HOST_ADGS", rs_client.get_auxip_client(), "href_adgs"],
        ["RSPY_HOST_CADIP", rs_client.get_cadip_client(CADIP_STATION), "href_cadip"],
        ["RSPY_HOST_CATALOG", rs_client.get_stac_client(), "href_catalog"],
    ]:
        # Without the env var, we should have an error
        with pytest.raises(RuntimeError):
            getattr(client, get_href)

        # If we set the global href, it should be returned
        client.rs_server_href = RS_SERVER_HREF
        assert getattr(client, get_href) == RS_SERVER_HREF

        # It can be overriden by the env var
        dummy_href = "DUMMY_HREF"
        monkeypatch.setenv(env_var, dummy_href)
        assert getattr(client, get_href) == dummy_href
        monkeypatch.delenv(env_var)


def test_cadip_sessions():
    """
    Test CadipClient.search_sessions
    """

    # Note: do some basic tests only. More extensive tests are done on the rs-server side.

    # Dummy values
    session_ids = ["id1", "id2"]
    start_date = datetime(2000, 1, 1)
    stop_date = datetime(2001, 1, 1)

    # Test the connection error with the dummy server
    with pytest.raises(RuntimeError) as error:
        CADIP_CLIENT.search_sessions(TIMEOUT, session_ids, start_date, stop_date, PLATFORMS)
    assert "ConnectionError" in str(error.getrepr())

    # Mock the response, now the call should work
    params = {
        "id": "id1,id2",
        "platform": "S1A,S2A",
        "start_date": start_date.strftime(DATETIME_FORMAT),
        "stop_date": stop_date.strftime(DATETIME_FORMAT),
    }
    mock_url = f"{RS_SERVER_HREF}/cadip/CADIP/session?{urllib.parse.urlencode(params)}"
    features = ["feature1", "feature2"]
    content = {"features": features}

    # Test a bad response content format
    with pytest.raises(RuntimeError) as error:
        with responses.RequestsMock() as resp:
            resp.get(url=mock_url, json={}, status=200)
            CADIP_CLIENT.search_sessions(TIMEOUT, session_ids, start_date, stop_date, PLATFORMS)
    assert "KeyError" in str(error.getrepr())

    # Test a bad response status code
    with responses.RequestsMock() as resp:
        resp.get(url=mock_url, json=content, status=500)
        sessions = CADIP_CLIENT.search_sessions(TIMEOUT, session_ids, start_date, stop_date, PLATFORMS)
        assert not sessions

    # Test the nominal case
    with responses.RequestsMock() as resp:
        resp.get(url=mock_url, json=content, status=200)
        sessions = CADIP_CLIENT.search_sessions(TIMEOUT, session_ids, start_date, stop_date, PLATFORMS)
        assert sessions == features
