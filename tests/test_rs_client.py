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

import pystac_client
import pytest
import responses

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_client.rs_client import RsClient
from rs_client.stac_client import StacClient
from rs_common.config import DATETIME_FORMAT, ECadipStation, EPlatform

# Use dummy values
RSPY_UAC_CHECK_URL = "http://www.rspy-uac-manager.com"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = "OWNER_ID"
CADIP_STATION = ECadipStation.CADIP
PLATFORMS = [EPlatform.S1A, EPlatform.S2A]


@pytest.fixture(name="generic_rs_client")
def generic_rs_client_(mocked_stac_catalog_url):
    """Return a generic RsClient instance for testing."""
    yield RsClient(mocked_stac_catalog_url, RS_SERVER_API_KEY, OWNER_ID)  # will be used to test the StacClient


@pytest.fixture(name="auxip_client")
def auxip_client_(generic_rs_client):
    """Return a generic AuxipClient instance for testing."""
    yield generic_rs_client.get_auxip_client()


@pytest.fixture(name="cadip_client")
def cadip_client_(generic_rs_client):
    """Return a generic CadipClient instance for testing."""
    yield generic_rs_client.get_cadip_client(CADIP_STATION)


@pytest.fixture(name="stac_client")
def stac_client_(generic_rs_client):
    """Return a generic StacClient instance for testing."""
    yield generic_rs_client.get_stac_client()


def test_get_child_client(auxip_client, cadip_client, stac_client):  # pylint: disable=redefined-outer-name
    """Test get_auxip_client, get_cadip_client, get_stac_client"""
    assert isinstance(auxip_client, AuxipClient)
    assert isinstance(cadip_client, CadipClient)
    assert isinstance(stac_client, StacClient)


def test_station_names(auxip_client, cadip_client, stac_client):  # pylint: disable=redefined-outer-name
    """Test the station name returned by the AuxipClient and CadipClient"""
    assert "AUXIP" in auxip_client.station_name
    assert "CADIP" in cadip_client.station_name
    assert isinstance(stac_client, StacClient)


def test_server_href(mocked_stac_catalog_url):
    """Test that the Auxip, Cadip, Catalog service URLs can be passed by environment variable."""

    rs_client = RsClient("", RS_SERVER_API_KEY, OWNER_ID)  # no global href
    dummy_href = "http://DUMMY_HREF"

    for env_var, client, get_href in [
        ["RSPY_HOST_ADGS", rs_client.get_auxip_client(), "href_adgs"],
        ["RSPY_HOST_CADIP", rs_client.get_cadip_client(CADIP_STATION), "href_cadip"],
    ]:
        # Without the env var, we should have an error
        with pytest.raises(RuntimeError):
            getattr(client, get_href)

        # If we set the global URL, it should be returned
        client.rs_server_href = mocked_stac_catalog_url
        assert getattr(client, get_href) == mocked_stac_catalog_url

        # It can be overriden by the env var
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv(env_var, dummy_href)
            assert getattr(client, get_href) == dummy_href

    # For the Stac client, we need a valid (or mocked, in our case) URL
    # or the constructor will fail.
    with pytest.raises(RuntimeError):
        rs_client.get_stac_client()

    # If we use the global URL, it should be returned
    stac_client = RsClient(  # pylint: disable=redefined-outer-name
        mocked_stac_catalog_url,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_stac_client()
    assert stac_client.href_catalog == mocked_stac_catalog_url

    # It can be overriden by the env var, but a dummy URL will raise a pystac exception
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("RSPY_HOST_CATALOG", dummy_href)
        with pytest.raises(pystac_client.exceptions.APIError):
            RsClient(mocked_stac_catalog_url, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()


def test_api_key_by_env_var():
    """Test that we can pass the API key by environment variable."""

    # Test that we can pass it by argument
    rs_client = RsClient("", RS_SERVER_API_KEY, owner_id=OWNER_ID)  # no global href
    assert rs_client.rs_server_api_key == RS_SERVER_API_KEY

    # Else, if we don't pass it and we don't have the env var, it will be None
    rs_client = RsClient("", owner_id=OWNER_ID)
    assert rs_client.rs_server_api_key is None

    # Else we can pass it by env var
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("RSPY_APIKEY", RS_SERVER_API_KEY)
        rs_client = RsClient("", owner_id=OWNER_ID)
        assert rs_client.rs_server_api_key == RS_SERVER_API_KEY


def test_cadip_sessions():
    """
    Test CadipClient.search_sessions
    """

    # Note: do some basic tests only. More extensive tests are done on the rs-server side.

    # Dummy values
    session_ids = ["id1", "id2"]
    start_date = datetime(2000, 1, 1)
    stop_date = datetime(2001, 1, 1)
    url = "http://mocked_cadip_url"
    cadip_client = RsClient(url, RS_SERVER_API_KEY, OWNER_ID).get_cadip_client(  # pylint: disable=redefined-outer-name
        CADIP_STATION,
    )

    # Test the connection error with the dummy server
    with pytest.raises(RuntimeError) as error:
        cadip_client.search_sessions(session_ids, start_date, stop_date, PLATFORMS)
    assert "ConnectionError" in str(error.getrepr())

    # Mock the response, now the call should work
    params = {
        "id": "id1,id2",
        "platform": "S1A,S2A",
        "start_date": start_date.strftime(DATETIME_FORMAT),
        "stop_date": stop_date.strftime(DATETIME_FORMAT),
    }
    mock_url = f"{url}/cadip/CADIP/session?{urllib.parse.urlencode(params)}"
    features = ["feature1", "feature2"]
    content = {"features": features}

    # Test a bad response content format
    with pytest.raises(RuntimeError) as error:
        with responses.RequestsMock() as resp:
            resp.get(url=mock_url, json={}, status=200)
            cadip_client.search_sessions(session_ids, start_date, stop_date, PLATFORMS)
    assert "KeyError" in str(error.getrepr())

    # Test a bad response status code
    with responses.RequestsMock() as resp:
        resp.get(url=mock_url, json=content, status=500)
        sessions = cadip_client.search_sessions(session_ids, start_date, stop_date, PLATFORMS)
        assert not sessions

    # Test the nominal case
    with responses.RequestsMock() as resp:
        resp.get(url=mock_url, json=content, status=200)
        sessions = cadip_client.search_sessions(session_ids, start_date, stop_date, PLATFORMS)
        assert sessions == features


@responses.activate
def test_cached_apikey_security(monkeypatch):
    """
    Test that we are caching the call results to the apikey_security function, that calls the
    apikey manager service and keycloak to check the apikey validity and information.
    """

    # Use a dummy URL to simulate the fact that we are in cluster mode (not local mode)
    dummy_href = "http://DUMMY_HREF"
    rs_client = RsClient(dummy_href, RS_SERVER_API_KEY, OWNER_ID)  # no global href

    # Mock the uac manager url
    monkeypatch.setenv("RSPY_UAC_CHECK_URL", RSPY_UAC_CHECK_URL)

    # Initial response expected from the function
    initial_response = {
        "iam_roles": ["initial", "roles"],
        "config": {"initial": "config"},
        "user_login": "initial_login",
    }

    # Clear the cached response and mock the uac manager response
    RsClient.apikey_security_cache.clear()
    responses.get(url=RSPY_UAC_CHECK_URL, status=200, json=initial_response)

    # Check the apikey_security result
    assert rs_client.apikey_iam_roles == initial_response["iam_roles"]
    assert rs_client.apikey_config == initial_response["config"]
    assert rs_client.apikey_user_login == initial_response["user_login"]

    # If the UAC manager response changes, we won't see it because the previous result was cached
    modified_response = {
        "iam_roles": ["modified", "roles"],
        "config": {"modified": "config"},
        "user_login": "modified_login",
    }
    responses.get(url=RSPY_UAC_CHECK_URL, status=200, json=modified_response)

    # Still the initial response !
    for _ in range(100):
        assert rs_client.apikey_iam_roles == initial_response["iam_roles"]
        assert rs_client.apikey_config == initial_response["config"]
        assert rs_client.apikey_user_login == initial_response["user_login"]

    # We have to clear the cache to obtain the modified response
    RsClient.apikey_security_cache.clear()
    assert rs_client.apikey_iam_roles == modified_response["iam_roles"]
    assert rs_client.apikey_config == modified_response["config"]
    assert rs_client.apikey_user_login == modified_response["user_login"]
