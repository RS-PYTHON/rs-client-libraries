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

import pytest
import requests
import responses
from pystac_client.exceptions import APIError

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_client.rs_client import RsClient
from rs_client.stac_client import StacClient
from rs_common.config import EAuxipStation, ECadipStation, EPlatform

# Use dummy values
RSPY_UAC_CHECK_URL = "http://www.rspy-uac-manager.com"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = "OWNER_ID"
CADIP_STATION = "CADIP"
ADGS_STATION = "ADGS"
PLATFORMS = [EPlatform.S1A, EPlatform.S2A]


@pytest.fixture(name="generic_rs_client")
def generic_rs_client_(mocked_stac_catalog_url, monkeypatch):
    """Return a generic RsClient instance for testing."""
    monkeypatch.setenv("RSPY_OAUTH2_COOKIE", "RSPY_OAUTH2_COOKIE")
    yield RsClient(mocked_stac_catalog_url, RS_SERVER_API_KEY, OWNER_ID)  # will be used to test the StacClient


@pytest.fixture(name="auxip_client")
def auxip_client_(generic_rs_client):
    """Return a generic AuxipClient instance for testing."""
    yield generic_rs_client.get_auxip_client(ADGS_STATION)


@pytest.fixture(name="cadip_client")
def cadip_client_(generic_rs_client):
    """Return a generic CadipClient instance for testing."""
    yield generic_rs_client.get_cadip_client(CADIP_STATION)


@pytest.fixture(name="stac_client")
def stac_client_(generic_rs_client):
    """Return a generic StacClient instance for testing."""
    yield generic_rs_client.get_stac_client()


@pytest.mark.unit
def test_get_child_client(auxip_client, cadip_client, stac_client):  # pylint: disable=redefined-outer-name
    """Test get_auxip_client, get_cadip_client, get_stac_client"""
    assert isinstance(auxip_client, AuxipClient)
    assert isinstance(cadip_client, CadipClient)
    assert isinstance(stac_client, StacClient)


@pytest.mark.unit
def test_station_names(generic_rs_client):  # pylint: disable=redefined-outer-name
    """Test the station name returned by the AuxipClient and CadipClient"""
    # Try with invalid stations name, should raise runtime
    with pytest.raises(RuntimeError):
        generic_rs_client.get_auxip_client("Invalid")
    with pytest.raises(RuntimeError):
        generic_rs_client.get_auxip_client(ECadipStation.CADIP)
    with pytest.raises(RuntimeError):
        generic_rs_client.get_cadip_client("Invalid")
    with pytest.raises(RuntimeError):
        generic_rs_client.get_cadip_client(EAuxipStation.ADGS)

    # Try with station  as str
    assert "ADGS" in generic_rs_client.get_auxip_client(ADGS_STATION).station_name
    assert "CADIP" in generic_rs_client.get_cadip_client(CADIP_STATION).station_name
    # Try with station as enum
    assert "ADGS" in generic_rs_client.get_auxip_client(EAuxipStation.ADGS).station_name
    assert "CADIP" in generic_rs_client.get_cadip_client(ECadipStation.CADIP).station_name
    assert isinstance(generic_rs_client.get_stac_client(), StacClient)


@pytest.mark.unit
@responses.activate
def test_apikey_security(monkeypatch):
    """
    Test that we are caching the call results to the apikey_security function, that calls the
    apikey manager service and keycloak to check the apikey validity and information.
    """

    # Use a dummy URL to simulate the fact that we are in cluster mode (not local mode)
    dummy_href = "https://DUMMY_HREF"

    # Mock the uac manager url
    monkeypatch.setenv("RSPY_UAC_CHECK_URL", RSPY_UAC_CHECK_URL)

    # Initial response expected from the function
    initial_response = {
        "iam_roles": ["initial", "roles"],
        "config": {"initial": "config"},
        "user_login": "initiallogin",  # no special characters
    }

    # Clear the cached response and mock the uac manager response
    RsClient.apikey_security_cache.clear()
    responses.get(url=RSPY_UAC_CHECK_URL, status=200, json=initial_response)

    # Init RsClient
    rs_client = RsClient(dummy_href, RS_SERVER_API_KEY, owner_id=None)

    # Check the apikey_security result
    assert rs_client.apikey_iam_roles == initial_response["iam_roles"]
    assert rs_client.apikey_config == initial_response["config"]
    assert rs_client.apikey_user_login == initial_response["user_login"]

    # Check that the owner id is taken from the apikey user login
    assert rs_client.owner_id == initial_response["user_login"]

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


@pytest.mark.unit
@responses.activate
def test_oauth2_security(monkeypatch):
    """
    Test the oauth2 security that calls the rs-server endpoint and keycloak to check the user information.
    """

    # Use a dummy URL to simulate the fact that we are in cluster mode (not local mode)
    dummy_href = "https://DUMMY_HREF"

    # Mocked user information from keycloak
    auth_info = {
        "user_login": "ownerid",  # no special characters
        "iam_roles": ["role2", "role1", "role3"],
    }

    # Mocked cookie value that allows to call the rs-server endpoint
    monkeypatch.setenv("RSPY_OAUTH2_COOKIE", "RSPY_OAUTH2_COOKIE")

    # Mock the rs-server response
    responses.get(url=f"{dummy_href}/auth/me", status=200, json=auth_info)

    # Init RsClient
    rs_client = RsClient(dummy_href)

    # Check the oauth2_security result
    assert rs_client.oauth2_iam_roles == auth_info["iam_roles"]
    assert rs_client.oauth2_user_login == auth_info["user_login"]

    # Check that the owner id is taken from the user login
    assert rs_client.owner_id == auth_info["user_login"]


@pytest.mark.unit
def test_no_security():
    """If no apikey or oauth2 cookie is present, we should have an error."""

    # Use a dummy URL to simulate the fact that we are in cluster mode (not local mode)
    dummy_href = "https://DUMMY_HREF"

    with pytest.raises(RuntimeError):
        RsClient(dummy_href)  # "API key or OAuth2 cookie is mandatory for RS-Server authentication"


class TestRSClient:
    """Test class to group all RSClient methods."""

    @pytest.mark.unit
    def test_cadip_auxip_get_landing(self, mocker, auxip_client, cadip_client):
        """Test GET landing page."""
        mock_landing = mocker.patch("rs_client.rs_client.RsClient.get_landing", return_value={})
        auxip_client.get_landing()
        cadip_client.get_landing()
        assert mock_landing.call_count == 2

    @pytest.mark.unit
    def test_cadip_auxip_get_collections(self, mocker, auxip_client, cadip_client):
        """Test to get all client collections."""
        mock_collections = mocker.patch("rs_client.rs_client.RsClient.get_collections", return_value=[])
        auxip_client.get_collections()
        cadip_client.get_collections()
        assert mock_collections.call_count == 2

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "client, collection_id",
        [("auxip_client", "auxip_collection"), ("cadip_client", "cadip_collection")],
    )
    def test_cadip_auxip_get_collection(self, mocker, client, collection_id, request):
        """Test get valid collection id."""
        client_instance = request.getfixturevalue(client)

        mock_get_collection = mocker.patch("rs_client.rs_client.RsClient.get_collection", return_value=[])

        client_instance.get_collection(collection_id)

        mock_get_collection.assert_called_once_with(collection_id)

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_cadip_auxip_get_invalid_collection(self, mocker, client, request):
        """Test a invalid collection, should result in a empty response."""
        client_instance = request.getfixturevalue(client)

        mock_get_collection = mocker.patch.object(client_instance.ps_client, "get_collection", side_effect=APIError)

        collection = client_instance.get_collection("invalid_collection")

        mock_get_collection.assert_called_once_with("invalid_collection")
        assert not collection

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_cadip_auxip_get_collection_queryables(self, mocker, client, request):
        """Test to get a specific collection queryables."""
        client_instance = request.getfixturevalue(client)
        mock_get_queryables = mocker.patch.object(client_instance.ps_client, "get_merged_queryables", return_value={})
        client_instance.get_collection_queryables("valid_collection")
        mock_get_queryables.assert_called_once_with(["valid_collection"])

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "client, url",
        [
            ("auxip_client", "http://mocked_stac_catalog_url/auxip/queryables"),
            ("cadip_client", "http://mocked_stac_catalog_url/cadip/queryables"),
        ],
    )
    @responses.activate
    def test_cadip_auxip_get_queryables_error(self, client, url, request):
        """Test a bad response while requesting queryables."""
        client_instance = request.getfixturevalue(client)
        with pytest.raises(
            RuntimeError,
            match=f"Could not get queryables from {url}",
        ), responses.RequestsMock() as resp:
            # If /queryables return 404, then rs_client should raise Runtime
            resp.add(responses.GET, url=url, json={}, status=404)
            client_instance.get_queryables()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "client, url",
        [
            ("auxip_client", "http://mocked_stac_catalog_url/auxip/queryables"),
            ("cadip_client", "http://mocked_stac_catalog_url/cadip/queryables"),
        ],
    )
    @responses.activate
    def test_cadip_auxip_get_queryables_error_unwrapping(self, client, url, request):
        """Test a unwrapping error while requesting queryables."""
        client_instance = request.getfixturevalue(client)
        with pytest.raises(RuntimeError, match=f"Invalid JSON response from {url}"), responses.RequestsMock() as resp:
            # If /queryables return empty, then rs_client should raise Runtime
            resp.add(responses.GET, url=url, status=200)
            client_instance.get_queryables()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "client, url",
        [
            ("auxip_client", "http://mocked_stac_catalog_url/auxip/queryables"),
            ("cadip_client", "http://mocked_stac_catalog_url/cadip/queryables"),
        ],
    )
    @responses.activate
    def test_cadip_auxip_get_queryables_error_timeout(self, client, url, request):
        """Test a timeout when requesting queryables."""
        client_instance = request.getfixturevalue(client)

        def timeout_callback(request):
            raise requests.exceptions.Timeout("Request timed out")

        with pytest.raises(
            RuntimeError,
            match=f"Could not get the response from the endpoint {url}",
        ), responses.RequestsMock() as resp:
            # If /queryables result in a timeout, should raise runtimeerror
            resp.add_callback(responses.GET, url, callback=timeout_callback)
            client_instance.get_queryables()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "client, url",
        [
            ("auxip_client", "http://mocked_stac_catalog_url/auxip/queryables"),
            ("cadip_client", "http://mocked_stac_catalog_url/cadip/queryables"),
        ],
    )
    @responses.activate
    def test_cadip_auxip_get_queryables(self, client, url, request):
        """Test to verify the correct return of queryables."""
        client_instance = request.getfixturevalue(client)
        with responses.RequestsMock() as resp:
            # Valid
            resp.add(responses.GET, url=url, json={"Q1_name": "Q1_value"}, status=200)
            queryables = client_instance.get_queryables()
            assert {"Q1_name": "Q1_value"} == queryables
