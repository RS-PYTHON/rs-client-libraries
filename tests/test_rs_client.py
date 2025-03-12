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

import getpass

import pytest
import responses

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_client.catalog_client import CatalogClient
from rs_client.rs_client import RsClient
from rs_common.config import EAuxipStation, ECadipStation

from .conftest import ADGS_STATION, CADIP_STATION, RS_SERVER_API_KEY, RSPY_UAC_CHECK_URL


@pytest.mark.unit
def test_get_child_client(auxip_client, cadip_client, stac_client):  # pylint: disable=redefined-outer-name
    """Test get_auxip_client, get_cadip_client, get_stac_client"""
    assert isinstance(auxip_client, AuxipClient)
    assert isinstance(cadip_client, CadipClient)
    assert isinstance(stac_client, CatalogClient)


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
    assert isinstance(generic_rs_client.get_catalog_client(), CatalogClient)


@pytest.mark.unit
@responses.activate
def test_apikey_security(mocker):
    """
    Test that we are caching the call results to the apikey_security function, that calls the
    apikey manager service and keycloak to check the apikey validity and information.
    """

    # Use a dummy URL to simulate the fact that we are in cluster mode (not local mode)
    dummy_href = "https://DUMMY_HREF"

    # Mock the uac manager url global variable to simulate cluster mode. See: https://stackoverflow.com/a/69685866
    mocker.patch("rs_client.rs_client.RSPY_UAC_CHECK_URL", new=RSPY_UAC_CHECK_URL, autospec=False)

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
def test_oauth2_security(mocker, monkeypatch):
    """
    Test the oauth2 security that calls the rs-server endpoint and keycloak to check the user information.
    """

    # Use a dummy URL to simulate the fact that we are in cluster mode (not local mode)
    dummy_href = "https://DUMMY_HREF"

    # Mock the uac manager url global variable to simulate cluster mode. See: https://stackoverflow.com/a/69685866
    mocker.patch("rs_client.rs_client.RSPY_UAC_CHECK_URL", new=RSPY_UAC_CHECK_URL, autospec=False)

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


@responses.activate
@pytest.mark.parametrize("mode", ["local", "hybrid", "cluster"])
def test_owner_id(mode, mocker, monkeypatch):
    """
    Test different ways to set the owner_id, in local, hybrid and cluster mode.
    """
    local = mode == "local"
    hybrid = mode == "hybrid"
    cluster = mode == "cluster"

    # Configure the mode. The server URL is set only in hybrid and cluster modes.
    dummy_href = "https://DUMMY_HREF"
    rs_server_href = None if local else dummy_href

    # The uac manager url is set only in cluster mode
    if cluster:
        mocker.patch("rs_client.rs_client.RSPY_UAC_CHECK_URL", new=RSPY_UAC_CHECK_URL, autospec=False)

    # Different owner_id values, depending on how it is set. Don't use special characters.
    by_param = "param"
    by_envvar = "envvar"
    by_apikey = "apikey"
    by_oauth2 = "oauth2"

    # Error messages
    error_auth = "API key or OAuth2 cookie is mandatory"
    error_hybrid = "In hybrid mode, the owner_id must be set explicitly"

    # If the owner_id is not set, in local mode, it takes the system username
    if local:
        assert RsClient(rs_server_href).owner_id == getpass.getuser()
    # In hybrid or cluster mode, we have an exception saying the apikey or oauth2 must be set
    else:
        with pytest.raises(RuntimeError) as e:
            RsClient(rs_server_href)
        assert error_auth in str(e.value)

    # Try setting owner_id from lowest to hight priority ways. It can be deduced from the oauth2.
    monkeypatch.setenv("RSPY_OAUTH2_COOKIE", "RSPY_OAUTH2_COOKIE")
    responses.get(url=f"{dummy_href}/auth/me", status=200, json={"user_login": by_oauth2, "iam_roles": []})
    # In local mode we don't use neither apikey or oauth2
    if local:
        assert RsClient(rs_server_href).owner_id == getpass.getuser()
    # In hybrid mode and don't use oauth2 and the URL to get api key info is unreachable
    elif hybrid:
        with pytest.raises(RuntimeError) as e:
            RsClient(rs_server_href)
        assert error_hybrid in str(e.value)
    # In cluster mode we deduce the owner id from the oauth2 cookie
    elif cluster:
        assert RsClient(rs_server_href).owner_id == by_oauth2

    # owner_id deduced from the API key has higher priority than from oauth2.
    RsClient.apikey_security_cache.clear()
    responses.get(url=RSPY_UAC_CHECK_URL, status=200, json={"user_login": by_apikey, "iam_roles": [], "config": {}})
    if local:
        assert RsClient(rs_server_href, RS_SERVER_API_KEY).owner_id == getpass.getuser()
    elif hybrid:
        with pytest.raises(RuntimeError) as e:
            RsClient(rs_server_href, RS_SERVER_API_KEY)
        assert error_hybrid in str(e.value)
    elif cluster:
        assert RsClient(rs_server_href, RS_SERVER_API_KEY).owner_id == by_apikey

    # owner_id set by env var has higher priority than deduced from api key or oauth2
    monkeypatch.setenv("RSPY_HOST_USER", by_envvar)
    assert RsClient(rs_server_href, RS_SERVER_API_KEY).owner_id == by_envvar

    # owner_id set by parameter has higher priority than all others
    assert RsClient(rs_server_href, RS_SERVER_API_KEY, by_param).owner_id == by_param


def test_log_and_raise_runtime_error(generic_rs_client, mocker):
    """Test log_and_raise logs the message and raises RuntimeError."""
    mock_logger = mocker.patch.object(generic_rs_client.logger, "exception")  # Mock logger.exception
    original_exception = ValueError("Original exception")

    with pytest.raises(RuntimeError, match="Test exception message") as exc_info:
        generic_rs_client.log_and_raise("Test exception message", original_exception)

    # Ensure logger.exception was called with the correct message
    mock_logger.assert_called_once_with("Test exception message")

    # Verify the RuntimeError was raised
    assert isinstance(exc_info.value, RuntimeError)


def test_log_and_raise_exception_chaining(generic_rs_client):
    """Ensure log_and_raise correctly chains exceptions."""
    original_exception = ValueError("Original exception")

    with pytest.raises(RuntimeError) as exc_info:
        generic_rs_client.log_and_raise("Test exception message", original_exception)

    # Check if the cause of RuntimeError is the original exception
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert str(exc_info.value.__cause__) == "Original exception"
