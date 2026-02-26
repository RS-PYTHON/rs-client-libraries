# Copyright 2023-2026 Airbus, CS Group
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
import responses

from rs_client.rs_client import RsClient
from rs_client.stac.auxip_client import AuxipClient
from rs_client.stac.cadip_client import CadipClient
from rs_client.stac.catalog_client import CatalogClient
from rs_client.stac.edrs_client import EdrsClient
from rs_client.stac.prip_client import PripClient
from tests.common import json_landing_page

from .conftest import RS_SERVER_API_KEY, RSPY_UAC_CHECK_URL


@pytest.mark.unit
def test_get_child_client(  # pylint: disable=redefined-outer-name
    auxip_client,
    cadip_client,
    prip_client,
    stac_client,
    edrs_client,
):
    """Test get_*_client child factories."""
    assert isinstance(auxip_client, AuxipClient)
    assert isinstance(cadip_client, CadipClient)
    assert isinstance(prip_client, PripClient)
    assert isinstance(stac_client, CatalogClient)
    assert isinstance(edrs_client, EdrsClient)


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
    assert rs_client.apikey_attributes == initial_response["config"]
    assert rs_client.apikey_user_login == initial_response["user_login"]

    # Check that the owner id is taken from the apikey user login
    responses.get(url=f"{dummy_href}/catalog/", status=200, json=json_landing_page(dummy_href, "ownerid:collection_id"))
    assert rs_client.get_catalog_client().owner_id == initial_response["user_login"]

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
        assert rs_client.apikey_attributes == initial_response["config"]
        assert rs_client.apikey_user_login == initial_response["user_login"]

    # We have to clear the cache to obtain the modified response
    RsClient.apikey_security_cache.clear()
    assert rs_client.apikey_iam_roles == modified_response["iam_roles"]
    assert rs_client.apikey_attributes == modified_response["config"]
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
        "attributes": {"attr1": "value1", "attr2": "value2"},
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
    assert rs_client.oauth2_attributes == auth_info["attributes"]

    # Check that the owner id is taken from the user login
    responses.get(url=f"{dummy_href}/catalog/", status=200, json=json_landing_page(dummy_href, "ownerid:collection_id"))
    assert rs_client.get_catalog_client().owner_id == auth_info["user_login"]


@pytest.mark.unit
def test_no_security():
    """If no apikey or oauth2 cookie is present, we should have an error."""

    # Use a dummy URL to simulate the fact that we are in cluster mode (not local mode)
    dummy_href = "https://DUMMY_HREF"

    with pytest.raises(RuntimeError):
        RsClient(dummy_href)  # "API key or OAuth2 cookie is mandatory for RS-Server authentication"


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
