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

"""Unit tests for CatalogClient, AuxipClient, CadipClient."""

import pytest
import requests
import responses
from pystac_client.exceptions import APIError

MOCKED_URL = "https://mocked_stac_catalog_url/"


class TestStacBase:
    """Test class to group all StacBase methods."""

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_cadip_auxip_get_landing(self, client, request):
        """Test GET landing page."""
        client_instance = request.getfixturevalue(client)
        assert isinstance(client_instance.get_landing(), dict)

    @pytest.mark.unit
    def test_cadip_auxip_get_collections(self, mocker, auxip_client, cadip_client):
        """Test to get all client collections."""
        mock_collections = mocker.patch("rs_client.stac_base.StacBase.get_collections", return_value=[])
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

        mock_get_collection = mocker.patch("rs_client.stac_base.StacBase.get_collection", return_value=[])

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
    def test_cadip_auxip_get_invalid_items(self, mocker, client, request):
        """Test get_items from an invalid collection, should return None"""
        client_instance = request.getfixturevalue(client)
        mocker.patch.object(client_instance.ps_client, "get_collection", return_value=None)
        assert not client_instance.get_items("invalid_collection")

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_cadip_auxip_get_valid_items(self, mocker, client, request):
        """Test get_items from a valid collection."""
        client_instance = request.getfixturevalue(client)
        mock_collection = mocker.MagicMock()
        mock_collection.get_items.return_value = {"Item1": "data"}
        mocker.patch.object(client_instance.ps_client, "get_collection", return_value=mock_collection)

        assert client_instance.get_items("valid_collection") == {"Item1": "data"}
        client_instance.ps_client.get_collection.assert_called_once_with("valid_collection")
        mock_collection.get_items.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_cadip_auxip_get_invalid_item(self, mocker, client, request):
        """Test to get an invalid should return None"""
        client_instance = request.getfixturevalue(client)
        mocker.patch.object(client_instance.ps_client, "get_collection", return_value=None)
        assert not client_instance.get_item("invalid_collection", "invalid_item")

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_cadip_auxip_get_valid_item(self, mocker, client, request):
        """Test valid get_item from a valid collection."""
        client_instance = request.getfixturevalue(client)
        mock_collection = mocker.MagicMock()
        mock_collection.get_item.return_value = {"Item1": "data"}
        mocker.patch.object(client_instance.ps_client, "get_collection", return_value=mock_collection)

        assert client_instance.get_item("valid_collection", "Item1") == {"Item1": "data"}
        client_instance.ps_client.get_collection.assert_called_once_with("valid_collection")
        mock_collection.get_item.assert_called_once_with("Item1")

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
            ("auxip_client", MOCKED_URL + "auxip/queryables"),
            ("cadip_client", MOCKED_URL + "cadip/queryables"),
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
            ("auxip_client", MOCKED_URL + "auxip/queryables"),
            ("cadip_client", MOCKED_URL + "cadip/queryables"),
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
            ("auxip_client", MOCKED_URL + "auxip/queryables"),
            ("cadip_client", MOCKED_URL + "cadip/queryables"),
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
            ("auxip_client", MOCKED_URL + "auxip/queryables"),
            ("cadip_client", MOCKED_URL + "cadip/queryables"),
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
