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

import logging

import pytest
import requests
import responses
from pystac import Collection
from pystac_client.exceptions import APIError

from rs_client.stac.stac_base import StacBase

MOCKED_URL = "https://mocked_stac_catalog_url/"


class TestStacBase:
    """Test class to group all StacBase methods."""

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client", "edrs_client"])
    def test_cadip_auxip_get_landing(self, client, request):
        """Test GET landing page."""
        client_instance = request.getfixturevalue(client)
        assert isinstance(client_instance.get_landing(), dict)

    @pytest.mark.unit
    def test_cadip_auxip_get_collections(self, mocker, auxip_client, cadip_client, edrs_client):
        """Test to get all client collections."""
        mock_collections = mocker.patch("rs_client.stac.stac_base.StacBase.get_collections", return_value=[])
        auxip_client.get_collections()
        cadip_client.get_collections()
        edrs_client.get_collections()
        assert mock_collections.call_count == 3

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "client, collection_id",
        [
            ("auxip_client", "auxip_collection"),
            ("cadip_client", "cadip_collection"),
            ("edrs_client", "edrs_collection"),
        ],
    )
    def test_cadip_auxip_get_collection(self, mocker, client, collection_id, request):
        """Test get valid collection id."""
        client_instance = request.getfixturevalue(client)

        mock_get_collection = mocker.patch("rs_client.stac.stac_base.StacBase.get_collection", return_value=[])

        client_instance.get_collection(collection_id)

        mock_get_collection.assert_called_once_with(collection_id)

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client", "edrs_client"])
    def test_cadip_auxip_get_invalid_collection(self, mocker, client, request):
        """Test a invalid collection, should result in a empty response."""
        client_instance = request.getfixturevalue(client)

        mock_get_collection = mocker.patch.object(
            client_instance.ps_client,
            "get_collection",
            side_effect=APIError("API failure"),
        )
        with pytest.raises(RuntimeError, match="Pystac client returned exception: API failure"):
            client_instance.get_collection("invalid_collection")

        mock_get_collection.assert_called_once_with("invalid_collection")

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_cadip_auxip_get_invalid_items(self, mocker, client, request):
        """Test get_items from an invalid collection, should return None"""
        client_instance = request.getfixturevalue(client)
        mock_collection = mocker.MagicMock(spec=Collection)
        mock_collection.get_items.side_effect = APIError("API failure")  # Simulate APIError
        mocker.patch.object(client_instance.ps_client, "get_collection", return_value=mock_collection)

        with pytest.raises(RuntimeError, match="Pystac client returned exception: API failure"):
            list(client_instance.get_items("test-collection"))

        mock_collection.get_items.assert_called_once()

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
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client", "edrs_client"])
    def test_cadip_auxip_get_invalid_item(self, mocker, client, request):
        """Test to get an invalid should return None"""
        client_instance = request.getfixturevalue(client)
        mock_collection = mocker.MagicMock(spec=Collection)
        mock_collection.get_item.return_value = None  # Simulate missing item
        mocker.patch.object(client_instance.ps_client, "get_collection", return_value=mock_collection)

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
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client", "edrs_client"])
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
            ("edrs_client", MOCKED_URL + "edrs/queryables"),
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

    @pytest.mark.unit
    @pytest.mark.parametrize("client", ["auxip_client", "cadip_client"])
    def test_handle_api_error_decorator(self, client, mocker, request):
        """Test APIError while GET landing page."""
        client_instance = request.getfixturevalue(client)
        mocker.patch.object(client_instance.ps_client, "to_dict", side_effect=APIError)
        with pytest.raises(RuntimeError, match="Pystac client returned exception:"):
            client_instance.get_landing()


# Local stubs used to cover the EDRS-specific branches added in StacBase.get_items.
# These keep existing tests unchanged while providing minimal objects to drive the new code paths.
class StubLink:  # pylint: disable=too-few-public-methods
    """Minimal link stub used by StubCollection for get_single_link('items')."""

    def __init__(self, href: str):
        """Store a fake href."""
        self._href = href

    def get_href(self) -> str:
        """Return the stored href."""
        return self._href


class StubCollection:
    """Minimal collection stub providing items link and per-item fetch tracking."""

    def __init__(self, has_items_link: bool = True):
        """Track fetched items and optionally expose an 'items' link."""
        self.items_fetched: list[str] = []
        self.has_items_link = has_items_link

    def get_single_link(self, rel: str):
        """Return a fake items link when requested."""
        if rel == "items" and self.has_items_link:
            return StubLink("http://fake/items")
        return None

    def get_item(self, item_id: str):
        """Record and return a fake item."""
        self.items_fetched.append(item_id)
        return {"id": item_id}


class StubPsClient:  # pylint: disable=too-few-public-methods
    """Minimal ps_client stub to drive StacBase.get_items branches with _request present."""

    def __init__(self, collection: StubCollection):
        self.collection = collection
        self.request_called = False
        self.last_params = None

    def get_collection(self, _collection_id):
        """Return the stub collection."""
        return self.collection

    def _request(self, method, url, params=None):  # pylint: disable=unused-argument
        """Simulate a /items request storing params."""
        self.request_called = True
        self.last_params = params or {}
        return {"features": ["ok"]}

    def _parse_item_collection(self, response, collection):  # pylint: disable=unused-argument
        """Simulate parsing an item collection."""
        return ["parsed"]


class StubStacBase(StacBase):  # pylint: disable=too-few-public-methods,super-init-not-called
    """Lightweight StacBase with stubbed ps_client."""

    def __init__(self, ps_client):  # pylint: disable=missing-function-docstring,super-init-not-called
        # Bypass parent init; just attach logger/ps_client used by get_items
        self.logger = logging.getLogger("dummy-stac-base")
        self.ps_client = ps_client


class StubStacIO:  # pylint: disable=too-few-public-methods
    """Simple STAC IO stub with read_json support for fallback path."""

    def __init__(self):
        self.last_params = None

    def read_json(self, href, parameters=None):  # pylint: disable=unused-argument
        """Return a minimal FeatureCollection dict, storing the received params."""
        self.last_params = parameters or {}
        return {
            "type": "FeatureCollection",
            "stac_version": "1.0.0",
            "features": [
                {
                    "type": "Feature",
                    "id": "x",
                    "stac_version": "1.0.0",
                    "stac_extensions": [],
                    "properties": {"datetime": "2020-01-01T00:00:00Z"},
                    "geometry": None,
                    "links": [],
                    "assets": {},
                    "collection": "col1",
                },
            ],
            "links": [],
        }


class StubPsClientNoRequest:  # pylint: disable=too-few-public-methods
    """ps_client stub without _request, but with _stac_io.read_json (fallback path)."""

    def __init__(self, collection: StubCollection, stac_io: StubStacIO):
        self.collection = collection
        self._stac_io = stac_io

    def get_collection(self, _collection_id):
        """Return the stub collection."""
        return self.collection


class TestStacBaseExtra:
    """Additional coverage for get_items branches added for EDRS: manual /items and fallback."""

    def test_get_items_with_query_params_manual_items_call(self):
        """
        When query_params are provided, StacBase.get_items should hit the manual /items
        path (EDRS-specific path) and return the parsed collection iterator.
        """
        collection = StubCollection()
        ps_client = StubPsClient(collection)
        base = StubStacBase(ps_client)

        result = list(base.get_items("col1", None, limit=1, page=2))

        assert ps_client.request_called is True
        assert ps_client.last_params.get("limit") == 1
        assert ps_client.last_params.get("page") == 2
        assert result == ["parsed"]

    def test_get_items_with_ids_fetches_individually(self):
        """
        When items_ids are provided, StacBase.get_items should fetch items one by one
        (avoid pystac-client /search fallback).
        """
        collection = StubCollection()
        ps_client = StubPsClient(collection)
        base = StubStacBase(ps_client)

        items = list(base.get_items("col1", items_ids=["a", "b"]))

        assert collection.items_fetched == ["a", "b"]
        assert items == [{"id": "a"}, {"id": "b"}]

    def test_get_items_with_query_params_stac_io_fallback(self):
        """
        If ps_client lacks _request but has _stac_io.read_json, fallback should be used,
        and ids should be injected into params.
        """
        collection = StubCollection()
        stac_io = StubStacIO()
        ps_client = StubPsClientNoRequest(collection, stac_io)
        base = StubStacBase(ps_client)

        items = list(base.get_items("col1", items_ids=["a", "b"], limit=5))

        assert stac_io.last_params.get("ids") == "a,b"
        assert stac_io.last_params.get("limit") == 5
        assert len(items) == 1
        assert items[0].id == "x"

    def test_get_items_with_query_params_missing_link_raises(self):
        """If collection has no items link, a RuntimeError should be raised."""
        collection = StubCollection(has_items_link=False)
        stac_io = StubStacIO()
        ps_client = StubPsClientNoRequest(collection, stac_io)
        base = StubStacBase(ps_client)

        with pytest.raises(RuntimeError, match="has no 'items' link"):
            list(base.get_items("col1", limit=1))
