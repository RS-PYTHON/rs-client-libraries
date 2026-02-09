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

# pylint: disable=no-member
"""All tests for the Stac Catalog Client."""

import getpass
import os
from datetime import datetime

import pytest
import responses
from pystac import Collection, Extent, Item, SpatialExtent, TemporalExtent
from requests import HTTPError

from rs_client.rs_client import RsClient
from rs_client.stac.catalog_client import CatalogClient
from tests.common import json_landing_page

from .conftest import MOCKED_RSPY_WEBSITE, RS_SERVER_API_KEY, RSPY_UAC_CHECK_URL

OWNER_ID = "OWNER_ID"


def test_create_object_catalog_client(mocked_rspy_landing_pages):  # pylint: disable=missing-function-docstring
    #####################
    # Loads the catalog #
    #####################
    catalog: CatalogClient = RsClient(MOCKED_RSPY_WEBSITE, RS_SERVER_API_KEY, OWNER_ID).get_catalog_client()
    assert catalog.ps_client.id == "stac-fastapi"


def test_get_collection_catalog_client(
    mocked_stac_catalog_get_collection,
):  # pylint: disable=missing-function-docstring
    catalog: CatalogClient = RsClient(MOCKED_RSPY_WEBSITE, RS_SERVER_API_KEY, OWNER_ID).get_catalog_client()

    ##################################################
    # Get the collection S1_L1 from toto catalog #
    ##################################################

    collection = catalog.get_collection(collection_id="S1_L1", owner_id="toto")
    assert collection.id == "S1_L1" if collection else False


def test_all_collections_catalog_client(
    mocked_stac_catalog_get_collection,
):  # pylint: disable=missing-function-docstring
    catalog: CatalogClient = RsClient(MOCKED_RSPY_WEBSITE, RS_SERVER_API_KEY, OWNER_ID).get_catalog_client()

    #######################################################
    # Get all the collections accessible from pyteam user #
    #######################################################

    collections = catalog.get_collections()
    for collection in collections:
        assert collection is not None


def test_get_items_catalog_client(mocked_stac_catalog_get_collection):  # pylint: disable=missing-function-docstring
    catalog: CatalogClient = RsClient(MOCKED_RSPY_WEBSITE, RS_SERVER_API_KEY, OWNER_ID).get_catalog_client()

    ###################################################
    # Get all the item from the collection toto:S1_L1 #
    ###################################################

    collection = catalog.get_collection(collection_id="S1_L1", owner_id="toto")

    items = collection.get_all_items()  # type: ignore
    assert items


def test_create_new_collection_catalog_client():  # pylint: disable=missing-function-docstring
    spatial = SpatialExtent(bboxes=[[-94.6911621, 37.0332547, -94.402771, 37.1077651]])
    date_strings = ["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]
    date_objects: list[datetime | None] = [  # mypy complains without this | None
        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ") for date_str in date_strings
    ]
    temporal = TemporalExtent(intervals=date_objects)
    extent = Extent(spatial=spatial, temporal=temporal)
    new_collection = Collection(id="S2_L2", description="S2_L2 collection.", extent=extent)

    new_collection_jgaucher = Collection(id="S3_L3", description="S3_L3 collection.", extent=extent)

    assert new_collection.id == "S2_L2"
    assert new_collection_jgaucher.id == "S3_L3"


def test_add_update_collection_catalog_client(
    mocked_stac_catalog_add_update_collection,
):  # pylint: disable=missing-function-docstring
    print(f"RSPY_HOST_CATALOG = {os.getenv('RSPY_HOST_CATALOG', None)}")
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_add_update_collection,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()

    spatial = SpatialExtent(bboxes=[[-94.6911621, 37.0332547, -94.402771, 37.1077651]])
    date_strings = ["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]
    date_objects: list[datetime | None] = [  # mypy complains without this | None
        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ") for date_str in date_strings
    ]
    temporal = TemporalExtent(intervals=date_objects)
    extent = Extent(spatial=spatial, temporal=temporal)
    new_collection = Collection(id="S2_L2", description="S2_L2 collection.", extent=extent)

    new_collection_jgaucher = Collection(id="S3_L3", description="S3_L3 collection.", extent=extent)

    # Publish a new collections in the catalog
    catalog.add_collection(new_collection)
    catalog.add_collection(new_collection_jgaucher)

    # Update a collection
    new_collection.description = "new description"
    catalog.update_collection(new_collection)


def test_add_patch_collection_catalog_client(mocked_stac_catalog_add_patch_collection):
    """Test 'add_collection' and 'patch_collection'"""
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_add_patch_collection,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()

    spatial = SpatialExtent(bboxes=[[-94.6911621, 37.0332547, -94.402771, 37.1077651]])
    date_strings = ["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]
    date_objects: list[datetime | None] = [  # mypy complains without this | None
        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ") for date_str in date_strings
    ]
    temporal = TemporalExtent(intervals=date_objects)
    extent = Extent(spatial=spatial, temporal=temporal)
    new_collection = Collection(id="S2_L2", description="S2_L2 collection.", extent=extent)

    # Publish a new collections in the catalog
    catalog.add_collection(new_collection)

    # Update a collection
    patch_values = {"description": "new description"}
    catalog.patch_collection(collection_id="S2_L2", owner_id="toto", patch_values=patch_values)


def test_add_collection_catalog_client_error(
    mocked_stac_catalog_add_collection_error,
):  # pylint: disable=missing-function-docstring
    print(f"RSPY_HOST_CATALOG = {os.getenv('RSPY_HOST_CATALOG', None)}")
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_add_collection_error,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()

    # Trigger a mocked error response
    response = catalog.http_session.post(f"{catalog.href_service}/catalog/collections")
    with pytest.raises(HTTPError):
        catalog.raise_for_status(response)


def test_delete_collection_catalog_client(
    mocked_stac_catalog_delete_collection,
):  # pylint: disable=missing-function-docstring
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_delete_collection,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()

    #######################
    # Delete a collection #
    #######################

    catalog.remove_collection(collection_id="S1_L1", owner_id="toto")  # default owner_id is 'pyteam'


def test_add_update_item_catalog_client(
    mocked_stac_catalog_add_update_item,
):  # pylint: disable=missing-function-docstring
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_add_update_item,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()

    # Add a new item from toto:S1_L1 collection

    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-94.6334839, 37.0595608],
                [-94.6334839, 37.0332547],
                [-94.6005249, 37.0332547],
                [-94.6005249, 37.0595608],
                [-94.6334839, 37.0595608],
            ],
        ],
    }
    properties = {
        "gsd": 0.5971642834779395,
        "owner": "toto",
        "width": 2500,
        "height": 2500,
        "datetime": "2000-02-02T00:00:00Z",
        "proj:epsg": 3857,
        "orientation": "nadir",
    }
    item = Item(
        id="item_0",
        geometry=geometry,
        bbox=[-180.0, -90.0, 180.0, 90.0],
        datetime=datetime.now(),
        properties=properties,
    )
    catalog.add_item(collection_id="S1_L1", item=item, owner_id="toto")

    # The collection is needed to update an item. In real use-case, it is set by rs-server.
    # Also add a random property.
    item.collection_id = "S1_L1"
    item.properties["new_property"] = "any_value"
    catalog.update_item(item)


def test_add_patch_item_catalog_client(mocked_stac_catalog_add_patch_item):
    """Test 'add_item' and 'patch_item'"""
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_add_patch_item,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()

    # Add a new item from toto:S1_L1 collection
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-94.6334839, 37.0595608],
                [-94.6334839, 37.0332547],
                [-94.6005249, 37.0332547],
                [-94.6005249, 37.0595608],
                [-94.6334839, 37.0595608],
            ],
        ],
    }
    properties = {
        "gsd": 0.5971642834779395,
        "owner": "toto",
        "width": 2500,
        "height": 2500,
        "datetime": "2000-02-02T00:00:00Z",
        "proj:epsg": 3857,
        "orientation": "nadir",
    }
    item = Item(
        id="item_0",
        geometry=geometry,
        bbox=[-180.0, -90.0, 180.0, 90.0],
        datetime=datetime.now(),
        properties=properties,
    )
    catalog.add_item(collection_id="S1_L1", item=item, owner_id="toto")

    # The collection is needed to update an item. In real use-case, it is set by rs-server.
    # Also add a random property.
    patch_values = {"properties": {"width": 3000}}
    catalog.patch_item(
        collection_id="S1_L1",
        item_id=item.id,
        owner_id=item.properties["owner"],
        patch_values=patch_values,
    )


def test_remove_item_catalog_client(mocked_stac_catalog_delete_item):  # pylint: disable=missing-function-docstring
    catalog: CatalogClient = RsClient(mocked_stac_catalog_delete_item, RS_SERVER_API_KEY, OWNER_ID).get_catalog_client()

    ##################
    # Delete an item #
    ##################

    catalog.remove_item("S1_L1", "item_0", "toto")


def test_search_item_inside_collection_catalog_client_mock(
    mocked_stac_catalog_search_inside_collection,
):
    """Test searching items inside a collection
    This test verifies that items within a specific collection are correctly retrieved and
    asserts their properties when the /catalog/collections/[<owner_id>:]<collection_id>/search endpoint
    is called
    Args:
        mocked_stac_catalog_search_inside_collection: Mock object for STAC catalog search
        inside a collection.
    """
    catalog: CatalogClient = RsClient(
        MOCKED_RSPY_WEBSITE,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()
    response = catalog.search(owner_id="toto", collections=["S1_L1"])
    expected_ids = [
        "DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw",
        "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ",
    ]
    count = 0
    for count, item in enumerate(response):  # type: ignore
        assert item.collection_id == "S1_L1"
        assert item.id == expected_ids[count]
    assert count == 1  # count should be 1 for two items


def test_get_invalid_item(mocked_stac_catalog_invalid_get_item):
    """Test that a invalid item from a valid collection result in None."""
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_invalid_get_item,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()
    item_id = "invalid_item"

    item = catalog.get_item("S1_L1", item_id, owner_id="toto")
    assert not item


def test_get_valid_item(mocked_stac_catalog_get_item):
    """Test get_item from a valid collection and a valid item."""
    catalog: CatalogClient = RsClient(
        mocked_stac_catalog_get_item,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_catalog_client()
    item_id = "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.EOF"

    item = catalog.get_item("S1_L1", item_id, owner_id="toto")
    assert item.id == item_id


@responses.activate
@pytest.mark.parametrize("mode", ["local", "hybrid", "cluster"])
def test_owner_id(mode, mocker, monkeypatch):  # pylint: disable=too-many-locals
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
        local_href = "https://dummy-catalog"
        monkeypatch.setenv("RSPY_HOST_CATALOG", local_href)
        responses.get(url=f"{local_href}/catalog/", status=200, json=json_landing_page(local_href, "foo:bar"))
        assert RsClient(rs_server_href).get_catalog_client().owner_id == getpass.getuser()
    # In hybrid or cluster mode, we have an exception saying the apikey or oauth2 must be set
    else:
        responses.get(url=f"{rs_server_href}/catalog/", status=200, json=json_landing_page(dummy_href, "foo:bar"))
        with pytest.raises(RuntimeError) as e:
            RsClient(rs_server_href).get_catalog_client()
        assert error_auth in str(e.value)

    # Try setting owner_id from lowest to hight priority ways. It can be deduced from the oauth2.
    monkeypatch.setenv("RSPY_OAUTH2_COOKIE", "RSPY_OAUTH2_COOKIE")
    responses.get(
        url=f"{dummy_href}/auth/me",
        status=200,
        json={"user_login": by_oauth2, "iam_roles": [], "attributes": {}},
    )
    # In local mode we don't use neither apikey or oauth2
    if local:
        assert RsClient(rs_server_href).get_catalog_client().owner_id == getpass.getuser()
    # In hybrid mode and don't use oauth2 and the URL to get api key info is unreachable
    elif hybrid:
        with pytest.raises(RuntimeError) as e:
            RsClient(rs_server_href).get_catalog_client()
        assert error_hybrid in str(e.value)
    # In cluster mode we deduce the owner id from the oauth2 cookie
    elif cluster:
        assert RsClient(rs_server_href).get_catalog_client().owner_id == by_oauth2

    # owner_id deduced from the API key has higher priority than from oauth2.
    RsClient.apikey_security_cache.clear()
    responses.get(url=RSPY_UAC_CHECK_URL, status=200, json={"user_login": by_apikey, "iam_roles": [], "config": {}})
    if local:
        assert RsClient(rs_server_href, RS_SERVER_API_KEY).get_catalog_client().owner_id == getpass.getuser()
    elif hybrid:
        with pytest.raises(RuntimeError) as e:
            RsClient(rs_server_href, RS_SERVER_API_KEY).get_catalog_client()
        assert error_hybrid in str(e.value)
    elif cluster:
        assert RsClient(rs_server_href, RS_SERVER_API_KEY).get_catalog_client().owner_id == by_apikey

    # owner_id set by env var has higher priority than deduced from api key or oauth2
    monkeypatch.setenv("RSPY_HOST_USER", by_envvar)
    assert RsClient(rs_server_href, RS_SERVER_API_KEY).get_catalog_client().owner_id == by_envvar

    # owner_id set by parameter has higher priority than all others
    assert RsClient(rs_server_href, RS_SERVER_API_KEY, by_param).get_catalog_client().owner_id == by_param
