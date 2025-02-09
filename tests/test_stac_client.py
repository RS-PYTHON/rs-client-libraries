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

"""All tests for the Stac Client."""

from datetime import datetime

from pystac import Collection, Extent, Item, SpatialExtent, TemporalExtent

from rs_client.rs_client import RsClient
from rs_client.stac_client import StacClient

RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = "OWNER_ID"


def test_create_object_stac_client(mocked_stac_catalog_url):  # pylint: disable=missing-function-docstring
    #####################
    # Loads the catalog #
    #####################
    catalog: StacClient = RsClient(mocked_stac_catalog_url, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()
    assert catalog.id == "stac-fastapi"


def test_get_collection_stac_client(mocked_stac_catalog_get_collection):  # pylint: disable=missing-function-docstring
    catalog: StacClient = RsClient(mocked_stac_catalog_get_collection, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

    ##################################################
    # Get the collection S1_L1 from jgaucher catalog #
    ##################################################

    collection = catalog.get_collection(collection_id="S1_L1", owner_id="toto")
    assert collection.id == "S1_L1"


def test_all_collections_stac_client(mocked_stac_catalog_get_collection):  # pylint: disable=missing-function-docstring
    catalog: StacClient = RsClient(mocked_stac_catalog_get_collection, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

    #######################################################
    # Get all the collections accessible from pyteam user #
    #######################################################

    collections = catalog.get_collections()
    for collection in collections:
        assert collection is not None


def test_get_items_stac_client(mocked_stac_catalog_get_collection):  # pylint: disable=missing-function-docstring
    catalog: StacClient = RsClient(mocked_stac_catalog_get_collection, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

    ###################################################
    # Get all the item from the collection toto:S1_L1 #
    ###################################################

    collection = catalog.get_collection(collection_id="S1_L1", owner_id="toto")

    items = collection.get_all_items()
    for item in items:
        print(item)


def test_create_new_collection_stac_client():  # pylint: disable=missing-function-docstring
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


def test_add_collection_stac_client(mocked_stac_catalog_add_collection):  # pylint: disable=missing-function-docstring
    import os
    print(f"RSPY_HOST_CATALOG = {os.getenv('RSPY_HOST_CATALOG', None)}")
    catalog: StacClient = RsClient(mocked_stac_catalog_add_collection, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

    spatial = SpatialExtent(bboxes=[[-94.6911621, 37.0332547, -94.402771, 37.1077651]])
    date_strings = ["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]
    date_objects: list[datetime | None] = [  # mypy complains without this | None
        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ") for date_str in date_strings
    ]
    temporal = TemporalExtent(intervals=date_objects)
    extent = Extent(spatial=spatial, temporal=temporal)
    new_collection = Collection(id="S2_L2", description="S2_L2 collection.", extent=extent)

    new_collection_jgaucher = Collection(id="S3_L3", description="S3_L3 collection.", extent=extent)

    ###########################################
    # Publish a new collection in the catalog #
    ###########################################

    response = catalog.add_collection(new_collection)
    assert response.status_code == 200

    response = catalog.add_collection(new_collection_jgaucher)
    assert response.status_code == 200


def test_delete_collection_stac_client(
    mocked_stac_catalog_delete_collection,
):  # pylint: disable=missing-function-docstring
    catalog: StacClient = RsClient(mocked_stac_catalog_delete_collection, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

    #######################
    # Delete a collection #
    #######################

    response = catalog.remove_collection(collection_id="S1_L1", owner_id="toto")  # default owner_id is 'pyteam'
    assert response.status_code == 200


def test_add_item_stac_client(mocked_stac_catalog_add_item):  # pylint: disable=missing-function-docstring
    catalog: StacClient = RsClient(mocked_stac_catalog_add_item, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

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
        "owner": "jgaucher",
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
    response = catalog.add_item(collection_id="S1_L1", item=item, owner_id="toto")

    print(response)


def test_remove_item_stac_client(mocked_stac_catalog_delete_item):  # pylint: disable=missing-function-docstring
    catalog: StacClient = RsClient(mocked_stac_catalog_delete_item, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

    ##################
    # Delete an item #
    ##################

    response = catalog.remove_item("S1_L1", "item_0", "toto")
    assert response.status_code == 200


def test_search_item_inside_collection_stac_client_mock(
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
    catalog: StacClient = RsClient(
        mocked_stac_catalog_search_inside_collection,
        RS_SERVER_API_KEY,
        OWNER_ID,
    ).get_stac_client()

    response = catalog.search_inside_collection(owner_id="toto", collection_id="S1_L1")

    expected_ids = [
        "DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw",
        "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ",
    ]

    count = 0

    for count, item in enumerate(response.items()):
        assert item.collection_id == "S1_L1"
        assert item.id == expected_ids[count]

    assert count == 1  # count should be 1 for two items
