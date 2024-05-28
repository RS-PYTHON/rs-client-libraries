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

from rs_client.rs_client import RsClient
from rs_client.stac_client import StacClient

RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = "OWNER_ID"


def test_create_object_stac_client(mocked_stac_catalog_url):  # pylint: disable=missing-function-docstring
    #####################
    # Loads the catalog #
    #####################
    catalog = RsClient(mocked_stac_catalog_url, RS_SERVER_API_KEY, OWNER_ID).get_stac_client()

    # ##################################################
    # # Get the collection S1_L1 from jgaucher catalog #
    # ##################################################
    # collection = catalog.get_collection(collection_id="S1_L1", owner_id="jgaucher")
    # assert collection.id == "S1_L1"

    #######################################################
    # Get all the collections accessible from pyteam user #
    #######################################################
    collections = catalog.get_collections()
    for collection in collections:
        print(collection)

    #########################################################
    # Create a new collection S2_L2 in the owner_id catalog ##############################
    # If not specified, the default owner_id will be the value of the attribute owner_id #
    ######################################################################################
    new_collection = catalog.create_new_collection(
        collection_id="S2_L2",
        extent={
            "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
            "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
        },
    )
    print(new_collection["id"])

    ##########################################################
    # Create a new collection S3_L3 specifying the owner id. #
    ##########################################################
    new_collection_jgaucher = catalog.create_new_collection(
        collection_id="S3_L3",
        extent={
            "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
            "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
        },
        description="This is the collection S3_L3 of the user jgaucher",
        owner_id="jgaucher",
    )
    print(new_collection_jgaucher["id"])

    ###########################################
    # Publish a new collection in the catalog #
    ###########################################
    response = catalog.post_collection(new_collection)
    assert response.status_code == 200

    response = catalog.post_collection(new_collection_jgaucher)
    assert response.status_code == 200

    #######################
    # Delete a collection #
    #######################
    response = catalog.delete_collection(collection_id="S2_L2")  # default owner_id is 'pyteam'
    assert response.status_code == 200

    response = catalog.delete_collection(collection_id="S3_L3", owner_id="jgaucher")
    assert response.status_code == 200
