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

"""Test rs-client-libraries staging functions"""

import getpass
import json
import os.path as osp
from pathlib import Path
from typing import Any

import pytest
import responses
from starlette import status

from rs_client.ogcapi.ogcapi_client import OgcValidationException
from rs_client.rs_client import RsClient

RESOURCES_FOLDER = Path(osp.realpath(osp.dirname(__file__))) / "resources"
AUXIP = "AUXIP"
CADIP = "CADIP"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"

OWNER_ID = getpass.getuser()
OUTPUT_COLLECTION = "my_test_collection"
TIMEOUT = 5

# -------------------------- Staging fixtures --------------------------


@pytest.fixture(name="dummy_href")
def get_dummy_href():
    """
    Dummy href for local_mode
    """
    dummy_href = "https://DUMMY_HREF"
    return dummy_href


@pytest.fixture(name="staging_client")
def get_staging_client(dummy_href):
    """Create a staging client

    Args:
        href (str): rs_server href for local-mode

    Returns:
        StagingClient: StagingClient object to apply the staging
    """
    client = RsClient(
        rs_server_href=dummy_href,
        rs_server_api_key=RS_SERVER_API_KEY,
        owner_id=OWNER_ID,
        logger=None,
    )
    return client.get_staging_client()


@pytest.fixture(name="cadip_data")
def get_cadip_data():
    """
    Return cadip FeatureCollection as a dictionary
    """
    cadip_data_json = osp.join(RESOURCES_FOLDER, "staging", "cadip_data.json")
    with open(cadip_data_json, encoding="utf-8") as file:
        return json.loads(file.read())


@pytest.fixture(name="cadip_data_link")
def get_cadip_data_link():
    """
    Return cadip link pointing to a FeatureCollection
    """
    return "http://localhost:8002/cadip/search?ids=S1A_20231120061537234567&collections=cadip_sentinel1"


@pytest.fixture(name="auxip_data")
def get_auxip_data():
    """
    Return auxip FeatureCollection as a dictionary
    """
    auxip_data_json = osp.join(RESOURCES_FOLDER, "staging", "auxip_data.json")
    with open(auxip_data_json, encoding="utf-8") as file:
        return json.loads(file.read())


@pytest.fixture(name="auxip_data_link")
def get_auxip_data_link():
    """
    Return cadip link pointing to a FeatureCollection
    """
    return (
        "http://localhost:8001/auxip/search?"
        "ids=S1A_OPER_AUX_PREORB_OPOD_20240527T062732_"
        "V20240527T062732_20240527T062732.EOF&collections=adgs"
    )


@pytest.fixture(name="staging_response_sample")
def get_staging_response_sample():
    """
    Return auxip FeatureCollection as a dictionary
    """
    return {
        "processID": "string",
        "type": "process",
        "jobID": "e390e31c-b274-49d2-88c2-466cc4fe23c9",
        "status": "accepted",
        "message": "string",
        "created": "2019-08-24T14:15:22Z",
        "started": "2019-08-24T14:15:22Z",
        "finished": "2019-08-24T14:15:22Z",
        "updated": "2019-08-24T14:15:22Z",
        "progress": 100,
        "links": [
            {"href": "string", "rel": "service", "type": "application/json", "hreflang": "en", "title": "string"},
        ],
    }


# -------------------------- Test for staging endpoints --------------------------


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, data_fixture, data_link_fixture",
    [
        (CADIP, "cadip_data", "cadip_data_link"),
        (AUXIP, "auxip_data", "auxip_data_link"),
    ],
)
def test_staging_ok(
    station,
    data_fixture,
    data_link_fixture,
    request,
    dummy_href,
    staging_client,
    staging_response_sample,
):  # pylint: disable=R0913, R0917
    """
    Nominal cases for staging
    """
    data_to_stage = request.getfixturevalue(data_fixture)
    data_link_to_stage = request.getfixturevalue(data_link_fixture)
    process_id = "staging"

    # Nominal case - stage a FeatureCollection
    json_response = staging_response_sample

    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    staging_resp = staging_client.run_staging(data_to_stage, OUTPUT_COLLECTION)
    assert staging_resp is not None

    # Nominal case - stage a Feature
    staging_resp = staging_client.run_staging(
        data_to_stage["features"][0],
        OUTPUT_COLLECTION,
    )
    assert staging_resp is not None

    # Nominal case - check that the test pass if the input data is a json file with a valid format
    item_file_to_stage = osp.join(RESOURCES_FOLDER, "staging", f"{station.lower()}_data.json")
    staging_resp = staging_client.run_staging(item_file_to_stage, OUTPUT_COLLECTION)
    assert staging_resp is not None

    # Nominal case - check that the test pass if the input data is a json string with a valid format
    staging_resp = staging_client.run_staging(json.dumps(data_to_stage), OUTPUT_COLLECTION)
    assert staging_resp is not None

    # Nominal case - check that the test pass if the input data is a valid url pointing to
    # a link that returns a STAC itemCollection
    # (for example https://rspy.ops.rs-python.eu/cadip/search?ids=S1A_20241123044108056677&collections=s1_mti)
    staging_resp = staging_client.run_staging(data_link_to_stage, OUTPUT_COLLECTION)
    assert staging_resp is not None


@pytest.mark.unit
@responses.activate
def test_staging_fails_stage_empty_dict(dummy_href, staging_client):
    """
    Failing case where we use an empty dictionary in input of the staging
    In this case an exception should be raised
    """
    process_id = "staging"
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json={},
        status=422,
    )
    with pytest.raises(KeyError) as exc_info:
        staging_client.run_staging(
            {},
            OUTPUT_COLLECTION,
        )
    assert "Key 'type' is missing from the staging input data" in str(exc_info.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, data_fixture, data_link_fixture",
    [
        (CADIP, "cadip_data", "cadip_data_link"),
        (AUXIP, "auxip_data", "auxip_data_link"),
    ],
)
def test_staging_fails_wrong_data_format(  # pylint: disable=R0913, R0917
    station,
    data_fixture,
    data_link_fixture,
    dummy_href,
    staging_client,
    request,
    staging_response_sample,
):
    """
    Failing case where the  input data is a json file
    with an unvalid format - In this case check that a pydantic ValueError is raised
    """
    json_response = staging_response_sample
    process_id = "staging"
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # ----- Check that the test raises an exception if the input file has a wrong data format
    item_file_to_stage = osp.join(RESOURCES_FOLDER, "staging", f"wrong_{station.lower()}_data.json")
    with pytest.raises(ValueError) as exc_info:
        staging_client.run_staging(
            item_file_to_stage,
            OUTPUT_COLLECTION,
        )
    assert "bbox is required if geometry is not null" in str(exc_info.value)

    # ----- Check that we get an exception if we pass in input a json string which is not compliant with stac
    data_to_stage = request.getfixturevalue(data_fixture)
    data_to_stage["features"][0].pop("bbox")
    with pytest.raises(ValueError) as exc_info:
        staging_client.run_staging(json.dumps(data_to_stage), OUTPUT_COLLECTION)
    assert "bbox is required if geometry is not null" in str(exc_info.value)

    # ------ Check that the right exception is raised if we use an unvalid link for the staging
    data_link_to_stage = request.getfixturevalue(data_link_fixture)
    unvalid_link = data_link_to_stage.replace("http://", "")
    with pytest.raises(OgcValidationException) as exc_info:  # type: ignore
        staging_client.run_staging(unvalid_link, OUTPUT_COLLECTION)
    assert "Invalid input format" in str(exc_info.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "data_fixture",
    ["cadip_data", "auxip_data"],
)
def test_staging_fails_endpoint_send_error(data_fixture, request, dummy_href, staging_client):
    """
    Failing case where the staging endpoint fails and return an error status code
    """
    data_to_stage = request.getfixturevalue(data_fixture)
    json_response: dict[Any, Any] = {
        "type": "https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/500",
        "status": 500,
        "detail": "Request body validation error",
    }
    process_id = "staging"

    # Case of a timeout for the staging
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    response = staging_client.run_staging(data_to_stage, OUTPUT_COLLECTION)
    assert "Request body validation error" in response["detail"]
