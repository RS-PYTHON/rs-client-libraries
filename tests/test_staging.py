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

""" Test rs-client-libraries staging functions """

import getpass
import json
import os.path as osp
from pathlib import Path
from typing import Any

import pytest
import responses
from starlette import status

from rs_client.rs_client import RsClient
from rs_common.config import EDownloadStatus

RESOURCES_FOLDER = Path(osp.realpath(osp.dirname(__file__))) / "resources"
AUXIP = "AUXIP"
CADIP = "CADIP"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"

OWNER_ID = getpass.getuser()
OUTPUT_COLLECTION = "my_test_collection"


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
def cadip_data():
    """
    Return cadip FeatureCollection as a dictionary
    """
    cadip_data_json = osp.join(RESOURCES_FOLDER, "staging", "cadip_data.json")
    with open(cadip_data_json, encoding="utf-8") as file:
        return json.loads(file.read())


@pytest.fixture(name="auxip_data")
def auxip_data():
    """
    Return auxip FeatureCollection as a dictionary
    """
    auxip_data_json = osp.join(RESOURCES_FOLDER, "staging", "auxip_data.json")
    with open(auxip_data_json, encoding="utf-8") as file:
        return json.loads(file.read())


@pytest.mark.unit
@responses.activate
def test_get_processes(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    json_response = {
        "processes": [
                {
                    "name": "staging", 
                    "processor": "Staging",
                    "id": "staging_processor",
                    "version": "0.0.1",   
                }
            ],
        "links": [
            {
                "href": "https://example.com/api/service"
            }
        ],      
    }

    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # Check that the job information are returned if we specify a valid job identifier in input
    job_response = staging_client.get_processes()
    assert not job_response.errors
    assert job_response.data == json_response
    
    # check that an exception is raised if the endpoint response
    # is invalid according to the ogc standard (here we removed the required field
    # "id" and ensure that the corresponding validation exception is raised)
    json_response = {
        "processes": [
                {
                    "name": "staging", 
                    "processor": "Staging",
                    "version": "0.0.1",   
                }
            ],
        "links": [
            {
                "href": "https://example.com/api/service"
            }
        ],      
    }

    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # Check that the job information are returned if we specify a valid job identifier in input
    job_response = staging_client.get_processes()
    assert job_response.errors
    assert not job_response.data


@pytest.mark.unit
@responses.activate
def test_get_process(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    json_response = {"processes": [{"name": "staging", "processor": "Staging"}]}
    process_id = "staging"

    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes/{process_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # Check that the job information are returned if we specify a valid job identifier in input
    job_response = staging_client.get_process(process_id)
    
    assert job_response.status_code == status.HTTP_200_OK
    assert job_response.json() == json_response

    # Check that the right error status code is returned if trying to get an unexisting resource
    process_id = "process_that_doesnt_exist"
    not_found_response = {"detail": "Resource not found"}
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes/{process_id}",
        json=not_found_response,
        status=status.HTTP_404_NOT_FOUND,
    )

    job_response = staging_client.get_process(process_id)
    assert job_response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, data_fixture",
    [
        (CADIP, "cadip_data"),
        (AUXIP, "auxip_data"),
    ],
)
def test_staging_ok(station, data_fixture, request, dummy_href, staging_client):
    """
    Nominal cases for staging
    """
    data_to_stage = request.getfixturevalue(data_fixture)
    process_id = "staging"
    # Nominal case - stage a FeatureCollection
    json_response = {
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
            {
            "href": "string",
            "rel": "service",
            "type": "application/json",
            "hreflang": "en",
            "title": "string"
            }
        ]
    }
    
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_400_BAD_REQUEST,
    )
    staging_status, staging_response = staging_client.run_staging(data_to_stage, OUTPUT_COLLECTION)
    assert staging_status == status.HTTP_200_OK
    assert staging_response == json_response["status"]["started"]

    # Nominal case - stage a Feature
    staging_status, staging_response = staging_client.run_staging(
        data_to_stage["features"][0],
        OUTPUT_COLLECTION,
    )
    assert staging_status == status.HTTP_200_OK
    assert staging_response == json_response["status"]["started"]

    # Nominal case - check that the test pass if the input data is a json file with a valid format
    item_file_to_stage = osp.join(RESOURCES_FOLDER, "staging", f"{station.lower()}_data.json")
    staging_status, staging_response = staging_client.run_staging(item_file_to_stage, OUTPUT_COLLECTION)
    assert staging_status == status.HTTP_200_OK
    assert staging_response == json_response["status"]["started"]

    # Nominal case - check that the test pass if the input data is a json string with a valid format
    staging_status, staging_response = staging_client.run_staging(json.dumps(data_to_stage), OUTPUT_COLLECTION)
    assert staging_status == status.HTTP_200_OK
    assert staging_response == json_response["status"]["started"]


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
        staging_status, staging_response = staging_client.run_staging(  # pylint: disable=unused-variable
            {},
            OUTPUT_COLLECTION,
        )
    assert "Key 'type' is missing from the staging input data" in str(exc_info.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, data_fixture",
    [
        (CADIP, "cadip_data"),
        (AUXIP, "auxip_data"),
    ],
)
def test_staging_fails_wrong_data_format(station, data_fixture, dummy_href, staging_client, request):
    """
    Failing case where the  input data is a json file
    with an unvalid format - In this case check that a pydantic ValueError is raised
    """
    json_response = {"status": {"started": "e390e31c-b274-49d2-88c2-466cc4fe23c9"}}
    process_id = "staging"
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # Check that the test raises an exception if the input file has a wrong data format
    item_file_to_stage = osp.join(RESOURCES_FOLDER, "staging", f"wrong_{station.lower()}_data.json")
    with pytest.raises(ValueError) as exc_info:
        staging_status, staging_response = staging_client.run_staging(  # pylint: disable=unused-variable
            item_file_to_stage,
            OUTPUT_COLLECTION,
        )
    assert "bbox is required if geometry is not null" in str(exc_info.value)

    # Check that the test fails if we pass in input a json string which is not compliant with stac
    data_to_stage = request.getfixturevalue(data_fixture)
    data_to_stage["features"][0].pop("bbox")
    with pytest.raises(ValueError) as exc_info:
        staging_status, staging_response = staging_client.run_staging(json.dumps(data_to_stage), OUTPUT_COLLECTION)
    assert "bbox is required if geometry is not null" in str(exc_info.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "data_fixture",
    ["cadip_data", "auxip_data"],
)
def test_staging_fails_endpoint_send_error(data_fixture, request, dummy_href, staging_client):
    """
    Failing case where the staging endpoint fails and doesn't return a job identifier
    """
    data_to_stage = request.getfixturevalue(data_fixture)
    json_response: dict[Any, Any] = {}
    process_id = "staging"

    # Case of a timeout for the staging
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_408_REQUEST_TIMEOUT,
    )
    staging_status, staging_response = staging_client.run_staging(data_to_stage, OUTPUT_COLLECTION)
    assert staging_status == status.HTTP_408_REQUEST_TIMEOUT
    assert staging_response is None


@pytest.mark.unit
@responses.activate
def test_get_jobs(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get all running jobs
    """
    json_jobs_data = {
        "identifier": "afbec9b5-7e46-4251-8e71-ec38479dbb11",
        "created_at": "2024-12-16T13:27:44.787943",
        "detail": "Sending tasks to the dask cluster",
        "updated_at": "2024-12-16T13:27:44.900254",
        "status": "IN_PROGRESS",
        "progress": 0.0,
    }

    # Mock the response of the endpoint to get all jobs
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs",
        json=json_jobs_data,
        status=status.HTTP_200_OK,
    )
    jobs_response = staging_client.get_jobs()

    assert jobs_response.status_code == status.HTTP_200_OK
    assert jobs_response.json() == json_jobs_data


@pytest.mark.unit
@responses.activate
def test_get_job(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    job_id = "afbec9b5-7e46-4251-8e71-ec38479dbb11"
    json_response = {
        "created_at": "2024-12-16T13:27:44.787943",
        "detail": "Finished",
        "identifier": "afbec9b5-7e46-4251-8e71-ec38479dbb11",
        "progress": 100.0,
        "status": "FINISHED",
        "updated_at": "2024-12-16T13:27:46.640572",
    }

    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}",
        json=json_response,
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    # Check that the job information are returned if we specify a valid job identifier in input
    job_response = staging_client.get_job_info(job_id)
    assert job_response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert job_response.json() == json_response

    # Check that an exception is raised if we don't specify a valid job identifier
    job_response = staging_client.get_job_info(job_id)
    assert job_response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR

    # Check that the right download status is sent back
    json_response["status"] = EDownloadStatus.IN_PROGRESS
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    job_response = staging_client.get_job_info(job_id)
    assert job_response.status_code == status.HTTP_200_OK
    assert job_response.json()["status"] == EDownloadStatus.IN_PROGRESS


@pytest.mark.unit
@responses.activate
def test_delete_job(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    job_id = "0474d453-3306-48e2-ab32-ac00bafb3115"
    json_response = {"message": f"Job {job_id} deleted successfully"}

    responses.add(
        method=responses.DELETE,
        url=f"{dummy_href}/jobs/{job_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # Check that the job information are returned if we specify a valid job identifier in input
    job_response = staging_client.delete_job(job_id)
    assert job_response.status_code == status.HTTP_200_OK
    assert job_response.json() == json_response

    # Check that we obtain the right error status_code when wanting to delete an unexisting job
    responses.add(
        method=responses.DELETE,
        url=f"{dummy_href}/jobs/{job_id}",
        json=None,
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    job_response = staging_client.delete_job(job_id)
    assert job_response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR


@pytest.mark.unit
@responses.activate
def test_get_job_results(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    job_id = "0474d453-3306-48e2-ab32-ac00bafb3115"
    json_response = "FINISHED"

    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}/results",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # Check that the job results are returned if we specify a valid job identifier in input
    job_response = staging_client.get_job_results(job_id)
    assert job_response.status_code == status.HTTP_200_OK
    assert job_response.json() == json_response

    # Check that we obtain the right error status_code when wanting to get results from unexisting job
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}/results",
        json=None,
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    job_response = staging_client.get_job_results(job_id)
    assert job_response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
