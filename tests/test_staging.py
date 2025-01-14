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

import requests
from rs_client.staging_client import StagingValidationException

RESOURCES_FOLDER = Path(osp.realpath(osp.dirname(__file__))) / "resources"
AUXIP = "AUXIP"
CADIP = "CADIP"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"

OWNER_ID = getpass.getuser()
OUTPUT_COLLECTION = "my_test_collection"


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


@pytest.fixture(name="auxip_data")
def get_auxip_data():
    """
    Return auxip FeatureCollection as a dictionary
    """
    auxip_data_json = osp.join(RESOURCES_FOLDER, "staging", "auxip_data.json")
    with open(auxip_data_json, encoding="utf-8") as file:
        return json.loads(file.read())

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
            {
            "href": "string",
            "rel": "service",
            "type": "application/json",
            "hreflang": "en",
            "title": "string"
            }
        ]
    }


@pytest.fixture(name="processes_sample")
def get_processes_sample():
    return {
        "processes": [
            {
            "title": "string",
            "description": "string",
            "keywords": [
                "string"
            ],
            "metadata": [
                {
                "title": "string",
                "role": "string",
                "href": "string"
                }
            ],
            "additionalParameters": {
                "title": "string",
                "role": "string",
                "href": "string",
                "parameters": [
                {
                    "name": "string",
                    "value": [
                    "string"
                    ]
                }
                ]
            },
            "id": "string",
            "version": "string",
            "jobControlOptions": [
                "sync-execute"
            ],
            "outputTransmission": [
                "value"
            ],
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
        ],
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

# -------------------------- Test for staging endpoints --------------------------

@pytest.mark.unit
@responses.activate
def test_get_processes(staging_client, dummy_href, processes_sample):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    json_response = processes_sample
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    # Check that the job information are returned if we specify a valid job identifier in input
    processes_resp = staging_client.get_processes()
    assert processes_resp == json_response
    
    # Check that we get a validation error if the server sends a response with an unvalid format 
    # (without the "links" required attribute)
    json_response.pop("links")
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        process_resp = staging_client.get_processes()
    assert "\'links\' is a required property" in str(exc_info.value)

@pytest.mark.unit
@responses.activate
def test_get_process(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """  
    process_id = "staging"  
    json_response = {
        "id": "EchoProcess",
        "title": "Echo Process",
        "description": "This process accepts and number of input and simple echoes each input as an output.",
        "version": "1.0.0",
        "inputs": {
            "stringInput": {
                "title": "String Literal Input Example",
                "description": "This is an example of a STRING literal input.",
                "minOccurs": 1,
                "schema": {
                    "exclusiveMaximum": False,
                    "exclusiveMinimum": False,
                    "minLength": 0,
                    "minItems": 0,
                    "uniqueItems": False,
                    "minProperties": 0,
                    "enum": ["Value1", "Value2", "Value3"],
                    "type": "string",
                    "additionalProperties": True,
                    "nullable": False,
                    "readOnly": False,
                    "writeOnly": False,
                    "deprecated": False,
                },
            }
        },
        "outputs": {
            "stringOutput": {
                "schema": {
                    "exclusiveMaximum": False,
                    "exclusiveMinimum": False,
                    "minLength": 0,
                    "minItems": 0,
                    "uniqueItems": False,
                    "minProperties": 0,
                    "enum": ["Value1", "Value2", "Value3"],
                    "type": "string",
                    "additionalProperties": True,
                    "nullable": False,
                    "readOnly": False,
                    "writeOnly": False,
                    "deprecated": False,
                }
            }
        },
    }

    # Check that the process information are returned if we specify a valid job identifier in input
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes/{process_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    process_resp = staging_client.get_process(process_id)
    assert process_resp is not None

    # Check that the right error status code is returned if trying to get an unexisting resource
    process_id = "process_that_doesnt_exist"
    not_found_response = {
        "type": "string",
        "title": "string",
        "status": 0,
        "detail": "string",
        "instance": "string"
    }
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes/{process_id}",
        json=not_found_response,
        status=status.HTTP_404_NOT_FOUND,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        process_resp = staging_client.get_process(process_id)
    assert "Unknown response http status: 404" in str(exc_info.value)
    
    # Check that we get a validation error if the server sends a response with an unvalid format 
    # (e.g. we add a wrong key in the expected data)
    json_response = {
        "id": "EchoProcess",
        "title": "Echo Process",
        "description": "This process accepts and number of input and simple echoes each input as an output.",
        "version": "1.0.0",
        "inputs": {
            "stringInput": {
                "schema": {
                    "wrong_key": False,
                },
            }
        },
    }
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes/{process_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        process_resp = staging_client.get_process(process_id)
    assert "{\'wrong_key\': False} is not valid under any of the given schemas" in str(exc_info.value)

@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, data_fixture",
    [
        (CADIP, "cadip_data"),
        (AUXIP, "auxip_data"),
    ],
)
def test_staging_ok(station, data_fixture, request, dummy_href, staging_client, staging_response_sample):
    """
    Nominal cases for staging
    """
    data_to_stage = request.getfixturevalue(data_fixture)
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
        staging_resp = staging_client.run_staging(  # pylint: disable=unused-variable
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
def test_staging_fails_wrong_data_format(station, data_fixture, dummy_href, staging_client, request, staging_response_sample):
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
    # Check that the test raises an exception if the input file has a wrong data format
    item_file_to_stage = osp.join(RESOURCES_FOLDER, "staging", f"wrong_{station.lower()}_data.json")
    with pytest.raises(ValueError) as exc_info:
        staging_resp = staging_client.run_staging(  # pylint: disable=unused-variable
            item_file_to_stage,
            OUTPUT_COLLECTION,
        )
    assert "bbox is required if geometry is not null" in str(exc_info.value)

    # Check that we get an exception if we pass in input a json string which is not compliant with stac
    data_to_stage = request.getfixturevalue(data_fixture)
    data_to_stage["features"][0].pop("bbox")
    with pytest.raises(ValueError) as exc_info:
         staging_resp = staging_client.run_staging(json.dumps(data_to_stage), OUTPUT_COLLECTION)
    assert "bbox is required if geometry is not null" in str(exc_info.value)


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
    json_response: dict[Any, Any] = {}
    process_id = "staging"

    # Case of a timeout for the staging
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_500_INTERNAL_SERVER_ERROR
    )
    with pytest.raises(StagingValidationException) as exc_info:
         staging_resp = staging_client.run_staging(data_to_stage, OUTPUT_COLLECTION)
    assert "Unknown response http status: 500" in str(exc_info.value)


@pytest.mark.unit
@responses.activate
def test_get_jobs(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get all running jobs
    """
    json_response = {
        "jobs": [
            {
            "processID": "string",
            "type": "process",
            "jobID": "string",
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
        ],
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
    
    # Check that the jobs information are sent if the endpoints returns a valid response
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    jobs_resp = staging_client.get_jobs()
    assert jobs_resp is not None
    
    # Check that an exception is raised if the endpoints returns a status error code
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs",
        json=json_response,
        status=status.HTTP_404_NOT_FOUND,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        jobs_resp = staging_client.get_jobs()
    assert "Unknown response http status: 404" in str(exc_info.value)
    
    # Check that an exception is raised if the endpoints returns an unvalid response
    # e.g. we remove the mandatory attribute "jobID"
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs",
        json=json_response["jobs"][0].pop("jobID"),
        status=status.HTTP_200_OK,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        jobs_resp = staging_client.get_jobs()
    assert "Failed to cast value to object type" in str(exc_info.value)


@pytest.mark.unit
@responses.activate
def test_get_job(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    job_id = "afbec9b5-7e46-4251-8e71-ec38479dbb11"
    json_response = {
        "processID": "string",
        "type": "process",
        "jobID": "string",
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
    # Check that the job information are returned if we specify a valid job identifier in input
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    job_resp = staging_client.get_job_info(job_id)
    assert job_resp is not None

    # Check that an exception is raised if we don't specify a valid job identifier
    job_id = "0000000"
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}",
        json=json_response,
        status=status.HTTP_404_NOT_FOUND,
    )
    with pytest.raises(StagingValidationException) as exc_info:
         job_response = staging_client.get_job_info(job_id)
    assert "Unknown response http status: 404" in str(exc_info.value)

    # Check that the right download status is sent back
    json_response["status"] = "running"
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    job_resp = staging_client.get_job_info(job_id)
    assert job_resp["status"] == "running"
    
    # Check that an exception is raised if the endpoints returns an unvalid response
    # e.g. we remove the mandatory attribute "jobID"
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs",
        json=json_response.pop("jobID"),
        status=status.HTTP_200_OK,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        jobs_resp = staging_client.get_jobs()
    assert "Failed to cast value to object type" in str(exc_info.value)
    

@pytest.mark.unit
@responses.activate
def test_delete_job(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    job_id = "0474d453-3306-48e2-ab32-ac00bafb3115"
    json_response = {
        "processID": "string",
        "type": "process",
        "jobID": "string",
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
    # Check that the job information are returned if we specify a valid job identifier in input
    responses.add(
        method=responses.DELETE,
        url=f"{dummy_href}/jobs/{job_id}",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    job_resp = staging_client.delete_job(job_id)
    assert job_resp is not None

    # Check that we obtain the right error status_code when wanting to delete a job with an identifier that doesn't exist
    responses.add(
        method=responses.DELETE,
        url=f"{dummy_href}/jobs/{job_id}",
        json={},
        status=status.HTTP_404_NOT_FOUND,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        job_resp = staging_client.delete_job(job_id)
    assert "Unknown response http status: 404" in str(exc_info.value)
    
    # Check that an exception is raised if the endpoints returns an unvalid response
    # e.g. we remove the mandatory attribute "jobID"
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs",
        json=json_response.pop("jobID"),
        status=status.HTTP_200_OK,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        jobs_resp = staging_client.get_jobs()
    assert "Failed to cast value to object type" in str(exc_info.value)

@pytest.mark.unit
@responses.activate
def test_get_job_results(staging_client, dummy_href):
    """
    Test to check the behaviour of the function to get the status of a specific job
    """
    job_id = "0474d453-3306-48e2-ab32-ac00bafb3115"
    json_response = {
        "property1": "string",
        "property2": "string"
    }

    # Check that the job results are returned if we specify a valid job identifier in input
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}/results",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    job_result_resp = staging_client.get_job_results(job_id)
    assert job_result_resp is not None

    # Check that we obtain the right error status_code when wanting to get results from unexisting job
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/jobs/{job_id}/results",
        json=None,
        status=status.HTTP_404_NOT_FOUND,
    )
    with pytest.raises(StagingValidationException) as exc_info:
        job_result_resp = staging_client.get_job_results(job_id)
    assert "Unknown response http status: 404" in str(exc_info.value)
    
    
# -------------------------- Test for methods used in the staging process --------------------------

@pytest.mark.unit
@responses.activate
def test_validate_and_unmarshal_request(staging_client, dummy_href, staging_response_sample):
    """
    Test to check the behaviour of the method to validate endpoints responses
    """
    process_id = "staging"
    request_body = {
        "inputs": {
            "property1": "string",
            "property2": "string"
        },
        "outputs": {
            "property1": {
            "format": {
                "mediaType": "string",
                "encoding": "string",
                "schema": "string"
            },
            "transmissionMode": "value"
            },
            "property2": {
            "format": {
                "mediaType": "string",
                "encoding": "string",
                "schema": "string"
            },
            "transmissionMode": "value"
            }
        },
        "response": "raw",
        "subscriber": {
            "successUri": "http://example.com",
            "inProgressUri": "http://example.com",
            "failedUri": "http://example.com"
        }
    }
    request_body = {
        "data": "aaa"
    }

    json_response = staging_response_sample
    # Nominal case - the json request body is valid
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/processes/{process_id}/execution",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    response = requests.post(
            f"{dummy_href}/processes/{process_id}/execution",
            json= request_body,
        )
    result = staging_client.validate_and_unmarshal_request(response.request)
    
    # Check that we obtain an exception if the json request body is not valid
    

@pytest.mark.unit
@responses.activate
def test_validate_and_unmarshal_response(staging_client, dummy_href, processes_sample):
    """
    Test to check the behaviour of the method to validate endpoints responses
    """
    
    # Case 1: We send a valid response from the /processes endpoint and we check 
    # in that case that the response data are well returned
    json_response = processes_sample
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    response = requests.get(url=f"{dummy_href}/processes")
    process_resp = staging_client.validate_and_unmarshal_response(response)
    assert process_resp == json_response
    
    # Case 2: Let's now modify the content of the mocked response so that it becomes
    # invalid: here we remove the "links" field of the /processes response that is 
    # required in the associated schema processList.yaml
    # In this case we expect an exception to be raised by the 
    # validate_and_unmarshal_response() method displaying the corresonding error obtained
    # during validation
    
    # Generate invalid response
    json_response = processes_sample.copy()
    json_response.pop("links")
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    response = requests.get(url=f"{dummy_href}/processes")
    with pytest.raises(StagingValidationException) as exc_info:
        process_resp = staging_client.validate_and_unmarshal_response(response)  
    assert "\'links\' is a required property" in str(exc_info.value)

    
    # Case 3: Let's now modify the status of the mocked response to an error status
    # status.HTTP_500_INTERNAL_SERVER_ERROR. In this case we expect an exception to be 
    # raised by the validate_and_unmarshal_response() method displaying the corresonding 
    # status code error
    json_response = processes_sample
    responses.add(
        method=responses.GET,
        url=f"{dummy_href}/processes",
        json=json_response,
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    response = requests.get(url=f"{dummy_href}/processes")
    with pytest.raises(StagingValidationException) as exc_info:
        process_resp = staging_client.validate_and_unmarshal_response(response)  
    assert "Unknown response http status: 500" in str(exc_info.value)