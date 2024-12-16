import pytest

import requests
import responses
from rs_common.logging import Logging
from rs_client.rs_client import RsClient
import getpass
from starlette.status import *
import os
import os.path as osp
from pathlib import Path
import json
from fastapi import status

RESOURCES_FOLDER = Path(osp.realpath(osp.dirname(__file__))) / "resources"
AUXIP = "AUXIP"
CADIP = "CADIP"
RESOURCE = "staging"
APIKEY = None
OWNER_ID=getpass.getuser()
OUTPUT_COLLECTION = "my_test_collection" 

@pytest.fixture(name="rs_server_href")
def rs_server_href():
    return None

@pytest.fixture(name="client")
def client(rs_server_href):
    return RsClient(
        rs_server_href = rs_server_href,
        rs_server_api_key=APIKEY,
        owner_id=OWNER_ID,
        logger=None,
    )

@pytest.fixture(name="href_staging")
def href_staging():
    href_staging = os.environ["RSPY_HOST_STAGING"] = "http://127.0.0.1:8004"
    return href_staging

@pytest.fixture(name="cadip_feature")
def cadip_feature():
    cadip_feature_json = osp.join(RESOURCES_FOLDER, "staging", "cadip_data.json")
    with open(cadip_feature_json, encoding="utf-8") as file:
        return json.loads(file.read()) 

@pytest.fixture(name="auxip_feature")
def auxip_feature():
    auxip_feature_json = osp.join(RESOURCES_FOLDER, "staging", "auxip_data.json")
    with open(auxip_feature_json, encoding="utf-8") as file:
        return json.loads(file.read()) 

@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, href, collection_id, fixture_name",
    [
        (CADIP, "http://127.0.0.1:8002", "cadip_sentinel1", "cadip_feature"),
        (AUXIP, "http://127.0.0.1:8001", "adgs", "auxip_feature")
    ],
)
def test_staging_ok(station, href, collection_id, fixture_name, request, href_staging, client):
    """
    Test the rs-client staging function - nominal case
    """
    
    # Mock CADIP/AUXIP search response by loading a json file containing some sessions
    feature = request.getfixturevalue(fixture_name)
    responses.add(
        method=responses.GET,
        url=f"{href}/{station.lower()}/collections/{collection_id}/items",
        json=feature,
        status=200
    )
    # Check response status
    search_response = requests.get(f"{href}/{station.lower()}/collections/{collection_id}/items")
    
    assert search_response.status_code == HTTP_200_OK
    search_response_json = search_response.json()
    # Check that we obtain the good number of elements
    assert len(search_response_json["features"]) == 2
    
    #Mock the staging response
    json_response = {'status': {'started': 'e390e31c-b274-49d2-88c2-466cc4fe23c9'}}
    responses.add(
        method=responses.POST,
        url=f"{href_staging}/processes/{RESOURCE}/execution",
        json=json_response,
        status=200,
    )
    
    staging_client = client.get_staging_client()  
    staging_status, staging_response = staging_client.run_staging(search_response_json, OUTPUT_COLLECTION)
    
    assert staging_status == HTTP_200_OK
    assert staging_response == json_response

    # Check that  the staging is also working when passing a Feature object instead of a FeatureCollection object
    staging_status, staging_response = staging_client.run_staging(search_response_json["features"][0], OUTPUT_COLLECTION)
    
    assert staging_status == HTTP_200_OK
    assert staging_response == json_response
    
@pytest.mark.unit
@responses.activate
def test_staging_fails_stage_empty_dict(href_staging, client):
    """
    Test the rs-client staging function - failing case where we use an empty dictionary in input of the staging
    """    
    # Case of an error returned by the staging endpoint
    responses.add(
        method=responses.POST,
        url=f"{href_staging}/processes/{RESOURCE}/execution",
        json={},
        status=422,
    )
    
    staging_client = client.get_staging_client()  

    with pytest.raises(KeyError) as exc_info:
        staging_status, staging_response = staging_client.run_staging({}, OUTPUT_COLLECTION)
    assert "Staging input data has missing key 'type'" in str(exc_info.value)

@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        AUXIP
    ],
)
def test_staging_fails_wrong_data_format(station, href_staging, client):
    """
    Test the rs-client staging function - failing case where the loaded input data have a wrong format
    """
    staging_client = client.get_staging_client()  
    json_response = {'status': {'started': 'e390e31c-b274-49d2-88c2-466cc4fe23c9'}}
    # Case of a timeout for the staging
    responses.add(
        method=responses.POST,
        url=f"{href_staging}/processes/{RESOURCE}/execution",
        json=json_response,
        status=200,
    )
    # Check that the test pass if the input file has a correct data format
    item_file_to_stage = osp.join(RESOURCES_FOLDER, "staging", f"{station.lower()}_data.json")
    staging_status, staging_response = staging_client.run_staging(item_file_to_stage, OUTPUT_COLLECTION)
    assert staging_status == HTTP_200_OK
    assert staging_response == json_response
    
    # Check that the test pass if the input file has a correct data format
    item_file_to_stage = osp.join(RESOURCES_FOLDER, "staging", f"wrong_{station.lower()}_data.json")
    with pytest.raises(ValueError) as exc_info:
        staging_status, staging_response = staging_client.run_staging(item_file_to_stage, OUTPUT_COLLECTION)
    assert "bbox is required if geometry is not null" in str(exc_info.value)

@pytest.mark.unit
@responses.activate
def test_get_jobs(client, rs_server_href, href_staging):
    json_jobs_data = {
        "identifier":"afbec9b5-7e46-4251-8e71-ec38479dbb11",
        "created_at":"2024-12-16T13:27:44.787943",
        "detail":"Sending tasks to the dask cluster",
        "updated_at":"2024-12-16T13:27:44.900254",
        "status":"IN_PROGRESS",
        "progress":0.0
    }
    
    staging_client = client.get_staging_client()  
    
    # Mock the response of the endpoint to get all jobs
    responses.add(
            method=responses.GET,
            url=f"{href_staging}/jobs",
            json=json_jobs_data,
            status=200,
        )
    jobs_response = staging_client.get_jobs()
    
    assert jobs_response.status_code == HTTP_200_OK
    assert jobs_response.json() == json_jobs_data

@pytest.mark.unit
@responses.activate
def test_get_job_status(client, href_staging):
    job_id = "afbec9b5-7e46-4251-8e71-ec38479dbb11"
    json_job = {   
        "created_at": "2024-12-16T13:27:44.787943",
        "detail": "Finished",
        "identifier": "afbec9b5-7e46-4251-8e71-ec38479dbb11",
        "progress": 100.0,
        "status": "FINISHED",
        "updated_at": "2024-12-16T13:27:46.640572"
    }
    
    staging_client = client.get_staging_client()  
    
    # Mock the response of the endpoint to get the status of a specific job
    responses.add(
            method=responses.GET,
            url=f"{href_staging}/jobs/{job_id}",
            json=json_job,
            status=200,
        )
    job_response = staging_client.get_job_status(job_id)
    assert job_response.status_code == HTTP_200_OK
    assert job_response.json() == json_job
    
    # Check that an exception is raised if we don't specify a valid job identifier
    with pytest.raises(requests.exceptions.ReadTimeout) as excinfo:
        job_response = staging_client.get_job_status("unexisting-job-1234")
    assert "The following input job doesn't exist" in str(excinfo.value.args[0])