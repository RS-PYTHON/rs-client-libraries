import pytest

import requests
import responses
from rs_common.logging import Logging
from rs_client.rs_client import RsClient
import getpass
from starlette.status import HTTP_200_OK, HTTP_403_FORBIDDEN
import os
import os.path as osp
from pathlib import Path
import json
from fastapi import status

RESOURCES_FOLDER = Path(osp.realpath(osp.dirname(__file__))) / "resources"
AUXIP = "AUXIP"
CADIP = "CADIP"

@pytest.fixture
def cadip_feature(name="cadip_feature"):
    cadip_feature_json = osp.join(RESOURCES_FOLDER, "cadip_catalog.json")
    with open(cadip_feature_json, encoding="utf-8") as file:
        return json.loads(file.read()) 

@pytest.fixture
def auxip_feature(name="auxip_feature"):
    auxip_feature_json = osp.join(RESOURCES_FOLDER, "adgs_catalog.json")
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
def test_staging_ok(station, href, collection_id, fixture_name, request):
    """
    Test the rs-client staging function - nominal case
    """
    href_staging = os.environ["RSPY_HOST_STAGING"] = "http://127.0.0.1:8004"
    resource = "staging"
    apikey = None
    output_collection = "my_test_collection" 
    owner_id=getpass.getuser()
    
    generic_client = RsClient(
        rs_server_href = None,
        rs_server_api_key=apikey,
        owner_id=owner_id,
        logger=None,
    )
    
    # Step 1 - Mock CADIP/AUXIP search response by loading a json file containing some sessions
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
    
    #Step 2 - Mock the staging response
    json_response = {'status': {'started': 'e390e31c-b274-49d2-88c2-466cc4fe23c9'}}
    responses.add(
        method=responses.POST,
        url=f"{href_staging}/processes/{resource}/execution",
        json=json_response,
        status=200,
    )
    
    staging_client = generic_client.get_staging_client()  
    staging_status, staging_response = staging_client.run_staging(search_response_json, output_collection)
    
    assert staging_status == HTTP_200_OK
    assert staging_response == json_response


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, href, collection_id, fixture_name",
    [
        (CADIP, "http://127.0.0.1:8002", "cadip_sentinel1", "cadip_feature"),
        (AUXIP, "http://127.0.0.1:8001", "adgs", "auxip_feature")
    ],
)
def test_staging_nok(station, href, collection_id, fixture_name, request):
    """
    Test the rs-client staging function - failing case
    """
    href_staging = os.environ["RSPY_HOST_STAGING"] = "http://127.0.0.1:8004"
    resource = "staging"
    apikey = None
    output_collection = "my_test_collection" 
    owner_id=getpass.getuser()
    
    generic_client = RsClient(
        rs_server_href = None,
        rs_server_api_key=apikey,
        owner_id=owner_id,
        logger=None,
    )
    
    # Step 1 - Mock CADIP/AUXIP search response by loading a json file containing some sessions
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
    
    #Step 2 - Mock the staging response
    json_response = {'status': {'started': 'e390e31c-b274-49d2-88c2-466cc4fe23c9'}}
    responses.add(
        method=responses.POST,
        url=f"{href_staging}/processes/{resource}/execution",
        json=json_response,
        status=200,
    )
    
    staging_client = generic_client.get_staging_client()  

    with pytest.raises(Exception) as e_info:
        staging_status, staging_response = staging_client.run_staging({}, output_collection)
    
    assert staging_status == HTTP_200_OK
    assert staging_response == json_response


@pytest.mark.unit
@responses.activate
def test_valid_staging_status():
    pass

@pytest.mark.unit
@responses.activate
def test_valid_staging_status():
    pass