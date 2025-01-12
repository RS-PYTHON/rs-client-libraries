import requests
import responses
from openapi_core import Spec
from openapi_core import validate_request
from openapi_core import validate_response
from openapi_core.contrib.requests import RequestsOpenAPIRequest, RequestsOpenAPIResponse
from starlette import status
from openapi_core import OpenAPI

YAML_PATH = "/home/mcolinde/Documents/REFERENCE_SYSTEM/rs-client-libraries/rs_client/staging_templates/yaml/staging_api_schema.yaml"



def get_partner_spec():
    ###return Spec.from_file_path(str(self.get_project_root()) + "/docs/main.yaml")
    return Spec.from_file_path(YAML_PATH)
    
def validate_response_swagger(response):
    request = RequestsOpenAPIRequest(response.request)
    response = RequestsOpenAPIResponse(response)
    validate_response(response=response, spec=get_partner_spec(), request=request)

@responses.activate
def test_unmarshall_response():
    
    openapi = OpenAPI.from_file_path(YAML_PATH)
    json_response =  json_response = {
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
        url=f"http://127.0.0.1:8004/processes",
        json=json_response,
        status=status.HTTP_200_OK,
    )
    session = requests.Session()
    response = session.get("http://127.0.0.1:8004/processes")
    
    request = RequestsOpenAPIRequest(response.request)
    response = RequestsOpenAPIResponse(response)
    
    # raises error if request is invalid
    result = openapi.unmarshal_response(request, response)
    return result

#test_validate_get_processes()
result = test_unmarshall_response()

print(result)