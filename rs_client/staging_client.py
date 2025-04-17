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

"""Lauch staging with rs-client-libraries"""

import json
import os
import os.path as osp
from typing import Any
from urllib.parse import urlparse

# openapi_core libraries used for endpoints validation
import requests
from openapi_core import OpenAPI  # Spec, validate_request, validate_response
from openapi_core.contrib.requests import (
    RequestsOpenAPIRequest,
    RequestsOpenAPIResponse,
)
from requests import Response
from requests.models import PreparedRequest
from stac_pydantic.api import Item, ItemCollection

from rs_client.rs_client import TIMEOUT, RsClient
from rs_common.utils import get_href_service

# WARNING: this env variable is temporarily added until the response of rssrver-staging endpoints are corrected
# with a valid format according to ogc standard. In the meantime, we don't perform validation to make all staging
# notebooks pass. If this env variable if not specified (for example that is the case when we launch the pytest),
# we perform this validation by default

PATH_TO_YAML_OPENAPI = osp.join(
    osp.realpath(osp.dirname(__file__)),
    "../config",
    "staging_templates",
    "yaml",
    "staging_openapi_schema.yaml",
)
RESOURCE = "staging"


def is_url(path):
    """
    Function to check if a string is an url
    """
    parsed = urlparse(path)
    return bool(parsed.scheme and parsed.netloc)


class StagingValidationException(Exception):
    """
    Exception raised when an error occurs during the OGC validation
    of the staging endpoints
    """


class StagingClient(RsClient):
    """
    Class to handle the staging process in rs-client-libraries

    This class provides python methods to call the different endpoints of the rs-server-staging method.

    Remark: this class don't inherits from the owslib.ogcapi.processes.Processes class because the latter
    doesn't provide wrapping for all endpoints defined in rs-server-staging (it only provides the  /processes
    and /processes/{processId}/execution endpoints + it doesn't allow to manage apikey_header parameter which
    is passed as an extra argument).
    """

    @property
    def href_service(self) -> str:
        """
        Return the RS-Server staging URL hostname.
        This URL can be overwritten using the RSPY_HOST_STAGING env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_STAGING")

    def validate_and_unmarshal_request(self, request: PreparedRequest) -> Any:
        """Validate an endpoint request according to the ogc specifications

        Args:
            request (Request): endpoint request

        Returns:
            ResponseUnmarshalResult.data: data validated by the openapi_core
            unmarshal_response method
        """

        if not os.path.isfile(PATH_TO_YAML_OPENAPI):
            raise FileNotFoundError(f"The following file path was not found: {PATH_TO_YAML_OPENAPI}")

        openapi = OpenAPI.from_file_path(PATH_TO_YAML_OPENAPI)
        openapi_request = RequestsOpenAPIRequest(request)

        # validate_request(request, spec=Spec.from_file_path(PATH_TO_YAML_OPENAPI))
        result = openapi.unmarshal_request(openapi_request)

        if result.errors:
            raise StagingValidationException(
                f"Error validating the request of the enpoint "
                f"{openapi_request.path}: {str(result.errors[0])}",  # type: ignore
            )
        if not result.body:
            raise StagingValidationException(
                f"Error validating the request of the enpoint "
                f"{openapi_request.path}: 'data' field of ResponseUnmarshalResult"
                f"object is empty",
            )
        return result.body

    def validate_and_unmarshal_response(self, response: Response) -> Any:
        """
        Validate an endpoint response according to the ogc specifications
        (described as yaml schemas)

        Args:
            response (Response): endpoint response
        Returns:
            ResponseUnmarshalResult.data: data validated by the openapi_core
            unmarshal_response method
        """
        if not os.path.isfile(PATH_TO_YAML_OPENAPI):
            raise FileNotFoundError(f"The following file path was not found: {PATH_TO_YAML_OPENAPI}")

        openapi = OpenAPI.from_file_path(PATH_TO_YAML_OPENAPI)
        openapi_request = RequestsOpenAPIRequest(response.request)
        openapi_response = RequestsOpenAPIResponse(response)

        # Alternative method to validate the response
        # validate_response(response=response, spec= Spec.from_file_path(PATH_TO_YAML_OPENAPI), request=request)
        result = openapi.unmarshal_response(openapi_request, openapi_response)  # type: ignore
        if result.errors:
            raise StagingValidationException(  # type: ignore
                f"Error validating the response of the enpoint {openapi_request.path} - "
                f"Server response content: {response.json()} - "
                f"Validation error of the server response: {str(result.errors[0])}",  # type: ignore
            )
        if not result.data:
            raise StagingValidationException(
                f"Error validating the response of the enpoint "
                f"{openapi_request.path}: 'data' field of ResponseUnmarshalResult"
                f"object is empty",
            )
        return json.loads(response.content)

    ############################
    # Call RS-Server endpoints #
    ############################

    def get_processes(self) -> dict:
        """_summary_

        Returns:
            dict: dictionary containing the content of the response
        """
        response = self.http_session.get(
            url=f"{self.href_service}/processes",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)

    def get_process(self, process_id: str) -> dict:
        """
        Wrapper to get a specific process
        Args:
            process_id (str): name of the resource
        """
        response = self.http_session.get(
            url=f"{self.href_service}/processes/{process_id}",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)

    def run_staging(  # pylint: disable=too-many-locals
        self,
        stac_input: dict[Any, Any] | str,
        out_coll_name: str,
    ) -> dict:
        """Method to start the staging process from rs-client - Call the endpoint /processes/staging/execution

        Args:
            stac_input (dict | str): input dictionary: the stac_input can have different format. It can be:
                - A Python dictionary corresponding to a Feature or a FeatureCollection (that can be for example
                  the output of a search for Cadip or Auxip sessions)
                - A json string corresponding to a Feature or a FeatureCollection
                - A string corresponding to a path to a json file containing a Feature or a FeatureCollection
                - A single link that returns a STAC itemCollection: this link should be an url to search a
                  itemCollection, for example:
                  http://localhost:8002/cadip/search?ids=S1A_20231120061537234567&collections=cadip_sentinel1
        out_coll_name (str): name of the output collection
        Return:
            job_id (int, str): Returns the status code of the staging request + the identifier
            (or None if staging endpoint fails) of the running job
        """
        stac_input_dict = {}
        is_an_url = False

        # ----- Case 1: we only load a link (that refers to a STAC itemCollection) in the staging request body
        if isinstance(stac_input, str):
            try:
                is_an_url = is_url(stac_input)
            except ValueError:
                is_an_url = False
        if is_an_url:
            staging_body = {"inputs": {"collection": out_coll_name, "items": {"href": stac_input}}}

        # ----- Case 2: we directly load a STAC ItemCollection in the staging request body
        else:
            # If stac_input is a file, load this file to a dictionary
            if isinstance(stac_input, str):
                # If the input is a valid path to a json_file, load this file
                if os.path.exists(os.path.dirname(stac_input)) and stac_input.endswith(".json"):
                    # Read the yaml or json file
                    with open(stac_input, encoding="utf-8") as opened:
                        stac_file_to_dict = json.loads(opened.read())
                        stac_input_dict = stac_file_to_dict
                # If the input string is not a path, try to convert the content of the string to a json dictionary
                else:
                    try:
                        stac_input_dict = json.loads(stac_input)
                    except json.JSONDecodeError as e:
                        raise StagingValidationException(
                            f"""Invalid input format: {stac_input} - Input data must be either:
                                - A Python dictionary corresponding to a Feature or a FeatureCollection
                                (that can be for example the output of a search for Cadip or Auxip sessions)
                                - A json string corresponding to a Feature or a FeatureCollection
                                - A string corresponding to a path to a json file containing a Feature or a
                                FeatureCollection
                                - A single link that returns a STAC ItemCollection: this link should be an
                                url to search an ItemCollection
                            """.strip(),
                        ) from e
            else:
                stac_input_dict = stac_input

            if "type" not in stac_input_dict:
                raise KeyError("Key 'type' is missing from the staging input data")

            # Validate input data using Pydantic
            if stac_input_dict["type"] == "Feature":
                stac_item = Item(**stac_input_dict)
                stac_item_collection = ItemCollection(
                    **{
                        "type": "FeatureCollection",
                        "context": {"limit": 1000, "returned": 2},
                        "features": [stac_item],
                    },  # type: ignore
                )
            else:
                stac_item_collection = ItemCollection(**stac_input_dict)

            staging_body = {
                "inputs": {
                    "collection": out_coll_name,
                    "items": {"value": stac_item_collection.model_dump(mode="json")},
                },
            }

        # Check that the request containing the staging body is valid
        request = requests.Request(  # pylint: disable=W0612 # noqa: F841
            method="POST",  # Méthode HTTP, peut être 'POST', 'GET', etc.
            url=f"{self.href_service}/processes/{RESOURCE}/execution",  # L'URL de l'endpoint
            json=staging_body,  # Corps de la requête en JSON
        ).prepare()

        # Validate the body of the request that will be sent to the staging
        self.validate_and_unmarshal_request(request)

        response = self.http_session.post(
            url=f"{self.href_service}/processes/staging/execution",
            json=staging_body,
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_jobs(self) -> dict:
        """Method to get running jobs"""
        response = self.http_session.get(
            url=f"{self.href_service}/jobs",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_job_info(self, job_id: str) -> dict:  # pylint: disable=too-many-locals
        """Method to get a specific job response"""
        response = self.http_session.get(
            url=f"{self.href_service}/jobs/{job_id}",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def delete_job(self, job_id: str) -> dict:  # pylint: disable=too-many-locals
        """Method to get a specific job response"""
        response = self.http_session.delete(
            url=f"{self.href_service}/jobs/{job_id}",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_job_results(self, job_id: str) -> dict:
        """Wrapper to get the result of a specfific job

        Args:
            job_id (str): _description_
        """
        response = self.http_session.get(
            url=f"{self.href_service}/jobs/{job_id}/results",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)
