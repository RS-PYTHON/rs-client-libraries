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

import requests

# openapi_core libraries used for endpoints validation
import openapi_core
from openapi_core import OpenAPI, Spec, validate_request, validate_response
from openapi_core.contrib.requests import (
    RequestsOpenAPIRequest,
    RequestsOpenAPIResponse,
)
from stac_pydantic.api import Item, ItemCollection

from rs_client.rs_client import TIMEOUT, RsClient

PATH_TO_YAML_OPENAPI = osp.join(
    osp.realpath(osp.dirname(__file__)),
    "staging_templates",
    "yaml",
    "staging_openapi_schema.yaml",
)


class StagingValidationException(Exception):
    """
    Exception raised when an error occurs during the validation
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

    def __init__(self, rs_server_href: str | None, rs_server_api_key: str | None, owner_id: str | None, logger: Any):
        """
        Initialize the StagingClient parameters.
        """
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)
        # Define logger
        self.logger = logger
        self.resource = "staging"

    @property
    def href_staging(self) -> str:
        """
        Return the RS-Server staging URL hostname.
        This URL can be overwritten using the RSPY_HOST_STAGING env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        if from_env := os.getenv("RSPY_HOST_STAGING", None):
            return from_env.rstrip("/")
        if not self.rs_server_href:
            raise RuntimeError("RS-Server URL is undefined")
        return self.rs_server_href.rstrip("/")

    def validate_and_unmarshal_request(self, request: requests.models.PreparedRequest):
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
        openapi_request: openapi_core.contrib.requests.RequestsOpenAPIRequest = RequestsOpenAPIRequest(request)

        # validate_request(request, spec=Spec.from_file_path(PATH_TO_YAML_OPENAPI))
        result = openapi.unmarshal_request(openapi_request)

        if result.errors:
            raise StagingValidationException(  # type: ignore
                f"Error validating the request of the enpoint " f"{openapi_request.path}: {str(result.errors[0])}", # type: ignore
            )
        if not result.body:
            raise StagingValidationException(
                f"Error validating the request of the enpoint "
                f"{openapi_request.path}: 'data' field of ResponseUnmarshalResult"
                f"object is empty",
            )
        return result.body

    def validate_and_unmarshal_response(self, response: requests.models.Response):
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
        result = openapi.unmarshal_response(openapi_request, openapi_response) # type: ignore
        if result.errors:
            raise StagingValidationException(  # type: ignore
                f"Error validating the response of the enpoint " f"{openapi_request.path}: {str(result.errors[0])}", # type: ignore
            )
        if not result.data:
            raise StagingValidationException(
                f"Error validating the response of the enpoint "
                f"{openapi_request.path}: 'data' field of ResponseUnmarshalResult"
                f"object is empty",
            )
        return result.data

    ############################
    # Call RS-Server endpoints #
    ############################

    def get_processes(self) -> requests.models.Response:
        """_summary_

        Returns:
            dict: dictionary containing the content of the response
        """
        response = self.http_session.get(
            url=f"{self.href_staging}/processes",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )

        return self.validate_and_unmarshal_response(response)

    def get_process(self, process_id: str) -> requests.models.Response:
        """
        Wrapper to get a specific process
        Args:
            process_id (str): name of the resource
        """
        response = self.http_session.get(
            url=f"{self.href_staging}/processes/{process_id}",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)

    def run_staging(  # pylint: disable=too-many-locals
        self,
        stac_input: dict[Any, Any] | str,
        out_coll_name: str,
    ) -> tuple[int, str | None]:
        """Method to start the staging process from rs-client - Call the endpoint /processes/staging/execution

        Args:
            stac_input (dict | str): input dictionary: the stac_input can have different format. It can be:
                - A Python dictionary corresponding to a Feature or a FeatureCollection (that can be for example
                  the output of a search for Cadip or Auxip sessions)
                - A json string corresponding to a Feature or a FeatureCollection
                - A string corresponding to a path to a json file containing a Feature or a FeatureCollection
            out_coll_name (): _description_

        Return:
            job_id (int, str): Returns the status code of the staging request + the identifier
            (or None if staging endpoint fails) of the running job
        """
        stac_input_dict = {}
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
                stac_input_dict = json.loads(stac_input)
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

        staging_body = {  # pylint: disable=line-too-long
            "version": "0.2.0",
            "id": "staging",
            "title": {"en": "Staging"},
            "description": {
                "en": "A process that takes an external STAC ItemCollection, asynchronously download"
                "its assets into the RS catalog bucket and creates the corresponding STAC items in the RS catalog.",
            },
            "jobControlOptions": ["async-execute"],
            "keywords": ["stac", "staging"],
            "links": [
                {
                    "type": "text/html",
                    "rel": "about",
                    "title": "documentation",
                    "href": "https://home.rs-python.eu/rs-documentation/rs-server"
                    "/docs/doc/users/functionalities/#staging",
                    "hreflang": "en-US",
                },
            ],
            "inputs": {
                "collection": {
                    "title": "Target collection",
                    "description": "The target collection identifier in the RS catalog",
                    "id": out_coll_name,
                    "schema": {"type": "string"},
                    "minOccurs": 1,
                    "maxOccurs": 1,
                },
                "items": stac_item_collection.model_dump(mode="json"),
                "provider": "cadip",
            },
            "outputs": {"featureCollectionOutput": {"transmissionMode": "value"}},
        }
        post_response = self.http_session.post(
            url=f"{self.href_staging}/processes/{self.resource}/execution",
            json=staging_body,
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        # return post_response.status_code, job_id
        return self.validate_and_unmarshal_response(post_response)

    def get_jobs(self) -> requests.models.Response:
        """Method to get running jobs"""
        response = self.http_session.get(
            url=f"{self.href_staging}/jobs",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_job_info(self, job_id: str) -> requests.models.Response:  # pylint: disable=too-many-locals
        """Method to get a specific job response"""
        job_response = self.http_session.get(
            url=f"{self.href_staging}/jobs/{job_id}",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(job_response)

    def delete_job(self, job_id: str) -> requests.models.Response:  # pylint: disable=too-many-locals
        """Method to get a specific job response"""
        response = self.http_session.delete(
            url=f"{self.href_staging}/jobs/{job_id}",
            **self.apikey_headers,
            timeout=TIMEOUT,
        )
        return self.validate_and_unmarshal_response(response)

    def get_job_results(self, job_id: str) -> requests.models.Response:
        """Wrapper to get the result of a specfific job

        Args:
            job_id (str): _description_
        """
        response = self.http_session.get(
            url=f"{self.href_staging}/jobs/{job_id}/results",
            timeout=TIMEOUT,
            **self.apikey_headers,
        )
        return self.validate_and_unmarshal_response(response)
