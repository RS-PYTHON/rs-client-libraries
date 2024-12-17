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
import pprint
from typing import Any

from stac_pydantic.api import Item, ItemCollection

from rs_client.rs_client import TIMEOUT, RsClient

pp = pprint.PrettyPrinter(indent=2, width=80, sort_dicts=False, compact=True)

if os.getenv("RSPY_LOCAL_MODE") == "1":
    RSPY_HOST_STAGING = os.getenv("RSPY_HOST_STAGING")
else:
    RSPY_HOST_STAGING = "http://rs-server-staging:8000"


class StagingClient(RsClient):
    """
    Class to handle the staging process in rs-client-libraries

    This class provides python methods to call the different endpoints of the rs-server-staging method

    Remark: this class don't inherits from the owslib.ogcapi.processes.Processes class because the latter
    doesn't provide wrapping for all endpoints defined in rs-server-staging (it only provides the  /processes
    and /processes/{processId}/execution endpoints + it doesn't allow to manage apikey_header parameter which
    is passed as an extra argument)
    """

    def __init__(self, rs_server_href: str | None, rs_server_api_key: str | None, owner_id: str | None, logger: Any):
        """
        Initialize the StagingClient parameters
        """
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)
        # Define logger
        self.logger = logger
        self.resource = "staging"
        self.job_id: str | None = None

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

    @property
    def href_search(self) -> str:
        """href for search"""
        return ""

    @property
    def href_status(self) -> str:
        """href for status"""
        return ""

    @property
    def href_processes(self) -> str:
        """
        Url to get processes
        """
        return f"{self.href_staging}/processes"

    @property
    def href_resources(self) -> str:
        """
        Url for the staging process
        """
        return f"{self.href_staging}/processes/{self.resource}"

    @property
    def href_execute_process(self) -> str:
        """
        Url to execute a specific process
        """
        return f"{self.href_staging}/processes/{self.resource}/execution"

    @property
    def href_jobs(self) -> str:
        """
        Url to get running jobs
        """
        return f"{self.href_staging}/jobs"

    @property
    def href_job_status(self) -> str:
        """
        Url to get status of a specific job
        """
        return f"{self.href_staging}/jobs/{self.job_id}"

    @property
    def href_job_result(self) -> str:
        """
        Url to get the results of a specific job
        """
        return f"{self.href_staging}/jobs/{self.job_id}/results"

    ############################
    # Call RS-Server endpoints #
    ############################

    def run_staging(  # pylint: disable=too-many-locals
        self,
        stac_input: dict[Any, Any] | str,
        out_coll_name: str,
    ):
        """Method to start the staging process from rs-client

        Args:
            stac_input (dict | str): input dictionary
            out_coll_name (_type_): _description_

        Return:
            job_id (str): identifier of the current job
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

        # Check that the type
        if "type" not in stac_input_dict:
            raise KeyError("Staging input data has missing key 'type'")

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
            "outputs": {
                "result": {
                    "title": "Output STAC items",
                    "id": "some_output_id",
                    "description": "The staged STAC ItemCollection",
                    "schema": "false",
                    "minOccurs": 1,
                    "maxOccurs": 1,
                },
            },
        }

        try:
            post_response = self.http_session.post(
                url=self.href_execute_process,
                json=staging_body,
                timeout=TIMEOUT,
                **self.apikey_headers,
            )
            self.logger.info(f"POST response vaut: {post_response}")

            # Monitor the running job
            resp = json.loads(post_response.content)
            pprint.PrettyPrinter(indent=4).pprint(resp)

            self.job_id = resp["status"]["started"]
            print(f"\nJob ID = {self.job_id}\n")

        except KeyError as e:
            self.logger.exception(f"Could not launch the staging - response doesn't have the right format: {e}")
            return post_response.status_code, None

        return post_response.status_code, resp

    def get_jobs(self):
        """Method to get running jobs"""
        return self.http_session.get(
            url=self.href_jobs,
            **self.apikey_headers,
            timeout=TIMEOUT,
        )

    def get_job_status(self, job_id: str):  # pylint: disable=too-many-locals
        """Method to get a specific job response"""
        self.job_id = job_id

        try:
            job_response = self.http_session.get(
                url=self.href_job_status,
                **self.apikey_headers,
                timeout=TIMEOUT,
            )

            if job_response.status_code != 200:
                self.logger.error(
                    f"Staging reponse status code: {job_response.status_code} - "
                    f"The following input job doesn't exist: {self.job_id}",
                )
            return job_response

        except Exception:
            self.logger.error(f"Input job identifier doesn't have the right format {self.job_id}")
            raise
