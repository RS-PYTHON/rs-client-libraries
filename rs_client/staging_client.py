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

"""Lauch staging with rs-clienclient-libraries"""

import getpass
import json
import os
import pprint
import sys
import time
from datetime import datetime
from typing import Any

import boto3
import botocore
import requests
from pystac import Collection, Extent, SpatialExtent, TemporalExtent

from rs_client.rs_client import TIMEOUT, RsClient

from rs_common.config import ECadipStation
from rs_common.logging import Logging
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

    def __init__(self, rs_server_href, rs_server_api_key, owner_id, logger):
        """
        Initialize the  owslib.ogcapi.processes.Processes object with provided parameters.
        """
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)
        # Define logger
        self.logger = logger
        self.resource = "staging"    
        self.job_id = None
    
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
    def href_processes(self) -> str:
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return f"{self.href_staging}/processes"
    
    @property
    def href_resources(self) -> str:
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return f"{self.href_staging}/processes/{self.resource}"
 
    @property
    def href_execute_process(self) -> str:
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return f"{self.href_staging}/processes/{self.resource}/execution"

    @property
    def href_jobs(self) -> str:
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return f"{self.href_staging}/jobs"

    @property
    def href_job_status(self) -> str:
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return f"{self.href_staging}/jobs/{self.job_id}"
    
    @property
    def href_job_result(self) -> str:
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return f"{self.href_staging}/jobs/{self.job_id}/results"

    ############################
    # Call RS-Server endpoints #
    ############################

    def run_staging(  # pylint: disable=too-many-locals
        self,
        stac_input: dict[Any, Any],
        out_coll_name: str,
    ): 
        """Method to start the staging process from rs-client

        Args:
            api_key_header (dict): api key to use in cluster mode
            stac_input (_type_): input information for the data to stage: either a Feature or a FeatureCollection
            stac_output_coll (_type_): _description_
        
        Return:
            job_id (str): identifier of the current job
        """
        staging_body = {
            "version": "0.2.0",
            "id": "staging",
            "title": {
                "en": "Staging"
            },
            "description": {
                "en": "A process that takes an external STAC ItemCollection, asynchronously download its assets into the RS catalog bucket and creates the corresponding STAC items in the RS catalog."
            },
            "jobControlOptions": [
                "async-execute"
            ],
            "keywords": [
                "stac",
                "staging"
            ],
            "links": [
                {
                    "type": "text/html",
                    "rel": "about",
                    "title": "documentation",
                    "href": "https://home.rs-python.eu/rs-documentation/rs-server/docs/doc/users/functionalities/#staging",
                    "hreflang": "en-US"
                }
            ],
            "inputs": {
                "collection": {
                    "title": "Target collection",
                    "description": "The target collection identifier in the RS catalog",
                    "id": out_coll_name,
                    "schema": {
                        "type": "string"
                    },
                    "minOccurs": 1,
                    "maxOccurs": 1
                },
                "items": stac_input,
                "provider": "cadip"
            },
            "outputs": {
                "result": {
                    "title": "Output STAC items",
                    "id": "some_output_id",
                    "description": "The staged STAC ItemCollection",
                    "schema": "false",
                    "minOccurs": 1,
                    "maxOccurs": 1
                }
            }
        }
        self.logger.info(f"href execute process vaut: {self.href_execute_process}")
        post_response = self.http_session.post(
                url=self.href_execute_process,
                json=staging_body,
                timeout=TIMEOUT,
                **self.apikey_headers,
            )
        # Monitor the running job
        resp = json.loads(post_response.content)
        self.logger.info(f"Response vaut: {resp}")
        
        pprint.PrettyPrinter(indent=4).pprint(resp)

        self.job_id = resp["status"]["started"]
        print(f"\nJob ID = {self.job_id}\n")
        
        return resp
    
    def get_jobs(self):
        """Method to get running jobs"""
        return self.http_session.get(
                url=self.href_jobs,
                **self.apikey_headers,
                timeout=TIMEOUT,
            )
    
    def get_job_status(  # pylint: disable=too-many-locals
        self,
        job_id: str
    ):
        """Method to get a specific job response"""
        self.job_id = job_id
        return self.http_session.get(
                url=self.href_job_status,
                **self.apikey_headers,
                timeout=TIMEOUT,
            )