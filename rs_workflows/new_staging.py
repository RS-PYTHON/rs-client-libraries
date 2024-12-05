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

from rs_client.rs_client import RsClient
from rs_common.config import ECadipStation
from rs_common.logging import Logging


RSPY_HOST_STAGING = os.getenv("RSPY_HOST_STAGING")

class RsStagingClient:
    """Class to handle the staging process in rs-client-libraries

    This class provides python methods to call the different endpoints of the rs-server-staging method

    Remark: this class don't inherits from the owslib.ogcapi.processes.Processes class because the latter
    doesn't provide wrapping for all endpoints defined in rs-server-staging (it only provides the  /processes
    and /processes/{processId}/execution endpoints + it doesn't allow to manage apikey_header parameter which
    is passed as an extra argument)
    """

    def __init__(self):
        """
        Initialize the  owslib.ogcapi.processes.Processes object with provided parameters.
        """
        # Define logger
        self.logger = Logging.default(__name__)

    def get_processes(self, timeout: int):
        """
        Call endpoint /processes - Returns list of all available processes from config
        """
        return requests.get(f"{RSPY_HOST_STAGING}/processes", timeout=timeout)  # pylint: disable=too-many-arguments

    def get_resource(self, resource: str, timeout: int):
        """
        Call endpoint /processes/{resource} - Should return info about a specific resource
        """
        return requests.get(
            f"{RSPY_HOST_STAGING}/processes/{resource}",
            timeout=timeout,
        )  # pylint: disable=too-many-arguments

    def execute_process(self, resource: str, timeout: int):
        """
        Call endpoint/processes/{resource}/execution - execute processing jobs
        Args:
            resource (str): name of the process to execute
        """
        return requests.get(f"{RSPY_HOST_STAGING}/processes/{resource}/execution", timeout=timeout)

    def get_job_status(self, job_id: int, timeout: int):
        """
        Call endpoint /jobs/{job_id} - get status of processing job
        Args:
            job_id (int): job identifier
        """
        return requests.get(f"{RSPY_HOST_STAGING}/jobs/{job_id}", timeout=timeout)

    def get_jobs(self, timeout: int):
        """
        Call endpoint /jobs to get the status of all jobs
        """
        return requests.get(f"{RSPY_HOST_STAGING}/jobs", timeout=timeout)

    def delete_job(self, job_id: int, timeout: int):
        """
        Call endpoint /jobs/{job_id} - get status of processing job
        Args:
            job_id (int): job identifier
        """
        return requests.delete(f"{RSPY_HOST_STAGING}/jobs/{job_id}", timeout=timeout)

    def get_specific_job_result(self, job_id):
        """
        Call endpoint /jobs/{job_id}/results - get result from a specific job
        Args:
            job_id (int): job identifier
        """

    def run_staging(  # pylint: disable=too-many-locals
        self,
        apikey_headers: dict[str, Any],
        stac_input: dict[Any, Any],
        out_coll_name: str,
        timeout: int,
    ):
        """Method to start the staging process from rs-client

        Args:
            api_key_header (dict): api key to use in cluster mode
            stac_input (_type_): input information for the data to stage: either a Feature or a FeatureCollection
            stac_output_coll (_type_): _description_
        """
        staging_body = {  # pylint: disable=line-too-long
            "version": "0.2.0",
            "id": "staging",
            "title": {"en": "Staging"},
            "description": {
                "en": "A process that takes an external STAC ItemCollection, "
                "asynchronously download its assets into the RS catalog bucket "
                "and creates the corresponding STAC items in the RS catalog.",
            },
            "jobControlOptions": ["async-execute"],
            "keywords": ["stac", "staging"],
            "links": [
                {
                    "type": "text/html",
                    "rel": "about",
                    "title": "documentation",
                    "href": "https://home.rs-python.eu/rs-documentation/"
                    "rs-server/docs/doc/users/functionalities/#staging",
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
                "items": stac_input,
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

        # ----- Step 3: Launch and monitor the staging process

        # Remark: this class don't inherits from the owslib.ogcapi.processes.Processes class because the latter
        # doesn't provide wrapping for all endpoints defined in rs-server-staging (it only provides the  /processes
        # and /processes/{processId}/execution endpoints + it doesn't allow to manage apikey_header parameter which
        # is passed as an extra argument)
        post_response = requests.post(
            f"{RSPY_HOST_STAGING}/processes/staging/execution",
            json=staging_body,
            **apikey_headers,
            timeout=timeout,
        )

        resp = json.loads(post_response.content)
        pprint.PrettyPrinter(indent=4).pprint(resp)

        job_id = resp["status"]["started"]
        print(f"\nJob ID = {job_id}\n")

        timeout = 120
        while timeout > 0:
            post_response = requests.get(
                f"{RSPY_HOST_STAGING}/jobs/{job_id}",
                **apikey_headers,
                timeout=timeout,
            )
            try:
                resp = json.loads(post_response.content)
                pprint.PrettyPrinter(indent=4).pprint(resp)
                print("\n")
                if resp["status"] == "FINISHED":
                    print("Job COMPLETED")
                    break

                if resp["status"] == "FAILED":
                    print("Job FAILED")
                    break
            except (json.JSONDecodeError,):
                continue
            time.sleep(2)
            timeout -= 2


if __name__ == "__main__":
    # ----------------- Input data -----------------
    TIMEOUT = 10
    CATALOG_BUCKET = "rs-cluster-catalog"
    APIKEY_HEADER = "x-api-key"
    COLLECTION_ID = "cadip_s1A"
    STAC_OUTPUT_COLL_NAME = "cadip_s1A_staged"
    APIKEY_VALUE = None  # "x-api-key" ### TODO: get apikey from frontend page
    APIKEY_HEADERS: dict = {"headers": {APIKEY_HEADER: APIKEY_VALUE}} if APIKEY_VALUE else {}
    user = os.getenv("RSPY_HOST_USER", default=getpass.getuser())
    local_mode = os.getenv("RSPY_LOCAL_MODE") == "1"
    rs_server_href = "" if local_mode else os.getenv("RSPY_WEBSITE")

    # Get client instances from the generic client
    generic_client = RsClient(
        rs_server_href,
        rs_server_api_key=APIKEY_VALUE,
        owner_id=user,
        logger=None,
    )
    stac_client = generic_client.get_stac_client()
    cadip_station = ECadipStation.CADIP  # you can also have: INS, MPS, MTI, NSG, SGS
    cadip_client = generic_client.get_cadip_client(cadip_station)
    logger = Logging.default(__name__)

    # ----- Step 1 - Create the output STAC collection if it doesn't alredy exists
    logger.info(f"Creating a new collection {STAC_OUTPUT_COLL_NAME} in the STAC catalog...")
    try:
        create_coll_response = stac_client.get_collection(STAC_OUTPUT_COLL_NAME)
        logger.info(
            f"Collection {STAC_OUTPUT_COLL_NAME} already exists -> staging process will use the existing one",
        )
    except: # pylint: disable=bare-except
        create_coll_response = stac_client.add_collection(
            Collection(
                id=STAC_OUTPUT_COLL_NAME,
                description=None,  # rs-client will provide a default description for us
                extent=Extent(
                    spatial=SpatialExtent(bboxes=[-180.0, -90.0, 180.0, 90.0]),
                    temporal=TemporalExtent(
                        [datetime(2000, 1, 1), datetime(2030, 1, 1)],
                    ),
                ),
            ),
        )
        logger.info(
            f"Resp status: {create_coll_response.status_code} | Message: {create_coll_response.reason}",
        )

    # ----- Step 2 - Create a bucket to store staged data
    PREFIX = "stream/"
    s3_session = boto3.session.Session()
    s3_client = s3_session.client(
        service_name="s3",
        aws_access_key_id=os.environ["S3_ACCESSKEY"],
        aws_secret_access_key=os.environ["S3_SECRETKEY"],
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
    )
    try:
        s3_client.head_bucket(Bucket=CATALOG_BUCKET)
        logger.info(f"The bucket {CATALOG_BUCKET} already exists")
    except botocore.client.ClientError as error:
        if int(error.response["Error"]["Code"]) == 404:
            try:
                s3_client.create_bucket(Bucket=CATALOG_BUCKET)
            except botocore.exceptions.ClientError as e:
                logger.info(f"Bucket CATALOG_BUCKET error: {e}")
                sys.exit(-1)
        else:
            logger.info("PANIC: Could not get bucket info. Exiting")
            sys.exit(-1)
    # Delete all existing objects from rs-server-catalog
    if PREFIX:
        response = s3_client.list_objects_v2(Bucket=CATALOG_BUCKET, Prefix=PREFIX)
        if response.get("Contents", None):
            for elem in response["Contents"]:
                logger.info(f"Deleting {elem['Key']}")
                s3_client.delete_object(Bucket=CATALOG_BUCKET, Key=elem["Key"])

    # Apply a request to get information about sessions that you want to stage
    session = requests.Session()
    search_result = session.get(f"{os.getenv('RSPY_HOST_CADIP')}/cadip/collections/{COLLECTION_ID}/items").json()

    # Create necessary clients to perform catalog search and staging opeation
    staging_client = RsStagingClient()
    # Launch staging process
    staging_client.run_staging(APIKEY_HEADERS, search_result, STAC_OUTPUT_COLL_NAME, TIMEOUT)
