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

"""Run the staging workflow."""

import argparse
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime

# RSPY_APIKEY="RSPY_APIKEY"
import boto3
import botocore
import requests

from rs_client.rs_client import RsClient
from rs_common.config import DATETIME_FORMAT
from rs_workflows.staging import (
    CATALOG_REQUEST_TIMEOUT,
    PrefectFlowConfig,
    create_collection_name,
    staging_flow,
)

# NOTE: for local mode, use the followings exports:
# export RSPY_HOST_ADGS=http://127.0.0.1:8001
# export RSPY_HOST_CADIP=http://127.0.0.1:8002
# export RSPY_HOST_CATALOG=http://127.0.0.1:8003
# and set the argument --url to ""


s3_session = boto3.session.Session()
s3_client = s3_session.client(
    service_name="s3",
    aws_access_key_id=os.environ["S3_ACCESSKEY"],
    aws_secret_access_key=os.environ["S3_SECRETKEY"],
    endpoint_url=os.environ["S3_ENDPOINT"],
    region_name=os.environ["S3_REGION"],
)

BUCKETS = ["rs-cluster-temp", "rs-cluster-catalog"]  # bucket names under S3_ENDPOINT
BUCKET_DIR = "stations"
BUCKET_URL = f"s3://{BUCKETS[0]}/{BUCKET_DIR}"


@dataclass
class Collection:
    """A collection for test purpose."""

    user: str
    name: str

    @property
    def id_(self) -> str:
        """Returns the id."""
        return f"{self.user}_{self.name}"

    @property
    def properties(self):
        """Returns the properties."""
        return {
            "id": self.name,
            "type": "Collection",
            "links": [
                {
                    "rel": "items",
                    "type": "application/geo+json",
                    "href": f"http://localhost:8082/collections/{self.name}/items",
                },
                {"rel": "parent", "type": "application/json", "href": "http://localhost:8082/"},
                {"rel": "root", "type": "application/json", "href": "http://localhost:8082/"},
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"""http://localhost:8082/collections/{self.name}""",
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/publicdomain/",
                    "title": "public domain",
                },
            ],
            "extent": {
                "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
                "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
            },
            "license": "public-domain",
            "description": "Some description",
            "stac_version": "1.0.0",
            "owner": self.user,
        }


def create_collection(_rs_client, collection_name, _logger):
    """Create a collection in the catalog."""
    catalog_endpoint = _rs_client.href_catalog + "/catalog/collections"
    collection_type = Collection(_rs_client.owner_id, collection_name)
    _logger.info(f"Endpoint used to insert the item info  within the catalog: {catalog_endpoint}")
    # try:
    response = requests.post(
        catalog_endpoint,
        data=None,
        json=collection_type.properties,
        timeout=CATALOG_REQUEST_TIMEOUT,
        **_rs_client.apikey_headers,
    )
    # except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
    #    _logger.exception(f"Could not get the response from the session search endpoint: {e}")
    #    return
    # except urllib3.exceptions.IncompleteRead as e:
    #    _logger.exception(f"urllib exception: {e}")
    #    return
    _logger.info(f"response = {response}")


if __name__ == "__main__":
    # pylint: disable=pointless-string-statement
    """
    This is a demo which integrates the search and download from a CADIP server.
    It also checks the download status.
    """

    # If the bucket is already created, clear all files to start fresh for each demo.
    for b in BUCKETS:
        try:
            s3_client.create_bucket(Bucket=b)
        except botocore.exceptions.ClientError as e:
            print(f"Bucket {b} error: {e}")
    LOG_FOLDER = "./demo/"
    os.makedirs(LOG_FOLDER, exist_ok=True)
    log_formatter = logging.Formatter("[%(asctime)-20s] [%(name)-10s] [%(levelname)-6s] %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_formatter)
    log_filename = LOG_FOLDER + "s3_handler_" + time.strftime("%Y%m%d_%H%M%S") + ".log"
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_formatter)
    gen_logger = logging.getLogger("test_backend")
    gen_logger.setLevel(logging.DEBUG)
    gen_logger.handlers = []
    gen_logger.propagate = False
    gen_logger.addHandler(console_handler)
    gen_logger.addHandler(file_handler)
    logger = gen_logger

    parser = argparse.ArgumentParser(
        description="Starts the demo for sprint 1 phase",
    )
    parser.add_argument("-a", "--url", type=str, required=True, help="Url of the RS-Server endpoints")

    parser.add_argument(
        "-s",
        "--station",
        type=str,
        required=True,
        help="Station name (use CADIP or ADGS and the url accordingly)",
    )

    parser.add_argument("-u", "--user", type=str, required=True, help="User name")

    parser.add_argument("-b", "--start-date", type=str, required=True, help="Start date used for time interval search")

    parser.add_argument("-e", "--stop-date", type=str, required=True, help="Stop date used for time interval search")

    parser.add_argument("-m", "--mission", type=str, required=True, help="Mission name")

    parser.add_argument(
        "-t",
        "--max-tasks",
        type=int,
        required=False,
        help="Maximum number of prefect tasks. Default 1 (the prefect flow will not be started)",
        default=1,
    )

    parser.add_argument(
        "-p",
        "--location",
        type=str,
        required=False,
        help="Location where the files are saved",
        default=None,
    )

    parser.add_argument(
        "-o",
        "--s3-storage",
        type=str,
        required=False,
        help="S3 path on the bucket where the files will be pushed through s3 protocol",
        default="",
    )

    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        required=False,
        help="Limit for returning results",
        default=100,
    )

    parser.add_argument(
        "-k",
        "--apikey",
        type=str,
        required=False,
        help="The apikey to be used in endpoints calling",
        default=None,
    )

    args = parser.parse_args()

    # check if the RSPY_APIKEY env var is set
    if not args.apikey:
        args.apikey = os.environ.get("RSPY_APIKEY", None)

    generic_client = RsClient(args.url, args.apikey, args.user, logger=None)

    if args.station.upper() == "ADGS":
        rs_client = generic_client.get_auxip_client()
    else:
        # the platforms is needed for sessions id search endpoint
        # only, so let's use an empty list for time being
        try:
            rs_client = generic_client.get_cadip_client(args.station.upper())
        except RuntimeError as e:
            logger.exception(f"Could not get the cadip client. Error: {e}")
            sys.exit(-1)

    create_collection(rs_client.get_stac_client(), create_collection_name(args.mission, rs_client.station_name), logger)

    # catalog_endpoint = args.url_catalog.rstrip("/") + "/catalog/collections"
    # collection_type = Collection(args.user, "s1_aux")
    # logger.info(f"Endpoint used to insert the item info  within the catalog: {catalog_endpoint}")
    # response = requests.post(catalog_endpoint, data=None, json=collection_type.properties, **apikey_headers)
    # logger.info("response = {} ".format(response))
    # collection_type = Collection(args.user, "s1_chunk")
    # logger.info(f"Endpoint used to insert the item info  within the catalog: {catalog_endpoint}")
    # response = requests.post(catalog_endpoint, data=None, json=collection_type.properties, **apikey_headers)
    # logger.info("response = {} ".format(response))

    # By default, save files into a local temp directory
    if args.location:
        location = args.location
        temp_dir = None  # pylint: disable=invalid-name
    else:
        temp_dir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        location = temp_dir.name

    flowConfig = PrefectFlowConfig(
        rs_client,
        "s1",
        location,
        args.s3_storage,
        args.max_tasks,
        datetime.strptime(args.start_date, DATETIME_FORMAT),
        datetime.strptime(args.stop_date, DATETIME_FORMAT),
        None,
    )

    # Clean temp directory
    if temp_dir:
        temp_dir.cleanup()

    DOWNLOAD_FLOW_ID = staging_flow(flowConfig)
    logger.info("EXIT !")
    # mission = "s1"
    # catalog_data = json.loads(
    #     (
    #         requests.get(
    #             args.url_catalog.rstrip("/") + f"/catalog/collections/{args.user}:{mission}_aux/items?limit=100",
    #             **apikey_headers,
    #         ).content.decode()
    #     ),
    # )

    # for feature in catalog_data["features"]:
    #     print(
    #         requests.get(
    #             args.url_catalog
    #             + f"/catalog/collections/{args.user}:{mission}_aux/items/{feature['id']}/download/file",
    #             **apikey_headers,
    #         ).content,
    #     )
