# Copyright 2023-2026 Airbus
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

"""sentinel 1 Level-0 processing."""

import json

from prefect import flow, get_run_logger, task
from pystac import Item, ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@flow(name="process a sentinel-1 sessions")
async def s1l0_processing(
    session: str,
    collection: str = "s01-cadip-session",
    owner_identifier: str = "copernicus",
    verbose: bool = False,
):
    logger = get_run_logger()

    # Check S1 session name format
    if not session.startswith("S1"):
        logger.error("Bad sentinel-1 session name.")
        raise ValueError(f"Invalid session name : {session} does not start with 'S1'")
    if len(session) < 4 or session[3] != "_":
        logger.error("Bad sentinel-1 session name.")
        raise ValueError(f"The 4th character of '{session}' is not '_'")
    logger.info("Sentinel-1 session name is correct. ")

    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    with flow_env.start_span(__name__, "cadip-search"):
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()

        # Try to retrieve the session on the collection
        logger.info("Search session on the rs-catalog.")
        item_collection = catalog_client.search(method="POST", collections=[collection], ids=[session])
        count = len(item_collection.items)
        if count == 1:
            print(f"The session '{session}' has been found on the collection '{collection}'.")
        else:
            print(f"The session '{session}' has NOT been found on the collection '{collection}'.")
            print("Try to stage it from all S1 stations.")
        # print(json.dumps(item_collection.to_dict(), indent=2))
