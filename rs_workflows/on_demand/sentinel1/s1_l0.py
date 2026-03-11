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
from rs_workflows.on_demand.stage_last_sessions import stage_session_common, cadip_session_search
from rs_workflows.utils.artifact_verbose import ReportManager
from rs_client.stac.cadip_client import CadipClient


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
    with flow_env.start_span(__name__, "sentinel1-level0"):
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()

        # Try to retrieve the session on the collection
        logger.info("Search session on the rs-catalog.")
        item_collection = catalog_client.search(method="POST", collections=[collection], ids=[session])
        if item_collection is not None:
            count = len(item_collection.items)
        else:
            count = 0
        if count == 1:
            print(f"The session '{session}' has been found on the rs-catalog collection '{collection}'.")
        else:
            print(f"The session '{session}' has NOT been found on the rs-catalog collection '{collection}'.")
            print("Try to stage it from all S1 stations.")
            item_col = cadip_session_search_by_name(flow_env, session)
            print(json.dumps(item_col.items[0].to_dict(), indent=2))


@task(name="Cadip session search by name")
async def cadip_session_search_by_name(env: FlowEnv, session: str) -> ItemCollection:
    """
    """
    logger = get_run_logger()

    # Initialize flow environment and telemetry span
    cadip_client: CadipClient = env.rs_client.get_cadip_client()

    # Log query for debugging
    logger.info("Start request on all S1 CADIP stations")

    # Execute search request
    found = cadip_client.search(
        method="GET",
        ids=[session],
        collections=["s1_ins", "s1_kse", "s1_mps", "s1_mti", "s1_nsg", "s1_sgs"],
        max_items=1,
        limit=1
    )

    return found
