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

"""Helper task to interact with the rs-cadip."""

from rs_workflows.flow_utils import FlowEnv
from prefect import get_run_logger, task
import os
import json
from pystac import Item, ItemCollection
from rs_client.stac.catalog_client import CatalogClient
from rs_client.stac.cadip_client import CadipClient


@task(name="Search the cadip station that has got a session")
async def get_cadip_station(flow_env: FlowEnv, session: str, cadip_collections : list[str]) -> str|None:
    """ """
    logger = get_run_logger()
    result = None
    
    # Initialize flow environment and telemetry span
    cadip_client: CadipClient = flow_env.rs_client.get_cadip_client()

    # Log query for debugging
    logger.info(f"Search a Cadip station {', '.join(cadip_collections)} looking for the session'{session}'")

    # Execute search request
    item_col:ItemCollection = cadip_client.search(
        method="GET",
        ids=[session],
        collections=cadip_collections,
        max_items=1,
        limit=1,
    )

    cadip_station = ""
    if len(item_col) == 1:
        collection_links = [link for link in item_col[0].links if link.rel == "collection"]
        if collection_links:
            href = collection_links[0].href
            result = href.rstrip("/").split("/")[-1]
            logger.info(f"✔️ The session '{session}' is available at station {result}")
    if result is None:
        logger.info(f"❌ The session '{session}' can not be found on stations {', '.join(cadip_collections)}")

    return result