# Copyright 2025 CS Group
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

"""Prip flow implementation"""
from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_client.stac.prip_client import PripClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@flow(name="Prip search")
async def search(
    env: FlowEnvArgs,
    prip_cql2: dict,
    prip_collection: str = "",
    error_if_empty: bool = False,
) -> ItemCollection | None:
    """
    Search Prip products.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        prip_cql2: PRIP CQL2 filter.
        prip_collection: PRIP ollection identifier (to know the station)
        error_if_empty: Raise a ValueError if the results are empty.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "prip-search"):

        logger.info("Start PRIP search")
        prip_client: PripClient = flow_env.rs_client.get_prip_client()
        found = prip_client.search(
            method="POST",
            stac_filter=prip_cql2.get("filter"),
            max_items=prip_cql2.get("limit", 10),
            sortby=prip_cql2.get("sortby", "-created"),
            collections=[prip_collection],
        )
        if (not found) and error_if_empty:
            raise ValueError("No PRIP products found")
        logger.info(f"PRIP search found {len(found)} results: {found}")
        return found


###########################
# Call the flows as tasks #
###########################


@task(name="PRIP search")
async def search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)
