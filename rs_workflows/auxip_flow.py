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

"""Auxip flow implementation"""

import json

from prefect import flow, get_run_logger, task
from pystac import Item, ItemCollection

from rs_client.stac.auxip_client import AuxipClient
from rs_workflows.catalog_flow import catalog_search_task
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.staging_flow import staging_task_auxip

###############
# Auxip flows #
###############


@flow(name="Auxip search")
async def search(
    env: FlowEnvArgs,
    auxip_cql2: dict,
    error_if_empty: bool = False,
) -> ItemCollection | None:
    """
    Search Auxip products.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        auxip_cql2: Auxip CQL2 filter read from the processor tasktable.
        error_if_empty: Raise a ValueError if the results are empty.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "auxip-search"):

        logger.info("Start Auxip search")
        auxip_client: AuxipClient = flow_env.rs_client.get_auxip_client()
        found = auxip_client.search(
            method="POST",
            stac_filter=auxip_cql2.get("filter"),
            max_items=auxip_cql2.get("limit"),
            sortby=auxip_cql2.get("sortby"),
        )
        if (not found) and error_if_empty:
            raise ValueError(
                f"No Auxip product found for CQL2 filter: {json.dumps(auxip_cql2, indent=2)}",
            )
        logger.info(f"Auxip search found {len(found)} results: {found}")
        return found


@flow(name="Auxip staging")
async def auxip_staging(
    env: FlowEnvArgs,
    stac_query: json,
    catalog_collection_identifier: str,
    timeout_seconds: int = -1,
):
    """
    Generic flow to retrieve a list of items matching the STAC CQL2 filter given, and to stage the ones
    that are not already in the catalog.

    Args:
        env (FlowEnvArgs): Prefect flow environment
        stac_query (json): CQL2 filter to select which files to stage
        catalog_collection_identifier (str): Catalog collection identifier where CADIP sessions and AUX data are staged
        timeout_seconds (int): Timeout value for the Auxip search task.
            Optional, if no value is given the process will run until it is completed
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "auxip-staging"):

        # TODO Add timeout
        # Search Auxip products
        auxip_items: ItemCollection = search_task.submit(
            flow_env.serialize(),
            auxip_cql2={"filter": stac_query},
            error_if_empty=False,
        )

        # Stop process if search task didn't return any item
        if len(auxip_items) == 0:
            logger.info("Nothing to stage: Auxip search with given filter returned empty result.")
            return

        # Search catalog items
        catalog_items: ItemCollection = catalog_search_task.submit(
            flow_env.serialize(),
            catalog_cql2={"filter": stac_query},
            error_if_empty=False,
        )

        # Compare results of Auxip search with results of Catalog search
        # to see what is missing in Catalog
        missing_items_list: list[Item] = []
        for item in auxip_items:
            if item not in catalog_items:
                missing_items_list.append(item)
        missing_auxip_items = ItemCollection(missing_items_list)
        logger.info(f"Number of items missing in the catalog to stage: {len(missing_auxip_items)}")

        # Stop process if all the Auxip items found are already in the catalog
        if len(missing_auxip_items) == 0:
            logger.info("Nothing to stage: all Auxip items found are already in the catalog.")
            return

        # Stage missing Auxip items
        staged = staging_task_auxip.submit(
            flow_env.serialize(),
            missing_auxip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        staged.result()  # type: ignore[unused-coroutine]


###########################
# Call the flows as tasks #
###########################


@task(name="Auxip search")
async def search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)
