# Copyright 2023-2026 Airbus, CS Group
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

import datetime

from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_client.stac.prip_client import PripClient
from rs_common.utils import create_valcover_filter
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, RetryConfig
from rs_workflows.staging_flow import staging_task


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


@flow(name="On-demand Prip staging")
async def on_demand_prip_staging(
    env: FlowEnvArgs,
    start_datetime: datetime.datetime | str,
    end_datetime: datetime.datetime | str,
    product_type: str,
    prip_collection: str,
    catalog_collection_identifier: str,
    retry_config: RetryConfig = RetryConfig(),  # type: ignore
):
    """
    Flow to retrieve Prip files with the given time interval defined by
    start_datetime and end_datetime, select only the type of files wanted,
    stage the files and add STAC items into the catalog.

    Args:
        env: Prefect flow environment
        start_datetime: Start datetime for the time interval used to filter the files
            (date or timestamp, e.g. "2025-08-07T11:51:12.509000Z")
        end_datetime: End datetime for the time interval used to filter the files
            (date or timestamp, e.g. "2025-08-10T14:00:00.509000Z")
        product_type: Prip product type wanted
        prip_collection: PRIP collection identifier (station)
        catalog_collection_identifier: Catalog collection identifier where PRIP data are staged
    """

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "on-demand-prip-staging"):

        # CQL2 filter: filter on product type and time interval
        cql2_filter = create_valcover_filter(start_datetime, end_datetime, product_type)

        # Search Prip products
        prip_items = search_task.with_options(
            retries=retry_config.staging_retries,
            retry_delay_seconds=retry_config.staging_retry_delay,
        ).submit(
            flow_env.serialize(),
            prip_cql2={"filter": cql2_filter},
            prip_collection=prip_collection,
            error_if_empty=False,
        )

        # Stage Prip items
        staged = staging_task.with_options(
            retries=retry_config.staging_retries,
            retry_delay_seconds=retry_config.staging_retry_delay,
        ).submit(
            flow_env.serialize(),
            prip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end (unwrap exceptions if any)
        staged.result()  # type: ignore[unused-coroutine]


###########################
# Call the flows as tasks #
###########################


@task(name="PRIP search")
async def search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)
