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

"""Cadip flow implementation"""

from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_client.stac.cadip_client import CadipClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.staging_flow import staging_task


@flow(name="Cadip search")
async def search(
    env: FlowEnvArgs,
    cadip_collection_identifier: str,
    session_identifier: str,
    error_if_empty: bool = False,
) -> ItemCollection:
    """
    Search Cadip sessions.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        cadip_collection_identifier: CADIP collection identifier (to know the station)
        session_identifier: Session identifier
        error_if_empty: Raise a ValueError if the results are empty.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "cadip-search"):

        logger.info("Start Cadip search")
        cadip_client: CadipClient = flow_env.rs_client.get_cadip_client()
        found = cadip_client.search(
            method="GET",
            ids=[session_identifier],
            collections=[cadip_collection_identifier],
        )
        if (not found) and error_if_empty:
            raise ValueError(
                f"No Cadip session found for id={session_identifier!r} collection={cadip_collection_identifier!r}",
            )
        logger.info(
            f"Cadip search found {len(found)} results for id={session_identifier!r} "
            f"collection={cadip_collection_identifier!r}",
        )
        return found


@flow(name="On-demand Cadip staging")
async def on_demand_cadip_staging(
    env: FlowEnvArgs,
    cadip_collection_identifier: str,
    session_identifier: str,
    catalog_collection_identifier: str,
    staging_retries: int = 3,
    staging_retry_delay: int = 60,
):
    """
    Flow to retrieve a session, stage it and add the STAC item into the catalog.

    Args:
        env: Prefect flow environment
        cadip_collection_identifier: CADIP collection identifier that contains the mission and station
            (e.g. s1_ins for Sentinel-1 sessions from the Inuvik station)
        session_identifier: Session identifier
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged
    """

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "on-demand-cadip-staging"):

        # Search Cadip sessions
        cadip_items = search_task.with_options(
            retries=3,
            retry_delay_seconds=60,
        ).submit(
            flow_env.serialize(),
            cadip_collection_identifier,
            session_identifier,
            error_if_empty=True,
        )

        # Stage Cadip items.
        staged = staging_task.with_options(
            retries=staging_retries,
            retry_delay_seconds=staging_retry_delay,
        ).submit(
            flow_env.serialize(),
            cadip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        staged.result()  # type: ignore[unused-coroutine]


###########################
# Call the flows as tasks #
###########################


@task(name="Cadip search")
async def search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)
