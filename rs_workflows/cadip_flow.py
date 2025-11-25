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

"""Cadip flow implementation"""

from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_client.stac.cadip_client import CadipClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


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
        logger.info(f"Cadip search found {len(found)} results: {found}")
        return found


###########################
# Call the flows as tasks #
###########################


@task(name="Cadip search")
async def search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)
