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

"""CadipFlow implementation"""

from prefect import flow, get_run_logger
from pystac import ItemCollection

from rs_workflows.flow_utils import FlowEnv, FlowEnvSerialized


@flow(name="cadip-search-stage")
async def search_and_stage(
    extra_args: FlowEnvSerialized,
    cadip_collection_identifier: str,
    session_identifier: str,
) -> ItemCollection | None:
    """
    Search and stage Cadip products.

    Args:
        extra_args: Prefect flow environment (at least the owner_id is required)
        cadip_collection_identifier: CADIP collection identifier (to know the station)
        session_identifier: Session identifier
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(extra_args)
    with flow_env.start_span(__name__, "cadip-search-stage"):

        # Search products
        logger.info("Start Cadip search")
        found = flow_env.rs_client.get_cadip_client().search(
            method="GET",
            ids=[session_identifier],
            collections=[cadip_collection_identifier],
        )
        logger.info(f"Cadip search found {len(found)} results: {found}")
        return found
