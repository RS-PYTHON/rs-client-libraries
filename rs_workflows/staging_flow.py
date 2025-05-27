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

"""Staging flow implementation"""

from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_workflows.flow_utils import FlowEnv, FlowEnv_


@flow(name="Staging")
async def staging(
    env: FlowEnv_,
    items: ItemCollection,
    catalog_collection_identifier: str,
    timeout: int = 120,
    poll_interval: int = 2,
) -> ItemCollection | None:
    """
    Stage STAC items.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        items: STAC items to stage, resulting from the Auxip or Cadip search.
        catalog_collection_identifier: Catalog collection identifier where items are staged
        timeout: Job completion timeout in seconds
        poll_interval: When to check again for job completion in seconds
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "staging"):

        # Trigger staging
        staging_client = flow_env.rs_client.get_staging_client()
        job_status = staging_client.run_staging(
            items.to_dict(),
            catalog_collection_identifier,
        )

        # Wait for the job to finish
        staging_client.wait_for_job(
            job_status,
            logger,
            "Staging",
            timeout,
            poll_interval,
        )


###########################
# Call the flows as tasks #
###########################


@task(name="Staging")
async def staging_task(*args, **kwargs):
    """See: staging"""
    return await staging.fn(*args, **kwargs)
