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

from rs_client.ogcapi.staging_client import StagingClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@flow(name="Staging")
async def staging(
    env: FlowEnvArgs,
    stac_input: str | ItemCollection | dict,  # warning: dict as last choice for prefect ui
    catalog_collection_identifier: str,
    timeout: int = 120,
    poll_interval: int = 2,
):
    """
    Stage STAC items.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        stac_input (dict | str): it can be:<br>
            - A Python dictionary corresponding to a Feature or a FeatureCollection (that can be for example
                the output of a search for Cadip or Auxip sessions)<br>
            - A json string corresponding to a Feature or a FeatureCollection<br>
            - A string corresponding to a path to a json file containing a Feature or a FeatureCollection<br>
            - A single link that returns a STAC ItemCollection: this link should be an url to search an ItemCollection
        catalog_collection_identifier: Catalog collection identifier where items are staged
        timeout: Job completion timeout in seconds
        poll_interval: When to check again for job completion in seconds
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "staging"):

        staging_client: StagingClient = flow_env.rs_client.get_staging_client()

        # Convert pystac object into dict
        if isinstance(stac_input, ItemCollection):
            stac_input = stac_input.to_dict()

        # Trigger the staging and wait for jobs to finish
        job_status = staging_client.run_staging(stac_input, catalog_collection_identifier)
        staging_client.wait_for_jobs(job_status, logger, timeout, poll_interval)


###########################
# Call the flows as tasks #
###########################


@task(name="Auxip staging")
async def staging_task_auxip(*args, **kwargs):
    """See: staging"""
    return await staging.fn(*args, **kwargs)


@task(name="Cadip staging")
async def staging_task_cadip(*args, **kwargs):
    """See: staging"""
    return await staging.fn(*args, **kwargs)
