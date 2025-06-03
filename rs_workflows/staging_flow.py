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

from collections import defaultdict
from urllib.parse import urlparse

import pystac
from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@flow(name="Staging")
async def staging(
    env: FlowEnvArgs,
    items: ItemCollection,
    catalog_collection_identifier: str,
    timeout: int = 120,
    poll_interval: int = 2,
):
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

        staging_client = flow_env.rs_client.get_staging_client()

        # Order the items by station url domain
        domains: dict[str, list[pystac.Item]] = defaultdict(list)
        for item in items:
            assets = list(item.assets.values())
            if assets:
                domain = urlparse(assets[0].href).hostname or ""
                domains[domain].append(item)

        # Trigger staging for each domain items
        all_job_status: dict[str, dict] = {}
        for domain, domain_items in domains.items():
            as_dict = ItemCollection(domain_items).to_dict()
            all_job_status[domain] = staging_client.run_staging(as_dict, catalog_collection_identifier)

        # Wait for jobs to finish
        for domain, job_status in all_job_status.items():
            staging_client.wait_for_job(
                job_status,
                logger,
                f"Staging from {domain!r}",
                timeout,
                poll_interval,
            )


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
