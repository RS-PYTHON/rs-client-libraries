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

from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_workflows.flow_utils import FlowEnv, FlowEnvSerialized


@flow(name="auxip-search")
async def search(
    env: FlowEnvSerialized,
    payload_file: str,
    cadip_data: ItemCollection,
    error_if_empty: bool = False,
) -> ItemCollection | None:
    """
    Search Auxip products.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        payload_file: S3 bucket location of the DPR payload file template.
        cadip_data: Results of the Cadip search
        error_if_empty: Raise a ValueError if the results are empty.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "cadip-search"):

        # Search products
        logger.info("Start Cadip search")
        found = flow_env.rs_client.get_cadip_client().search(
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


@task
def extract_module_and_processing_unit(payload_file: str):
    """Extract module and processing unit from the payload file."""
    logger = get_run_logger()

    with open(os.path.join(THIS_DIR, "l0", "config", payload_file)) as file:
        payload = yaml.safe_load(file)

    workflow = payload.get("workflow", [])
    for step in workflow:
        if "name" not in step:
            continue
        module = step.get("module")
        processing_unit = step.get("processing_unit")
        if not module:
            logger.error(
                f"Missing 'module' in processor payload configuration: {step['name']}",
            )
            return None, None
        if not processing_unit:
            logger.error(
                f"Missing 'processing_unit' in processor payload configuration: {step['name']}",
            )
            return None, None
        logger.info(
            f"For {step['name']} found module: {module} and processing_unit: {processing_unit}",
        )
        return module, processing_unit

    logger.error(
        f"No processor defined in the workflow of payload file {payload_file}.",
    )
    return None, None


###########################
# Call the flows as tasks #
###########################


@task
async def search_task(*args, **kwargs):
    """See: search"""
    return search.fn(*args, **kwargs)
