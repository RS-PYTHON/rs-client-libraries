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

"""Prefect flows and tasks for on-demand processing"""

from prefect import flow, get_run_logger
from prefect.client import get_client
from prefect.deployments.flow_runs import run_deployment

from rs_workflows import cadip_flow
from rs_workflows.flow_utils import FlowEnv, FlowEnvSerialized


@flow
async def on_demand_processing(
    env: FlowEnvSerialized,
    cadip_collection_identifier: str,
    session_identifier: str,
    payload_file: str,
    catalog_collection_identifier: str,
):
    """
    Prefect flow for on-demand processing.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        cadip_collection_identifier: CADIP collection identifier (to know the station)
        session_identifier: Session identifier
        payload_file: S3 bucket location of the DPR payload file template.
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged

    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "on-demand-processing"):

        cadip_data = await cadip_flow.search_task.submit(
            env=flow_env.serialize(),
            cadip_collection_identifier=cadip_collection_identifier,
            session_identifier=session_identifier,
            error_if_empty=True,
        ).result()

        bp = 0
