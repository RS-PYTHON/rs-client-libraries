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

"""common Level-0 processing."""

from datetime import datetime

from prefect import get_run_logger
from pystac import Item

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
)
from rs_workflows.on_demand.common.types import Level0FlowParams
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dpr import call_dpr_flow


async def process_l0_last_steps(
    mission: str,
    session: str,
    flow_params: Level0FlowParams,
    input_products: list[FlowInputProduct],
    verbose: bool,
):
    """
    Final processing steps that are common to all missions.
    Raises:
        ValueError: _description_
    """
    logger = get_run_logger()
    logger.info(f"Mode verbose is set to {verbose}")

    # Resolve parameters
    flow_params = flow_params or Level0FlowParams()
    p = await flow_params.resolve(mission)

    flow_env = FlowEnv(FlowEnvArgs(owner_id=p.owner_identifier))

    with flow_env.start_span(__name__, f"sentinel{mission}-level0-processing"):
        item_session: Item | None = await get_single_catalog_item(flow_env, session, [p.session_collection])

        if not item_session:
            logger.error("❌ The processing cannot be launched.")
            return

        # Satellite identifier
        satellite_identifier = f"sentinel-{mission}{session[2].lower()}"

        # Published date
        published = item_session.properties.get("published")
        if not isinstance(published, str):
            raise ValueError("Missing or invalid 'published' property in item_session")

        end_datetime = datetime.fromisoformat(published)
        start_datetime = end_datetime

        # Call DPR flow
        await call_dpr_flow(
            FlowEnvArgs(owner_id=p.owner_identifier),
            input_products=input_products,
            external_variables={
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "satellite_identifier": satellite_identifier,
            },
            dask_cluster_label=p.dask_cluster_label,
            processor_name=p.processor_name,
            processor_version=p.processor_version,
            pipeline=p.pipeline,
            unit=p.unit,
            priority=p.priority,
            processing_mode=p.processing_mode,
            workflow=p.workflow,
            generated_product_to_collection_identifier=p.generated_product_to_collection_identifier or [],
            auxiliary_product_to_collection_identifier=p.auxiliary_product_to_collection_identifier or [],
        )
