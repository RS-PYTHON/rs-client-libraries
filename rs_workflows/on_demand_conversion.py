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

"""Prefect flows and tasks for on-demand SAFE -zarr conversion."""

from prefect import flow, get_run_logger
from rs_workflows.flow_utils import FlowEnv, ConversionIn, RetryConfig
from rs_workflows.staging_flow import staging_task


@flow
async def on_demand_conversion(
    conversion_input: ConversionIn,
    retry_config: RetryConfig = RetryConfig(),
):
    logger = get_run_logger()
    logger.info(f"Starting on-demand conversion flow with input: {conversion_input}")
    flow_env = FlowEnv(conversion_input.env)
    with flow_env.start_span(__name__, "legacy-conversion"):
        # 1. stage
        logger.info("Staging task submitted, waiting for completion...")
        selected_assets = None
        if isinstance(conversion_input.stac_input, dict):
            item = conversion_input.stac_input.get("features", [conversion_input.stac_input])[0]
            if "product" in item.get("assets", {}):
                selected_assets = {"product"}

        legacy_product = staging_task.submit(
            flow_env.serialize(),
            stac_input=conversion_input.stac_input,
            catalog_collection_identifier=conversion_input.generated_product_to_collection_identifier.collection_name,
            asset_names=selected_assets,
            poll_interval=10,
        )
        logger.info("Staging task completed, proceeding with conversion...")
        # 2. unzip if needed
        # 3. compute the output product type from the product type mapping
        # 4. compute the output bucket from the provided generated_product_to_collection_identifier mapping
        # 5. convert to zarr
        # 6. Read .zattrs to get stac item
        # 7. upload to S3
        # 8. post / put to catalog
        # 9. cleanup (legacy files, staging area)
        logger.info("On-demand conversion flow completed successfully.")
