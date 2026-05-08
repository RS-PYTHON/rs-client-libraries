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
from pystac import ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import ConversionIn, FlowEnv, RetryConfig
from rs_workflows.staging_flow import staging_task
from rs_workflows.utils.utils import (
    asset_unzip_decompress_task,
    get_archived_item_indexes,
)


@flow
async def on_demand_conversion(
    conversion_input: ConversionIn,
    retry_config: RetryConfig = RetryConfig(),
):
    """Docstring"""
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

        legacy_product = staging_task.with_options(
            retries=retry_config.staging_retries,
            retry_delay_seconds=retry_config.staging_retry_delay,
        ).submit(
            flow_env.serialize(),
            stac_input=conversion_input.stac_input,
            catalog_collection_identifier=conversion_input.generated_product_to_collection_identifier.collection_name,
            asset_names=selected_assets,
            poll_interval=10,
        )
        staging_results = legacy_product.result()  # type: ignore[unused-coroutine]

        for job_name, job_result in staging_results.items():
            if job_result.get("status") != "successful":
                raise RuntimeError(
                    f"Staging job {job_name!r} failed with status {job_result.get('status')!r}: "
                    f"{job_result.get('message')}",
                )
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        catalog_items = ItemCollection(
            catalog_client.get_items(
                collection_id=conversion_input.generated_product_to_collection_identifier.collection_name,
                items_ids=[item.id for item in ItemCollection.from_dict(conversion_input.stac_input).items],
            ),
        )
        logger.info(f"Retrieved catalog items after staging: {catalog_items.to_dict()}")
        # 2. Prepare assets for conversion (e.g. unzip if needed)
        try:
            for idx in get_archived_item_indexes(catalog_items):
                safe_zipped_item = catalog_items.items[idx]
                logger.info(f"Processing item {safe_zipped_item.id} for asset extraction...")
                safe_unzipped_item = asset_unzip_decompress_task.submit(safe_zipped_item)
                safe_item = safe_unzipped_item.result()
                catalog_client.update_item(safe_item)
        except Exception as err:
            raise RuntimeError(
                "Error while trying to update the item collection with the uncompressed/unzipped items. "
                "This error is likely due to a failure in the asset_unzip_decompress_task. "
                "Check previous logs for more details.",
            ) from err
        logger.info(f"Asset preparation completed, proceeding with conversion... {safe_item.to_dict()}")
        logger.info("Staging task completed, proceeding with conversion...")
        # 3. compute the output product type from the product type mapping
        # 4. compute the output bucket from the provided generated_product_to_collection_identifier mapping
        # 5. convert to zarr
        # 6. Read .zattrs to get stac item
        # 7. upload to S3
        # 8. post / put to catalog
        # 9. cleanup (legacy files, staging area)
        logger.info("On-demand conversion flow completed successfully.")
