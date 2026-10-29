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

"""
Processing of a single product, on demand from a catalog product, or systematically from a product event: helpers of
the on-demand and systematic flows of a processing, such as the Sentinel-3 OLCI flows.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, Field

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, WorkflowType
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dpr import (
    ProcessingId,
    ProcessorSettings,
    load_processor_settings,
)
from rs_workflows.utils.prefect import get_logger


class FlowInputItem(BaseModel):
    """STAC item of the product to process, read from the catalog."""

    item_id: str = Field(description="STAC item identifier of the product.")
    collection_name: str = Field(description="Collection of the product.")


# Processing of a product, from the settings of the processing, the STAC item identifier, collection and properties of
# the product, and the workflow type. Returns the STAC items of the generated products.
ProductProcessing = Callable[
    [FlowEnvArgs, ProcessorSettings, str, str, dict[str, Any], WorkflowType],
    Awaitable[list[dict[str, Any]]],
]


async def process_catalog_product(
    env_args: FlowEnvArgs,
    input_item: FlowInputItem,
    force_systematic: bool,
    processing_id: ProcessingId,
    process_product: ProductProcessing,
) -> list[dict[str, Any]]:
    """
    Process on demand a product read from the catalog.

    Args:
        env_args: Flow environment arguments.
        input_item: STAC item identifier and collection of the product.
        force_systematic: Run the processing as a systematic one, so that the automations start the next processing
            of the chain from the events of the generated products.
        processing_id: Identifier of the processing, to read its default settings.
        process_product: Processing of the product.

    Returns:
        The STAC items of the generated products.
    """
    settings = await load_processor_settings(processing_id)
    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, "process-catalog-product"):  # NOSONAR
        item = await get_single_catalog_item(flow_env, input_item.item_id, [input_item.collection_name])
        if item is None:
            raise RuntimeError(
                f"❌ STAC item '{input_item.item_id}' not found in collection '{input_item.collection_name}'.",
            )
        return await process_product(
            env_args,
            settings,
            input_item.item_id,
            input_item.collection_name,
            item.properties,
            WorkflowType.of_run(force_systematic),
        )


async def process_event_product(
    payload: dict[str, Any],
    processing_id: ProcessingId,
    process_product: ProductProcessing,
) -> list[dict[str, Any]]:
    """
    Process systematically the product of a `product.<product type>.generated` event.

    Args:
        payload: Payload of the event, with the owner and the STAC item of the product.
        processing_id: Identifier of the processing, to read its default settings.
        process_product: Processing of the product.

    Returns:
        The STAC items of the generated products.
    """
    get_logger().info(f"Flow started from event with payload {payload}.")
    settings = await load_processor_settings(processing_id)
    env_args = FlowEnvArgs(owner_id=payload["owner_id"])
    product = payload["product"]
    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, "process-event-product"):  # NOSONAR
        return await process_product(
            env_args,
            settings,
            product["id"],
            product["collection"],
            product["properties"],
            WorkflowType.SYSTEMATIC,
        )
