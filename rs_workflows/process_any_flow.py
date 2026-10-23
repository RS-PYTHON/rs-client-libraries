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
Generic processing flow `process-any`, that runs a processor with its settings read from a Prefect variable, and the
helpers that run it on demand from a catalog product, or systematically from a product event.
"""

import json
from collections.abc import Awaitable, Callable
from enum import Enum
from pathlib import Path
from typing import Any

import anyio
from jsonschema import ValidationError, validate
from prefect import flow
from prefect.variables import Variable
from pydantic import BaseModel, Field

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand.common.events import emit_product_generated_events
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dask import is_dask_cluster_running
from rs_workflows.utils.dpr import call_dpr_flow
from rs_workflows.utils.prefect import get_logger

DASK_CLUSTER_STAGING = "dask-staging"
DEFAULT_SETTINGS_SCHEMA = Path(__file__).parent.parent / "config" / "ProcessorDefaultSettings.schema.json"


class ProcessingId(str, Enum):
    """
    Processing Identifier : usefull to determine configuration file pattern.
    """

    S3_OLCI_L1 = "s3l1olci"
    S3_OLCI_L2 = "s3l2olci"


class FlowInputItem(BaseModel):
    """STAC item of the product to process, read from the catalog."""

    item_id: str = Field(description="STAC item identifier of the product.")
    collection_name: str = Field(description="Collection of the product.")


# Processing of a product, from its STAC item identifier, collection and properties, and the workflow type.
# Returns the STAC items of the generated products.
ProductProcessing = Callable[[FlowEnvArgs, str, str, dict[str, Any], WorkflowType], Awaitable[list[dict[str, Any]]]]


@flow(name="process-any")
async def process_any(
    env_args: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    processing_id: ProcessingId,
    workflow: WorkflowType = WorkflowType.ON_DEMAND,
) -> list[dict[str, Any]]:
    """
    # Process Any

    This flow reads part of parameters from a Prefect Variable before starting the
    process-generic flow.

    ---
    ## Workflow Steps

    1. *Load settings* — Reads the `{processing_id}-default-setting` Prefect
    Variable and validates it against the processor default settings JSON
    schema.
    2. *Check clusters* — Verifies that the target Dask cluster and the staging
    Dask cluster are ready.
    3. *Process* — Calls the DPR flow with the input products, external
    variables, and the resolved processor/pipeline/priority settings.
    4. *Collect results* — Retrieves the list of generated/published items.
    5. *Emit events* — Emits a `product.<product type>.generated` Prefect event per generated product, related to
    the workflow. When the workflow is systematic, the automations start the next processing of the chain.
    ---
    ## Parameters
    Parameter | Type | Default | Description |
    |---|---|---|---|
    | `env_args` | `FlowEnvArgs` | *required* | Flow environment arguments |
    | `input_products` | `list[FlowInputProduct]` | *required* | Input products to process |
    | `external_variables` | `dict[str, Any]` | *required* | External variables used to retrieved aux data |
    | `processing_id` | `ProcessingId` | *required* | Processor to run, see `{processing_id}-default-setting` |
    | `workflow` | `WorkflowType` | `WorkflowType.ON_DEMAND` | Type of workflow triggering this run |

    > *Note:* The default settings variable must match the
    > `config/ProcessorDefaultSettings.schema.json` schema.

    ---
    ## Returns
    The STAC items of the generated products.
    """
    logger = get_logger()

    ########################################################################
    # Check Prefect variable {processing_id.value}-default-setting format
    #########################################################################
    default_settings_var_name: str = f"{processing_id.value}-default-setting"
    logger.info(f"Read prefect variable named '{default_settings_var_name}'")

    default_settings: dict = await Variable.get(default_settings_var_name)  # type: ignore[misc]
    if default_settings is None:
        raise RuntimeError(f"❌ Prefect variable {default_settings_var_name!r} is missing")

    # Check JSON schema
    async with await anyio.open_file(DEFAULT_SETTINGS_SCHEMA, encoding="utf-8") as f:
        default_settings_schema = json.loads(await f.read())

    try:
        validate(instance=default_settings, schema=default_settings_schema)
    except ValidationError as err:
        raise ValueError(
            f"❌ Invalid JSON schema for var set in Prefect variable {default_settings_var_name!r}:\n{err.message}",
        ) from err

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, f"process-any-{processing_id}"):  # NOSONAR
        ##################################################
        # Check cluster dasks
        ##################################################
        processing_dask_cluster = default_settings["dask_cluster_name"]
        for dask_cluster in (processing_dask_cluster, DASK_CLUSTER_STAGING):
            if not await is_dask_cluster_running(dask_cluster):
                raise ValueError(f"❌ '{dask_cluster}' is unknown or not ready.")

        ##################################################
        # Create all input for DPR processing
        ##################################################
        logger.info(
            "input_products:\n%s",
            json.dumps([product.model_dump(mode="json") for product in input_products], indent=2, ensure_ascii=False),
        )
        logger.info("external_variables:\n%s", json.dumps(external_variables, indent=2, ensure_ascii=False))
        published_items: list[dict[str, Any]] = await call_dpr_flow(
            env=env_args,
            input_products=input_products,
            external_variables=external_variables,
            dask_cluster_label=processing_dask_cluster,
            processor_name=default_settings["processor"]["name"],
            processor_version=default_settings["processor"]["version"],
            pipeline=default_settings.get("pipeline", None),
            unit=default_settings.get("unit", None),
            priority=default_settings.get("priority", Priority.LOW),
            processing_mode=default_settings.get("processing_mode", [ProcessingMode.ALWAYS]),
            workflow=workflow,
            generated_product_to_collection_identifier=default_settings.get(
                "generated_product_to_collection_identifier",
                [],
            ),
            auxiliary_product_to_collection_identifier=default_settings.get(
                "auxiliary_product_to_collection_identifier",
                [],
            ),
        )

        for i, item in enumerate(published_items):
            logger.info(f"--- Item {i + 1} ---")
            logger.info(json.dumps(item, indent=2, ensure_ascii=False))

        # Events of a systematic workflow start the next processing of the chain
        emit_product_generated_events(env_args.owner_id, published_items, workflow)

        return published_items


async def process_catalog_product(
    env_args: FlowEnvArgs,
    input_item: FlowInputItem,
    force_systematic: bool,
    process_product: ProductProcessing,
) -> list[dict[str, Any]]:
    """
    Process on demand a product read from the catalog.

    Args:
        env_args: Flow environment arguments.
        input_item: STAC item identifier and collection of the product.
        force_systematic: Run the processing as a systematic one, so that the automations start the next processing
            of the chain from the events of the generated products.
        process_product: Processing of the product.

    Returns:
        The STAC items of the generated products.
    """
    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, "process-catalog-product"):  # NOSONAR
        item = await get_single_catalog_item(flow_env, input_item.item_id, [input_item.collection_name])
        if item is None:
            raise RuntimeError(
                f"❌ STAC item '{input_item.item_id}' not found in collection '{input_item.collection_name}'.",
            )
        return await process_product(
            env_args,
            input_item.item_id,
            input_item.collection_name,
            item.properties,
            WorkflowType.SYSTEMATIC if force_systematic else WorkflowType.ON_DEMAND,
        )


async def process_event_product(payload: dict[str, Any], process_product: ProductProcessing) -> list[dict[str, Any]]:
    """
    Process systematically the product of a `product.<product type>.generated` event.

    Args:
        payload: Payload of the event, with the owner and the STAC item of the product.
        process_product: Processing of the product.

    Returns:
        The STAC items of the generated products.
    """
    get_logger().info(f"Flow started from event with payload {payload}.")
    env_args = FlowEnvArgs(owner_id=payload["owner_id"])
    product = payload["product"]
    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, "process-event-product"):  # NOSONAR
        return await process_product(
            env_args,
            product["id"],
            product["collection"],
            product["properties"],
            WorkflowType.SYSTEMATIC,
        )
