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

"""Process systematic and ondemand flow implementation"""

import datetime
import json
from enum import Enum
from pathlib import Path
from typing import Any

import anyio
from jsonschema import ValidationError, validate
from prefect import flow, runtime
from prefect.events import emit_event
from prefect.variables import Variable
from pystac import ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_common.utils import strftime_millis
from rs_workflows.adf_flow import substitute_values
from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand.adf.convert_adf_set import compute_cql2
from rs_workflows.utils.dask import is_dask_cluster_running
from rs_workflows.utils.dpr import call_dpr_flow
from rs_workflows.utils.prefect import get_logger

DASK_CLUSTER_STAGING = "dask-staging"


class ProcessingId(str, Enum):
    """
    Processing Identifier : usefull to determine configuration file pattern.
    """

    S3_L0 = "s3l0"
    S3_OLCI_L1 = "s3l1olci"
    S3_OLCI_L2 = "s3l2olci"


@flow(name="process-any")
async def process_any(
    env_args: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    processing_id: ProcessingId,
    workflow: WorkflowType = WorkflowType.ON_DEMAND,
):
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
    5. Raise a Prefect event per product generated
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
    > `processor_default_settings.schema.json` schema.

    ---
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
    schema = Path("rs_workflows/schemas/processor_default_settings.schema.json")
    if not schema.is_file():
        raise FileNotFoundError(f"❌ Schema file not found: {schema.resolve()}")
    async with await anyio.open_file(schema, encoding="utf-8") as f:
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
        if await is_dask_cluster_running(processing_dask_cluster) is False:
            raise ValueError(f"❌ '{processing_dask_cluster}' is unknown or not ready.")
        if await is_dask_cluster_running(DASK_CLUSTER_STAGING) is False:
            raise ValueError(f"❌ '{DASK_CLUSTER_STAGING}' is unknown or not ready.")

        ##################################################
        # Create all input for DPR processing
        ##################################################
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

        # Generate one event per product
        for i, item in enumerate(published_items):
            logger.info(f"--- Item {i + 1} ---")
            logger.info(json.dumps(item, indent=2, ensure_ascii=False))

            flow_run_id = str(runtime.flow_run.id or "unknown")

            event_name = f"product.{item['properties']['product:type']}.generated"
            event = emit_event(
                event=event_name,
                resource={
                    "prefect.resource.id": f"prefect.flow-run.{flow_run_id}",
                    "prefect.resource.name": f"{item['properties']['product:type']}",
                },
                related=[
                    {
                        "prefect.resource.id": "systematic",
                        "prefect.resource.role": "workflow",
                    },
                    {"prefect.resource.id": "prefect.tag.processing", "prefect.resource.role": "tag"},
                    {
                        "prefect.resource.id": f"prefect.tag.{item['properties']['platform']}",
                        "prefect.resource.role": "tag",
                    },
                ],
                payload={
                    "owner_id": f"{env_args.owner_id}",
                    "product": {
                        "id": f"{item['id']}",
                        "geometry": item["geometry"],
                        "collection": item["collection"],
                        "properties": item["properties"],
                    },
                },
            )

            if event is None:
                get_logger().warning(f"⚠️ Event '{event_name}' was not emitted")
            else:
                get_logger().info(f"Event '{event_name}' (id='{event.id}') emitted.")


async def process_any_on_demand(
    env_args: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    processing_id: ProcessingId,
    force_systematic: bool,
):
    """Call `process-any` from an on-demand flow, as a systematic run when `force_systematic` is set."""
    logger = get_logger()
    logger.info(
        "external_variables:\n%s",
        json.dumps(external_variables, indent=2, ensure_ascii=False),
    )

    if force_systematic:
        logger.info(
            "🛈 As force_systematic option is set to true, events will be emitted at the end of the processing.",
        )

    await process_any(
        env_args=env_args,
        input_products=input_products,
        external_variables=external_variables,
        processing_id=processing_id,
        workflow=WorkflowType.SYSTEMATIC if force_systematic else WorkflowType.ON_DEMAND,
    )


def get_latest_product_covering_interval(
    env_args: FlowEnvArgs,
    interval_begin_datetime: datetime.datetime,
    interval_end_datetime: datetime.datetime,
    product_type: str,
    satellite: str,
    collection: str,
) -> ItemCollection | None:
    """Search the catalog for the latest product of a type and satellite that covers the given interval."""
    logger = get_logger()
    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, "catalog_retrieve_product"):  # NOSONAR
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()

        logger.info("🧹 Computing cql2_filter")
        included_in_interval = compute_cql2("LatestIncludedWithSatellite", 2, 2, satellite)
        cql2_filter = substitute_values(
            included_in_interval,
            {
                "start_datetime": strftime_millis(interval_begin_datetime),
                "end_datetime": strftime_millis(interval_end_datetime),
                "product_type": product_type,
                "satellite": satellite,
            },
        )
        logger.info(
            f"We query the rs-catalog to found a product with type {product_type} on the interval "
            f"[{interval_begin_datetime}, {interval_end_datetime}]",
        )

        found = catalog_client.search(
            method="POST",
            stac_filter=cql2_filter.get("filter"),
            max_items=cql2_filter.get("limit"),
            sortby=cql2_filter.get("sortby"),
            collections=[collection],
        )

        if found:
            logger.info(f"✅ Found item named '{found[0].id}'")
        else:
            logger.info("🛈 No item found on the interval.")

        return found
