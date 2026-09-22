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

"""sentinel 3 OLCI Level-2 processing."""

# pylint: disable=duplicate-code

from typing import Any

from prefect import flow, get_run_logger, runtime, task
from prefect.events import emit_event

from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct
from rs_workflows.on_demand.common.types import Level2FlowParams
from rs_workflows.utils.dpr import call_dpr_flow


@flow(
    name="process-s3-l2-olci",
)
async def process_s3l2_olci(
    flow_params: Level2FlowParams | None = None,
    input_products: list[FlowInputProduct] | None = None,
) -> list[dict[str, Any]]:
    """
    Sentinel-3 OLCI L2 processing.
    """
    mission = "3"
    # to use s3-l2-default-setting
    flow_parameters = await (flow_params or Level2FlowParams()).resolve(mission)

    get_run_logger().info(f"Flow params: {flow_parameters}")
    # Call DPR flow
    products = await call_dpr_flow(
        FlowEnvArgs(owner_id=flow_parameters.owner_identifier),
        input_products=input_products or flow_parameters.input_products,
        external_variables={
            "start_datetime": flow_parameters.start_datetime,
            "end_datetime": flow_parameters.end_datetime,
            "satellite": flow_parameters.satellite,
        },
        dask_cluster_label=flow_parameters.dask_cluster_label,
        processor_name=flow_parameters.processor_name,
        processor_version=flow_parameters.processor_version,
        pipeline=flow_parameters.pipeline,
        unit=flow_parameters.unit,
        priority=flow_parameters.priority,
        processing_mode=flow_parameters.processing_mode,
        workflow=flow_parameters.workflow,
        generated_product_to_collection_identifier=flow_parameters.generated_product_to_collection_identifier or [],
        auxiliary_product_to_collection_identifier=flow_parameters.auxiliary_product_to_collection_identifier or [],
    )

    flow_run_id = str(runtime.flow_run.id or "unknown")
    # Trigger quicklooks independently for all published products.
    if products:
        quicklook_event_name = "rs-python.s3-l2.quicklook-inputs-ready"
        quicklook_event = emit_event(
            event=quicklook_event_name,
            resource={
                "prefect.resource.id": f"rs-python.s3-l2-result.{flow_run_id}",
                "prefect.resource.name": "S3 OLCI L2 products",
            },
            related=[
                {
                    "prefect.resource.id": f"prefect.flow-run.{flow_run_id}",
                    "prefect.resource.role": "flow-run",
                },
            ],
            payload={
                "owner_id": flow_parameters.owner_identifier,
                # Only send catalog references; the quicklook flow reads the full items itself.
                "published_items": [{"id": product["id"], "collection": product["collection"]} for product in products],
            },
        )
        if quicklook_event is None:
            get_run_logger().warning(
                "Quicklook-inputs-ready event was not emitted: event=%s, flow_run_id=%s",
                quicklook_event_name,
                flow_run_id,
            )
        else:
            get_run_logger().info(
                "Emitted event=%s, event_id=%s, product_count=%d",
                quicklook_event_name,
                quicklook_event.id,
                len(products),
            )
    return products


@task(name="process-s3-l2-olci")
async def process_s3l2_olci_task(*args, **kwargs) -> list[dict[str, Any]]:
    """See: dpr_processing"""
    return await process_s3l2_olci.fn(*args, **kwargs)
