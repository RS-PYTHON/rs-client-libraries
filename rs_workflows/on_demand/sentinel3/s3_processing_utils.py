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

"""Code shared by the Sentinel-3 OLCI processing flows."""

from typing import Any, Literal

from prefect import runtime
from prefect.events import emit_event

from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct, WorkflowType
from rs_workflows.process_any_flow import ProcessingId, process_any
from rs_workflows.utils.prefect import get_logger

OLCI_PROCESSING_IDS = {"l1": ProcessingId.S3_OLCI_L1, "l2": ProcessingId.S3_OLCI_L2}


async def process_olci(
    level: Literal["l1", "l2"],
    env_args: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    workflow: WorkflowType,
) -> list[dict[str, Any]]:
    """Run the OLCI processing of a level with `process-any`, then request the quicklooks of the generated products."""
    products = await process_any(
        env_args=env_args,
        input_products=input_products,
        external_variables=external_variables,
        processing_id=OLCI_PROCESSING_IDS[level],
        workflow=workflow,
    )
    emit_olci_quicklook_event(level, env_args.owner_id, products)
    return products


def emit_olci_quicklook_event(
    level: Literal["l1", "l2"],
    owner_id: str,
    products: list[dict[str, Any]],
) -> None:
    """Request quicklooks for published OLCI products, linked to the current flow run."""
    if not products:
        return

    flow_run_id = str(runtime.flow_run.id or "unknown")
    event_name = f"rs-python.s3-{level}.quicklook-inputs-ready"
    event = emit_event(
        event=event_name,
        resource={
            "prefect.resource.id": f"rs-python.s3-{level}-result.{flow_run_id}",
            "prefect.resource.name": f"S3 OLCI {level.upper()} products",
        },
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{flow_run_id}",
                "prefect.resource.role": "flow-run",
            },
        ],
        payload={
            "owner_id": owner_id,
            # Only send catalog references; the quicklook flow reads the full items itself.
            "published_items": [{"id": product["id"], "collection": product["collection"]} for product in products],
        },
    )
    if event is None:
        # Missing quicklook events do not prevent returning the published processing results.
        get_logger().warning(
            "Quicklook-inputs-ready event was not emitted: event=%s, flow_run_id=%s",
            event_name,
            flow_run_id,
        )
    else:
        get_logger().info(
            "Emitted event=%s, event_id=%s, product_count=%d",
            event_name,
            event.id,
            len(products),
        )
