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

"""sentinel 3 Level-0 processing."""

from typing import Any
from uuid import uuid4

from prefect import flow, get_run_logger, runtime, task
from prefect.events import emit_event

from rs_workflows.on_demand.common.types import Level0FlowParams
from rs_workflows.on_demand.sentinel3.s3_processing_utils import (
    read_s3_orchestration_settings,
)

S3_L0_RESULT_STORAGE = "local-file-system/s3-processing-shared-results"
S3_L0_PRODUCTS_READY_EVENT = "rs-python.s3-l0.products-ready"


def _build_mock_l0_products(output_collection: str) -> list[dict[str, Any]]:
    """Build fake OLCI/NAV products for testing the event-driven L1 trigger."""

    def mock_product(product_type: str) -> dict[str, Any]:
        item_id = f"{product_type}_MOCK_{uuid4().hex}.zarr"
        return {
            "type": "Feature",
            "id": item_id,
            "properties": {"product:type": product_type},
            "collection": output_collection,
        }

    return [
        mock_product("S03OLCL0_"),
        mock_product("S03OLCL0_"),
        mock_product("S03OLCL0_"),
        mock_product("S03NATL0_"),
    ]


@flow(
    name="process-s3-l0",
    persist_result=True,
    result_storage=S3_L0_RESULT_STORAGE,
)
async def process_s3l0(
    session: str,
    flow_params: Level0FlowParams | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Sentinel-3 L0 processing.
    The session should have been staged before.
    """

    # Resolve values from the s3-l0-default-setting Prefect variable before
    # using them to build the DPR input. Explicit flow parameters still win.
    resolved_flow_params = await (flow_params or Level0FlowParams()).resolve("3")

    orchestration_settings = await read_s3_orchestration_settings()
    logger = get_run_logger()
    logger.warning(
        "TEMPORARY TEST MODE: skipping S3 L0 processing and emitting fake OLCI/NAV products for session=%s",
        session,
    )
    products = _build_mock_l0_products(orchestration_settings.s3_l0_output_collection)

    flow_run_id = str(runtime.flow_run.id or "unknown")
    emitted_event = emit_event(
        event=S3_L0_PRODUCTS_READY_EVENT,
        resource={
            "prefect.resource.id": f"rs-python.s3-l0-result.{flow_run_id}",
            "prefect.resource.name": session,
            "rs-python.session-id": session,
        },
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{flow_run_id}",
                "prefect.resource.role": "flow-run",
            },
        ],
        payload={
            "flow_run_id": flow_run_id,
            "session_id": session,
            "owner_identifier": resolved_flow_params.owner_identifier,
            "mocked": True,
            "products": products,
        },
    )
    if emitted_event is None:
        logger.warning(
            "S3 L0 products-ready event was not emitted: event=%s, flow_run_id=%s, session=%s",
            S3_L0_PRODUCTS_READY_EVENT,
            flow_run_id,
            session,
        )
    else:
        logger.info(
            "Emitted event=%s, event_id=%s, flow_run_id=%s, session=%s, product_count=%d",
            S3_L0_PRODUCTS_READY_EVENT,
            emitted_event.id,
            flow_run_id,
            session,
            len(products),
        )

    return products


@task(name="process-s3-l0")
async def process_s3l0_task(*args, **kwargs) -> list[dict[str, Any]]:
    """See: dpr_processing"""
    return await process_s3l0.fn(*args, **kwargs)
