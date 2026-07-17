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

from prefect import flow, task
from typing import Any

from rs_workflows.flow_utils import (
    FlowInputProduct,
)
from rs_workflows.on_demand.common.l0_last_steps import process_l0_last_steps
from rs_workflows.on_demand.common.types import Level0FlowParams


@flow(name="process-s3-l0", persist_result=True)
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

    input_products = [
        FlowInputProduct(
            name="S3ACADUS",
            item_id=session,
            collection_name=resolved_flow_params.session_collection,
        ),
    ]

    return await process_l0_last_steps(
        mission="3",
        session=session,
        flow_params=resolved_flow_params,
        input_products=input_products,
        verbose=verbose,
    )


@task(name="process-s3-l0")
async def process_s3l0_task(*args, **kwargs) -> list[dict[str, Any]]:
    """See: dpr_processing"""
    return await process_s3l0.fn(*args, **kwargs)
