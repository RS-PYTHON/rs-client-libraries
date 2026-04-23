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

from rs_workflows.flow_utils import (
    FlowInputProduct,
)
from rs_workflows.on_demand.common.l0_last_steps import process_l0_last_steps
from rs_workflows.on_demand.common.types import Level0FlowParams


@flow(name="process sentinel-3 level-0")
async def process_s3l0(session: str, flow_params: Level0FlowParams, verbose: bool = False):
    """
    Sentinel-3 L0 processing.
    The session should have been staged before.
    """

    input_products = [
        FlowInputProduct(
            name="S3ACADUS",
            item_id=session,
            collection_name=flow_params.session_collection,
        ),
    ]

    await process_l0_last_steps(
        mission="3",
        session=session,
        flow_params=flow_params,
        input_products=input_products,
        verbose=verbose,
    )


@task(name="process sentinel-3 level-0")
async def process_s3l0_task(*args, **kwargs) -> None:
    """See: dpr_processing"""
    return await process_s3l0.fn(*args, **kwargs)
