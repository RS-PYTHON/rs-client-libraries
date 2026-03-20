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

"""Helper task to interact with the DPR as a service."""

import json
import time
from datetime import datetime

from prefect import get_run_logger, task
from prefect.variables import Variable
from pystac import Item, ItemCollection
from typing import Optional

from rs_client.ogcapi.dpr_client import (
    DprPipeline,
    DprProcessor,
)
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnvArgs,
    FlowGeneratedProduct,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand_processing import dpr_processing


def generate_payload_path(owner_id: str) -> str:
    """_summary_
    Generate an hard coded path to store the payload.
    This is a workaroud, waiting for share disk solution.
    """    
    # TODO : use a local path on the share disk
    s3_payload = f"s3://prip-rs-playground/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}"
    return s3_payload


async def call_dpr_flow(
    env: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    start_datetime: datetime,
    end_datetime: datetime,
    satellite_identifier: str,
    dask_cluster_label: str,
    processor_name: str,
    processor_version: str,
    pipeline: Optional[DprPipeline],
    unit: str,
    priority: Optional[Priority],
    processing_mode: list[ProcessingMode],
    workflow: Optional[WorkflowType],
    generated_product_to_collection_identifier:list[FlowGeneratedProduct],
    auxiliary_product_to_collection_identifier:list[AuxiliaryProductMapping]
) -> None:
    """
    Call any DPR processing flow with a set of default parameters.
    In case an optional parameter is not set, its value is get from Prefect Variable named 'prefect_settings'
    """
    s3_payload: str = generate_payload_path(env.owner_id)
    
    a_process: DprProcessIn = DprProcessIn(
        env=env,
        processor_name=DprProcessor(processor_name),
        processor_version=processor_version,
        dask_cluster_label=dask_cluster_label,
        s3_payload_file=f"{s3_payload}/payload_{processor_name}.yaml",
        pipeline=DprPipeline(pipeline),
        unit=unit,
        priority=Priority(priority),
        workflow_type=WorkflowType(workflow),
        input_products=input_products,
        generated_product_to_collection_identifier=generated_product_to_collection_identifier,
        auxiliary_product_to_collection_identifier=auxiliary_product_to_collection_identifier,
        processing_mode=processing_mode,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        satellite=satellite_identifier,
    )

    print(a_process.model_dump_json(indent=2))
    await dpr_processing_task(a_process)


@task(name="dpr processing")
async def dpr_processing_task(*args, **kwargs) -> tuple[bool, ItemCollection | None]:
    """See: dpr_processing"""
    return await dpr_processing.fn(*args, **kwargs)
