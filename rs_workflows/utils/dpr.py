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

import time
from typing import Any

from prefect import task

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
    LoggingLevel,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand_processing import dpr_processing


def generate_payload_path(owner_id: str) -> str:
    """
    Generate an hard coded path to store the payload.
    This is a workaroud, waiting for share disk solution.
    """
    # TODO : use a local path on the share disk
    s3_payload = f"s3://prip-rs-playground/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}"
    return s3_payload


async def call_dpr_flow(
    env: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    dask_cluster_label: str,
    processor_name: str,
    processor_version: str,
    pipeline: DprPipeline | str | None,
    unit: str | None,
    priority: Priority | None,
    processing_mode: list[ProcessingMode],
    workflow: WorkflowType | None,
    generated_product_to_collection_identifier: list[FlowGeneratedProduct],
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping],
    logging_level: LoggingLevel = LoggingLevel.INFO,
    dask_task_timeout: int | None = None,
    temporary_folder: str | None = None,
    temporary_shared: bool = False,
) -> list[dict[str, Any]]:
    """
    Call any DPR processing flow with a set of default parameters.
    In case an optional parameter is not set, its value is get from Prefect Variable named 'prefect_settings'
    The payload is stored on a S3 bucket.
    """
    s3_payload: str = generate_payload_path(env.owner_id)

    a_process: DprProcessIn = DprProcessIn(
        env=env,
        processor_name=DprProcessor(processor_name),
        processor_version=processor_version,
        dask_cluster_label=dask_cluster_label,
        s3_payload_file=f"{s3_payload}/payload_{processor_name}.yaml",
        pipeline=(
            DprPipeline(pipeline) if pipeline in DprPipeline._value2member_map_ else pipeline  # pylint: disable=W0212
        ),
        unit=unit,
        priority=Priority(priority),
        workflow_type=WorkflowType(workflow),
        input_products=input_products,
        generated_product_to_collection_identifier=generated_product_to_collection_identifier,
        auxiliary_product_to_collection_identifier=auxiliary_product_to_collection_identifier,
        logging_level=logging_level,
        dask_task_timeout=dask_task_timeout,
        temporary_folder=temporary_folder,
        temporary_shared=temporary_shared,
        processing_mode=processing_mode,
        **external_variables,
    )

    print(a_process.model_dump_json(indent=2))
    return await dpr_processing_task(a_process)


@task(name="dpr processing")
async def dpr_processing_task(*args, **kwargs) -> list[dict[str, Any]]:
    """See: dpr_processing"""
    return await dpr_processing.fn(*args, **kwargs)
