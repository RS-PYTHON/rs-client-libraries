# Copyright 2023-2026 Airbus
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

from faker import Faker
from prefect import get_run_logger, task
from prefect.variables import Variable
from pystac import Item, ItemCollection

from rs_client.ogcapi.dpr_client import (
    DprPipeline,
    DprProcessor,
)
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnv,
    FlowEnvArgs,
    GeneratedProduct,
    InputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand_processing import dpr_processing


def generate_payload_path(owner_id: str) -> str:
    # TODO : use a local path on the share disk
    fake = Faker()
    s3_payload = f"s3://prip-rs-playground/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}-{fake.word().lower()}-{fake.word().lower()}"
    return s3_payload


async def call_dpr_flow(
    env: FlowEnvArgs,
    input_products: list[InputProduct],
    start_datetime: datetime,
    end_datetime: datetime,
    satellite_identifier: str,
    prefect_settings: str,
    dask_cluster_label: str="",
    processor_name: str="",
    processor_version: str="",
    pipeline: str="",
    unit: str="",
    priority: str="",
    processing_mode: list[ProcessingMode]= [],
    workflow: str=""
) -> None:
    """_summary_

    Args:
        owner_id (str): _description_
        dask_cluster_label (str): _description_
        item_session (Item): _description_
        start_datetime (datetime): _description_
        end_datetime (datetime): _description_
        satellite_identifier (str): _description_
    """
    s3_payload: str = generate_payload_path(env.owner_id)
    
    # Apply default configuration for unset parameters
    settings: dict = await read_prefect_variable(prefect_settings)
    processor_name = processor_name or settings["processor"]["name"]
    processor_version = processor_version or settings["processor"]["version"]
    if pipeline=="" and unit=="":
        pipeline = settings["pipeline"]
    priority = priority or settings["priority"]
    if processing_mode ==[]:
        processing_mode = settings["processing_mode"]
    workflow = workflow or settings["workflow"]
    dask_cluster_label = dask_cluster_label or settings["dask_cluster_label"]


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
        generated_product_to_collection_identifier=[
            GeneratedProduct(
                name="S01SARRAW",
                product_type="*",
                collection_name="s01sarraw",
            ),
            GeneratedProduct(
                name="S01GPSRAW",
                product_type="*",
                collection_name="s01gpsraw",
            ),
            GeneratedProduct(
                name="S01HKMRAW",
                product_type="*",
                collection_name="allproductions",
            ),
            GeneratedProduct(
                name="S01AISRAW",
                product_type="*",
                collection_name="allproductions",
            ),
        ],
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(
                product_type="MPL_ORBPRE",
                collection_name="s01-aux-mpl_orbpre",
            ),
            AuxiliaryProductMapping(
                product_type="MPL_ORBSCT",
                collection_name="s01-aux-mpl_orbpre",
            ),
        ],
        processing_mode=processing_mode,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        satellite=satellite_identifier,
    )

    print(a_process.model_dump_json(indent=2))
    # await dpr_processing_task(a_process)


@task(name="dpr processing")
async def dpr_processing_task(*args, **kwargs) -> tuple[bool, ItemCollection | None]:
    """See: dpr_processing"""
    return await dpr_processing.fn(*args, **kwargs)


@task(name="retrieve prefect variable")
async def read_prefect_variable(prefect_variable: str) -> dict:
    return await Variable.get(prefect_variable)
