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

import time
from rs_workflows.on_demand_processing import dpr_processing
from rs_workflows.flow_utils import FlowEnv
from prefect import get_run_logger, task
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
from rs_client.ogcapi.dpr_client import (
    DprPipeline,
    DprProcessor,
)
from datetime import datetime
from pystac import Item
from prefect.client import get_client
import json


def generate_payload_path(owner_id:str)->str:
    # TODO : use a local path on the share disk
    fake = Faker()
    s3_payload = f"s3://prip-rs-playground/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}-{fake.word().lower()}-{fake.word().lower()}"    
    return s3_payload
    

async def call_dpr_flow(
    owner_id: str,
    dask_cluster_label: str,
    input_products: list[InputProduct],
    start_datetime: datetime,
    end_datetime: datetime,
    satellite_identifier: str,
    prefect_settings: str,
    
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
    s3_payload:str = generate_payload_path(owner_id)
    settings:dict = await read_prefect_variable(prefect_settings)
    
    a_process_s1l0:DprProcessIn = DprProcessIn(
        env=FlowEnvArgs(owner_id=owner_id),
        processor_name=DprProcessor.S1L0,
        processor_version="1.4.0",  # TODO: retrieve automatically
        dask_cluster_label=dask_cluster_label,
        s3_payload_file=f"{s3_payload}/payload.yaml",
        pipeline=DprPipeline.S1L0FULL,
        unit=None,
        priority=Priority.LOW,  # TODO: expose priority
        workflow_type=WorkflowType.ON_DEMAND,
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
        processing_mode=[ProcessingMode.ALWAYS],
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        satellite=satellite_identifier,
    )
    print(a_process_s1l0.model_dump_json(indent=2))
    
    # await dpr_processing_task(a_process_s1l0)


@task(name="dpr processing")
async def dpr_processing_task(*args, **kwargs) -> tuple[bool, ItemCollection | None]:
    """See: dpr_processing"""
    return await dpr_processing.fn(*args, **kwargs)


@task(name="retrieve prefect variable")
async def read_prefect_variable(prefect_variable:str)->dict:
    logger = get_run_logger()
    async with get_client() as client:
        raw = await client.read_variable(prefect_variable)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.error(f"Variable {prefect_variable} is not valid JSON.")
        raise
    return data