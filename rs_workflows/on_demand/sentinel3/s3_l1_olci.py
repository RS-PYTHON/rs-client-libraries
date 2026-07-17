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

"""sentinel 3 OLCI Level-1 processing."""

from datetime import datetime

from prefect import flow, task

from rs_workflows.flow_utils import FlowEnvArgs
from rs_workflows.on_demand.common.types import Level1FlowParams
from rs_workflows.utils.dpr import call_dpr_flow


@flow(name="process-s3-l1-olci")
async def process_s3l1_olci(flow_params: Level1FlowParams):
    """
    Sentinel-3 OLCI L1 processing.
    The input_products should have been processed before by L0.
    """
    mission = "3"
    # how to use s3-l1-default-setting
    flow_parameters = await flow_params.resolve(mission)

    # Call DPR flow
    await call_dpr_flow(
        FlowEnvArgs(owner_id=flow_parameters.owner_identifier),
        input_products=flow_parameters.input_products,
        external_variables={
            "start_datetime": flow_params.start_datetime,
            "end_datetime": flow_params.end_datetime,
            "satellite": flow_params.satellite,
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
        logging_level=flow_parameters.logging_level,
    )


@task(name="process-s3-l1-olci")
async def process_s3l1_olci_task(*args, **kwargs) -> None:
    """See: dpr_processing"""
    return await process_s3l1_olci.fn(*args, **kwargs)
