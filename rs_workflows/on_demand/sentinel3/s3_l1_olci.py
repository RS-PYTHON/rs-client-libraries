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

from typing import Any

from prefect import flow, get_run_logger, task
from pystac import Item

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, FlowInputProduct
from rs_workflows.on_demand.common.types import Level1FlowParams
from rs_workflows.on_demand.sentinel3.s3_processing_utils import (
    build_olci_l1_input_products,
    read_s3_orchestration_settings,
)
from rs_workflows.utils.dpr import call_dpr_flow

OLCI_L0_PRODUCT_TYPE = "S03OLCL0_"
NAV_L0_PRODUCT_TYPE = "S03NATL0_"

@flow(
    name="process-s3-l1-olci",
    flow_run_name="s3-l1-olci-from-{source_l0_run_id}",
)
async def process_s3l1_olci(
    flow_params: Level1FlowParams | None = None,
    l0_products: list[dict[str, Any]] | None = None,
    source_l0_run_id: str = "manual",  # pylint: disable=unused-argument
) -> list[dict[str, Any]]:
    """
    Sentinel-3 OLCI L1 processing.
    The input_products should have been processed before by L0.

    ``l0_products`` is the raw product list emitted by S3 L0. When supplied by
    a Prefect Automation, it is converted here into the four (or more) processor inputs
    expected by OLCI L1. ``source_l0_run_id`` provides a short upstream
    reference used in the L1 flow-run name.
    """
    mission = "3"
    # how to use s3-l1-default-setting
    flow_parameters = await (flow_params or Level1FlowParams()).resolve(mission)

    if l0_products is not None:
        orchestration_settings = await read_s3_orchestration_settings()
        prepared_inputs = build_olci_l1_input_products(
            l0_products,
            orchestration_settings.s3_l0_output_collection,
        )
        flow_parameters.input_products = [FlowInputProduct.model_validate(product) for product in prepared_inputs]
        get_run_logger().info(
            "Built %d S3 L1 input product(s) from %d raw L0 product(s) received from Automation",
            len(flow_parameters.input_products),
            len(l0_products),
        )

    # Call DPR flow
    return await call_dpr_flow(
        FlowEnvArgs(owner_id=flow_parameters.owner_identifier),
        input_products=flow_parameters.input_products,
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
        logging_level=flow_parameters.logging_level,
    )


@task(name="process-s3-l1-olci")
async def process_s3l1_olci_task(*args, **kwargs) -> list[dict[str, Any]]:
    """See: dpr_processing"""
    return await process_s3l1_olci.fn(*args, **kwargs)
