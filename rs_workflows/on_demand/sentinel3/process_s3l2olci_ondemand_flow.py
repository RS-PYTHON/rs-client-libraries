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

"""Process on-demand S3 OLCI L2 flow implementation"""

from functools import partial
from typing import Any

from prefect import flow
from pydantic import Field

from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct, WorkflowType
from rs_workflows.on_demand.common.product_processing import (
    FlowInputItem,
    process_catalog_product,
)
from rs_workflows.on_demand.sentinel3.s3_processing_utils import process_olci
from rs_workflows.utils.dpr import ProcessingId, ProcessorSettings

OLCEFR_PRODUCT_TYPE = "S03OLCEFR"
OLCEFR_DEFAULT_BASE_NAME = "S03OLCEFR"


class FlowInputS3l2olci(FlowInputItem):
    """S03OLCEFR product to process with the S3 L2 OLCI processor."""

    collection_name: str = Field(
        default=OLCEFR_PRODUCT_TYPE.lower(),
        description="Collection name for S03OLCEFR product.",
    )


async def process_s3l2olci_product(
    env_args: FlowEnvArgs,
    settings: ProcessorSettings,
    item_id: str,
    collection_name: str,
    properties: dict[str, Any],
    workflow: WorkflowType,
    olcefr_base_name: str = OLCEFR_DEFAULT_BASE_NAME,
) -> list[dict[str, Any]]:
    """
    Run the S3 L2 OLCI processing of an S03OLCEFR product, its single input, with the sensing dates of the product
    as external variables.

    Args:
        env_args: Flow environment arguments.
        settings: Default settings of the S3 L2 OLCI processing.
        item_id: STAC item identifier of the S03OLCEFR product.
        collection_name: Collection of the S03OLCEFR product.
        properties: STAC item properties of the S03OLCEFR product (`start_datetime`, `end_datetime`).
        workflow: Type of workflow of the processing.
        olcefr_base_name: S03OLCEFR product name on the task table.

    Returns:
        The STAC items of the generated products.
    """
    input_products = [FlowInputProduct(name=olcefr_base_name, item_id=item_id, collection_name=collection_name)]
    external_variables = {"start_datetime": properties["start_datetime"], "end_datetime": properties["end_datetime"]}
    return await process_olci("l2", env_args, settings, input_products, external_variables, workflow)


@flow(name="process-s3-l2-olci-ondemand")
async def process_s3l2olci_ondemand(
    env_args: FlowEnvArgs,
    single_input_product: FlowInputS3l2olci,
    force_systematic: bool = False,
    olcefr_base_name: str = OLCEFR_DEFAULT_BASE_NAME,
) -> list[dict[str, Any]]:
    """
    # Process ON DEMAND S3 L2 OLCI

    This flow is in charge to prepare the S3 L2 OLCI processing by computing all the needed inputs.
    Then it runs the processor with workflow set to "on-demand" ("systematic" if `force_systematic` is set).

    ---
    ## Workflow Steps

    1. *load the settings* — reads the `s3l2olci-default-setting` Prefect variable and checks that the Dask clusters
       are running.
    2. *compute input products* — Single `S03OLCEFR` item get from parameters call.
    3. *compute external variables* — external variables are extracted from the first item properties.
    4. *start the processing* — calls the flow `process-generic`, then emits the events of the generated products.
    5. *request the quicklooks* — emits the `rs-python.s3-l2.quicklook-inputs-ready` event with the generated products.
    ---
    ## Parameters
    Parameter | Type | Default | Description |
    |---|---|---|---|
    | `env_args` | `FlowEnvArgs` | *required* | Flow environment arguments |
    | `single_input_product` | `FlowInputS3l2olci` | *required* | STAC item and collection of the `S03OLCEFR` product |
    | `force_systematic` | `bool` | `False` | If true, the workflow is systematic: the next processing follows. |
    | `olcefr_base_name` | `str` | `S03OLCEFR` | S03OLCEFR product name on the task table. |

    ---
    """
    return await process_catalog_product(
        env_args,
        single_input_product,
        force_systematic,
        ProcessingId.S3_OLCI_L2,
        partial(process_s3l2olci_product, olcefr_base_name=olcefr_base_name),
    )
