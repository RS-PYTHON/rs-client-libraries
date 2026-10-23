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

"""Process on-demand S3 OLCI L1 flow implementation"""

import datetime
from collections import Counter
from functools import partial
from typing import Any

from prefect import flow
from pydantic import Field
from pystac import ItemCollection

from rs_common.utils import strftime_millis
from rs_workflows.adf_flow import substitute_values
from rs_workflows.catalog_flow import catalog_search_task
from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct, WorkflowType
from rs_workflows.on_demand.adf.convert_adf_set import compute_cql2
from rs_workflows.on_demand.sentinel3.s3_processing_utils import process_olci
from rs_workflows.process_any_flow import FlowInputItem, process_catalog_product
from rs_workflows.utils.prefect import get_logger

OLCL0_PRODUCT_TYPE: str = "S03OLCL0_"
NATL0_PRODUCT_TYPE: str = "S03NATL0_"
OLCL0_DEFAULT_BASE_NAME: str = "S3OLCIL0_"
NAVL0_DEFAULT_BASE_NAME: str = "S3NAVL0_"


class FlowInputS3l1olci(FlowInputItem):
    """S03OLCL0_ product to process with the S3 L1 OLCI processor."""

    collection_name: str = Field(
        default=OLCL0_PRODUCT_TYPE.lower(),
        description="Collection name for S3OLCIL0 product.",
    )


async def get_latest_product_covering_interval(
    env_args: FlowEnvArgs,
    interval_begin_datetime: datetime.datetime,
    interval_end_datetime: datetime.datetime,
    product_type: str,
    satellite: str,
    collection: str,
) -> ItemCollection | None:
    """Search the catalog for the latest product of a type and satellite that covers the given interval."""
    cql2_filter = substitute_values(
        compute_cql2("LatestIncludedWithSatellite", 2, 2, satellite),
        {
            "start_datetime": strftime_millis(interval_begin_datetime),
            "end_datetime": strftime_millis(interval_end_datetime),
            "product_type": product_type,
            "satellite": satellite,
        },
    )
    return await catalog_search_task(env_args, cql2_filter, collections=[collection])


async def compute_s3l1olci_inputs(
    env_args: FlowEnvArgs,
    item_id: str,
    collection_name: str,
    satellite: str,
    start_datetime_txt: str,
    end_datetime_txt: str,
    olcil0_base_name: str,
    navl0_base_name: str,
) -> list[FlowInputProduct]:
    """
    Compute the input products of the S3 L1 OLCI processing of an S03OLCL0_ product: the product itself, the next
    and previous S03OLCL0_ products and the S03NATL0_ product of the same period, when they are in the catalog.
    """
    # Each product should be stored on a collection named <product_type>.
    # If this is not the case, we use the same 'collection_name' for both OLCL0 and NATL0.
    collection_natl0 = NATL0_PRODUCT_TYPE.lower()
    if collection_name != OLCL0_PRODUCT_TYPE.lower():
        collection_natl0 = collection_name
        get_logger().warning(f"⚠️ Workaround: we use collection {collection_natl0} for S3L0NATLO products")

    start_datetime = datetime.datetime.fromisoformat(start_datetime_txt)
    end_datetime = datetime.datetime.fromisoformat(end_datetime_txt)
    duration = end_datetime - start_datetime

    # The first element is the one used to call the flow
    input_products = [FlowInputProduct(name=f"{olcil0_base_name}1", item_id=item_id, collection_name=collection_name)]
    names = Counter({olcil0_base_name: 1})
    for base_name, product_type, collection, shift in (
        (olcil0_base_name, OLCL0_PRODUCT_TYPE, collection_name, duration),  # next product
        (olcil0_base_name, OLCL0_PRODUCT_TYPE, collection_name, -duration),  # previous product
        (navl0_base_name, NATL0_PRODUCT_TYPE, collection_natl0, datetime.timedelta(0)),
    ):
        found = await get_latest_product_covering_interval(
            env_args,
            start_datetime + shift,
            end_datetime + shift,
            product_type,
            satellite,
            collection,
        )
        if found:
            names[base_name] += 1
            input_products.append(
                FlowInputProduct(
                    name=f"{base_name}{names[base_name]}",
                    item_id=found[0].id,
                    collection_name=collection,
                ),
            )
    return input_products


async def process_s3l1olci_product(
    env_args: FlowEnvArgs,
    item_id: str,
    collection_name: str,
    properties: dict[str, Any],
    workflow: WorkflowType,
    olcil0_base_name: str = OLCL0_DEFAULT_BASE_NAME,
    navl0_base_name: str = NAVL0_DEFAULT_BASE_NAME,
) -> list[dict[str, Any]]:
    """
    Run the S3 L1 OLCI processing of an S03OLCL0_ product, with the sensing dates and the satellite of the product
    as external variables.

    Args:
        env_args: Flow environment arguments.
        item_id: STAC item identifier of the S03OLCL0_ product.
        collection_name: Collection of the S03OLCL0_ product.
        properties: STAC item properties of the S03OLCL0_ product (`platform`, `start_datetime`, `end_datetime`).
        workflow: Type of workflow of the processing.
        olcil0_base_name: Prefix of the OLCI L0 product name on the task table.
        navl0_base_name: Prefix of the NAV L0 product name on the task table.

    Returns:
        The STAC items of the generated products.
    """
    satellite = properties["platform"]
    start_datetime = properties["start_datetime"]
    end_datetime = properties["end_datetime"]
    input_products = await compute_s3l1olci_inputs(
        env_args,
        item_id,
        collection_name,
        satellite,
        start_datetime,
        end_datetime,
        olcil0_base_name,
        navl0_base_name,
    )
    external_variables = {"start_datetime": start_datetime, "end_datetime": end_datetime, "satellite": satellite}
    return await process_olci("l1", env_args, input_products, external_variables, workflow)


@flow(name="process-s3-l1-olci-ondemand")
async def process_s3l1olci_ondemand(
    env_args: FlowEnvArgs,
    single_input_product: FlowInputS3l1olci,
    force_systematic: bool = False,
    olcil0_base_name: str = OLCL0_DEFAULT_BASE_NAME,
    navl0_base_name: str = NAVL0_DEFAULT_BASE_NAME,
) -> list[dict[str, Any]]:
    """
    # Process ON DEMAND S3 L1 OLCI

    This flow is in charge to prepare the S3 L1 OLCI processing by computing all the needed inputs.
    Then it forwards to `process-any` with workflow set to "on-demand" ("systematic" if `force_systematic` is set).

    ---
    ## Workflow Steps

    1. *compute input products* — retrieve the stac items `S3OLCIL0_` from rs-catalog.
       The first one is retrieve from `item_id` and `collection_name` parameters.
       The other ones are get by requesting contiguous period on the rs-catalog.
       They can be one or two in this case, plus the `S3NAVL0_` item of the same period.
    2. *compute external variables* — external variables are extracted from the first item properties.
    3. *start the processing* — calls the flow `process-any`
    4. *request the quicklooks* — emits the `rs-python.s3-l1.quicklook-inputs-ready` event with the generated products.
    ---
    ## Parameters
    Parameter | Type | Default | Description |
    |---|---|---|---|
    | `env_args` | `FlowEnvArgs` | *required* | Flow environment arguments |
    | `single_input_product` | `FlowInputS3l1olci` | *required* | STAC item and collection of the OLCI L0 product |
    | `force_systematic` | `bool` | `False` | If true, the workflow is systematic: the L2 processing follows. |
    | `olcil0_base_name` | `str` | `S3OLCIL0_` | Prefix of the OLCI L0 product name on the task table. |
    | `navl0_base_name` | `str` | `S3NAVL0_` | Prefix of the NAV L0 product name on the task table. |

    > *Note:* if the collection for `S3OLCIL0_`name is equal to `s03olcl0_`, `S3NAVL0_` will be stored on the
    collection `s03natl0_`. Otherwise, `S3NAVL0_` will be stored on the same collection than `S3OLCIL0_`.
    ---
    """
    return await process_catalog_product(
        env_args,
        single_input_product,
        force_systematic,
        partial(process_s3l1olci_product, olcil0_base_name=olcil0_base_name, navl0_base_name=navl0_base_name),
    )
