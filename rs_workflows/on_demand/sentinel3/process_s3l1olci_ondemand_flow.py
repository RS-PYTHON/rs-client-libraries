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

import datetime
import json
from typing import Any

from prefect import flow
from pydantic import BaseModel, Field

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, FlowInputProduct, WorkflowType
from rs_workflows.process_any_flow import (
    ProcessingId,
    get_latest_product_covering_interval,
    process_any,
)
from rs_workflows.utils.prefect import get_logger

OLCL0_PRODUCT_TYPE = "S03OLCL0_"
NATL0_PRODUCT_TYPE = "S03NATL0_"
OLCL0_DEFAULT_BASE_NAME = "S3OLCIL0_"
NAVL0_DEFAULT_BASE_NAME = "S3NAVL0_"


class FlowInputS3l1olci(BaseModel):
    """Represents input product for the processor S3l1olci."""

    item_id: str = Field(description="STAC item identifier for S3OLCIL0 product.")
    collection_name: str = Field(
        default=OLCL0_PRODUCT_TYPE.lower(),
        description="Collection name for S3OLCIL0 product.",
    )

    def items(self):
        """Helper method to return the model fields as items, useful for logging."""
        return self.model_dump().items()


def compute_s3l1olci_inputs(
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
    Comput input products for the flow s3l1olci
    """
    logger = get_logger()

    #
    # Each product should be stored on a collection named <product_type>
    # We check if this is the case. If not, we will use the same 'collection_name' for both OLCL0 and NATL0
    collection_natl0: str = ""
    if OLCL0_PRODUCT_TYPE.lower == collection_name:
        collection_natl0 = NATL0_PRODUCT_TYPE.lower
        logger.info(f"🛈 We use collection {collection_natl0} for S3L0NATLO products")

    else:
        collection_natl0 = collection_name
        logger.warning(f"⚠️ Workaround: we use collection {collection_natl0} for S3L0NATLO products")

    # The first element is the one used to call the flow
    input_products: list[FlowInputProduct] = []
    input_products.append(
        {
            "name": f"{olcil0_base_name}1",
            "item_id": item_id,
            "collection_name": collection_name,
        },
    )
    index: int = 2
    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, f"process_s3l1olci_input"):
        # We will extract the product duration in seconds
        start_datetime = datetime.datetime.fromisoformat(start_datetime_txt)
        end_datetime = datetime.datetime.fromisoformat(end_datetime_txt)
        duration_seconds = (end_datetime - start_datetime).total_seconds()
        logger.info(f"The product duration is {duration_seconds} seconds.")

        #####################################################
        #  TRY to get the next product   OLCL0_PRODUCT_TYPE #
        #####################################################
        found_next = get_latest_product_covering_interval(
            env_args,
            start_datetime + datetime.timedelta(seconds=duration_seconds),
            end_datetime + datetime.timedelta(seconds=duration_seconds),
            OLCL0_PRODUCT_TYPE,
            satellite,
            collection_name,
        )

        if found_next:
            input_products.append(
                {
                    "name": f"{olcil0_base_name}{index}",
                    "item_id": found_next[0].to_dict()["id"],
                    "collection_name": collection_name,
                },
            )
            index = index + 1

        #######################################################
        #  TRY to get the previous product OLCL0_PRODUCT_TYPE #
        #######################################################
        found_previous = get_latest_product_covering_interval(
            env_args,
            start_datetime - datetime.timedelta(seconds=duration_seconds),
            end_datetime - datetime.timedelta(seconds=duration_seconds),
            OLCL0_PRODUCT_TYPE,
            satellite,
            collection_name,
        )

        if found_previous:
            input_products.append(
                {
                    "name": f"{olcil0_base_name}{index}",
                    "item_id": found_previous[0].to_dict()["id"],
                    "collection_name": collection_name,
                },
            )
            index = index + 1

        #######################################
        #  Get the product NATL0_PRODUCT_TYPE #
        #######################################
        found_nat = get_latest_product_covering_interval(
            env_args,
            start_datetime,
            end_datetime,
            NATL0_PRODUCT_TYPE,
            satellite,
            collection_natl0,
        )

        if found_nat:
            input_products.append(
                {
                    "name": f"{navl0_base_name}1",
                    "item_id": found_nat[0].to_dict()["id"],
                    "collection_name": collection_natl0,
                },
            )

    return input_products


def compute_s3l1olci_external_variables(
    satellite: str,
    start_datetime_txt: str,
    end_datetime_txt: str,
) -> dict[str, Any]:
    return {"start_datetime": start_datetime_txt, "end_datetime": end_datetime_txt, "satellite": satellite}


@flow(name="process-s3-l1-olci-ondemand")
async def process_s3l1olci_ondemand(
    env_args: FlowEnvArgs,
    single_input_product: FlowInputS3l1olci,
    force_systematic: bool = False,
    olcil0_base_name: str = OLCL0_DEFAULT_BASE_NAME,
    navl0_base_name: str = NAVL0_DEFAULT_BASE_NAME,
):
    """
    # Process ON DEMAND S3 L1 OLCI

    This flow is in charge to prepare the S3 L1 OLCI processing by computing all the needed inputs.
    Then it forwards to `process-any` with workflow set to "on-demand".

    ---
    ## Workflow Steps

    1. *compute input products* — retrieve the stac items `S3OLCIL0_` from rs-catalog.
       The first one is retrieve from `item_id` and `collection_name` parameters.
       The other ones are get by requesting contiguous period on the rs-catalog.
       They can be one or two in this case.
    2. *compute external variables* — external variables are extracted from the first item properties.
    3. *start the processing* — calls the flow `process-any`
    ---
    ## Parameters
    Parameter | Type | Default | Description |
    |---|---|---|---|
    | `env_args` | `FlowEnvArgs` | *required* | Flow environment arguments |
    | `single_input_product` | `FlowInputS3l1olci` | *required* | Stac item identifier and collection name corresponding to the OLCI L0 product |
    | `force_systematic` | `bool` | `False` | When this option is set to true, one event per product will be generated at the end of the processing. |
    | `olcil0_base_name` | `str` | `S3OLCIL0_` | Prefix of the OLCI L0 product name on the task table. |
    | `navl0_base_name` | `str` | `S3NAVL0_` | Prefix of the NAV L0 product name on the task table. |

    > *Note:* if the collection for `S3OLCIL0_`name is equal to `s3olcil0_`, `S3NAVL0_` will be stored on the collection `s3navl0_`.
    Otherwise, `S3NAVL0_` will be stored on the same collection than `S3OLCIL0_`.
    ---
    """
    logger = get_logger()

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, f"process_s3l1olci_ondemand"):
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        stac_item = catalog_client.get_item(single_input_product.collection_name, single_input_product.item_id)
        if stac_item is None:
            raise RuntimeError(
                f"❌ STAC item '{single_input_product.item_id}' not found in collection '{single_input_product.collection_name}'.",
            )
        item: dict = stac_item.to_dict()

        input_products: list[FlowInputProduct] = compute_s3l1olci_inputs(
            env_args,
            single_input_product.item_id,
            single_input_product.collection_name,
            item["properties"]["platform"],
            item["properties"]["start_datetime"],
            item["properties"]["end_datetime"],
            olcil0_base_name,
            navl0_base_name,
        )

        logger.info(
            "input_products:\n%s",
            json.dumps(input_products, indent=2, ensure_ascii=False),
        )

        external_variables = compute_s3l1olci_external_variables(
            item["properties"]["platform"],
            item["properties"]["start_datetime"],
            item["properties"]["end_datetime"],
        )

        logger.info(
            "external_variables:\n%s",
            json.dumps(external_variables, indent=2, ensure_ascii=False),
        )

        if force_systematic:
            logger.info(
                "🛈 As force_systematic option is set to true, events will be emitted at the end of the processing.",
            )

        await process_any(
            env_args=env_args,
            input_products=input_products,
            external_variables=external_variables,
            processing_id=ProcessingId.S3_OLCI_L1,
            workflow=WorkflowType.SYSTEMATIC if force_systematic else WorkflowType.ON_DEMAND,
        )
