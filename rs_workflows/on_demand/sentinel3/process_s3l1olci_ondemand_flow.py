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
import json

from prefect import flow, task
from pydantic import BaseModel, Field
from pystac import ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
)
from rs_workflows.process_any_flow import (
    ProcessingId,
    get_latest_product_covering_interval,
    process_any,
)
from rs_workflows.utils.prefect import get_logger


class FlowInputS3l1olci(BaseModel):
    """Represents input product for the processor S3l1olci."""

    item_id: str = Field(description="STAC item identifier for S3OLCIL0 product.")
    collection_name: str = Field(description="Collection name for S3OLCIL0 product.")

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
    product_type: str,
    olcil0_base_name: str,
    navl0_base_name: str,
) -> list[FlowInputProduct]:
    """
    Comput input products for the flow s3l1olci
    """
    logger = get_logger()

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

        # we compute the search interval for the next product
        next_interval_start = start_datetime + datetime.timedelta(seconds=duration_seconds)
        next_interval_end = end_datetime + datetime.timedelta(seconds=duration_seconds)

        found_next = get_latest_product_covering_interval(
            env_args,
            next_interval_start,
            next_interval_end,
            product_type,
            satellite,
            collection_name,
        )

        if found_next:
            logger.info(f"Found the next item named '{found_next[0].to_dict()["id"]}'")
            input_products.append(
                {
                    "name": f"{olcil0_base_name}{index}",
                    "item_id": found_next[0].to_dict()["id"],
                    "collection_name": collection_name,
                },
            )
            index = index + 1


@flow(name="process-s3l1olci-ondemand")
async def process_s3l1olci_ondemand(
    env_args: FlowEnvArgs,
    single_input_product: FlowInputS3l1olci,
    olcil0_base_name: str = "S3OLCIL0_",
    navl0_base_name: str = "S3NAVL0_",
):
    """
    This flow compute other input and external variable from a single input.
    Then it forwards to process-any  with workflow  set to "on-demand".

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
            item["properties"]["product:type"],
            olcil0_base_name,
            navl0_base_name,
        )

        logger.info(
            "input_products:\n%s",
            json.dumps(input_products, indent=2, ensure_ascii=False),
        )
