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

import json
from typing import Any

from prefect import flow
from pydantic import BaseModel, Field

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, FlowInputProduct
from rs_workflows.process_any_flow import ProcessingId, process_any_on_demand
from rs_workflows.utils.prefect import get_logger

OLCEFR_PRODUCT_TYPE = "S03OLCEFR"
OLCEFR_DEFAULT_BASE_NAME = "S03OLCEFR"


class FlowInputS3l2olci(BaseModel):
    """Represents input product for the processor S3l2olci."""

    item_id: str = Field(description="STAC item identifier for S03OLCEFR product.")
    collection_name: str = Field(
        default=OLCEFR_PRODUCT_TYPE.lower(),
        description="Collection name for S03OLCEFR product.",
    )

    def items(self):
        """Helper method to return the model fields as items, useful for logging."""
        return self.model_dump().items()


def compute_s3l2olci_external_variables(
    start_datetime_txt: str,
    end_datetime_txt: str,
) -> dict[str, Any]:
    """Compute the external variables of the S3 L2 OLCI processor."""
    return {"start_datetime": start_datetime_txt, "end_datetime": end_datetime_txt}


@flow(name="process-s3-l2-olci-ondemand")
async def process_s3l2olci_ondemand(
    env_args: FlowEnvArgs,
    single_input_product: FlowInputS3l2olci,
    force_systematic: bool = False,
    olcefr_base_name: str = OLCEFR_DEFAULT_BASE_NAME,
):
    """
    # Process ON DEMAND S3 L2 OLCI

    This flow is in charge to prepare the S3 L2 OLCI processing by computing all the needed inputs.
    Then it forwards to `process-any` with workflow set to "on-demand".

    ---
    ## Workflow Steps

    1. *compute input products* — Single `S03OLCEFR` item get from parameters call.
    2. *compute external variables* — external variables are extracted from the first item properties.
    3. *start the processing* — calls the flow `process-any`
    ---
    ## Parameters
    Parameter | Type | Default | Description |
    |---|---|---|---|
    | `env_args` | `FlowEnvArgs` | *required* | Flow environment arguments |
    | `single_input_product` | `FlowInputS3l2olci` | *required* | STAC item and collection of the `S03OLCEFR` product |
    | `force_systematic` | `bool` | `False` | If true, one event per product is emitted at the end of the processing. |
    | `olcefr_base_name` | `str` | `S03OLCEFR` | S03OLCEFR product name on the task table. |

    ---
    """
    logger = get_logger()

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, "process_s3l2olci_ondemand"):  # NOSONAR
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        stac_item = catalog_client.get_item(single_input_product.collection_name, single_input_product.item_id)
        if stac_item is None:
            raise RuntimeError(
                f"❌ STAC item '{single_input_product.item_id}' not found "
                f"in collection '{single_input_product.collection_name}'.",
            )
        item: dict = stac_item.to_dict()

        input_products: list[FlowInputProduct] = []
        input_product: FlowInputProduct = FlowInputProduct(
            name=f"{olcefr_base_name}",
            item_id=single_input_product.item_id,
            collection_name=single_input_product.collection_name,
        )

        input_products.append(input_product)

        logger.info(
            "input_products:\n%s",
            json.dumps(
                [product.model_dump(mode="json") for product in input_products],
                indent=2,
                ensure_ascii=False,
            ),
        )

        external_variables = compute_s3l2olci_external_variables(
            item["properties"]["start_datetime"],
            item["properties"]["end_datetime"],
        )

        await process_any_on_demand(
            env_args,
            input_products,
            external_variables,
            ProcessingId.S3_OLCI_L2,
            force_systematic,
        )
