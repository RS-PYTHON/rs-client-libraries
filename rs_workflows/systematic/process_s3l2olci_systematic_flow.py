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

"""Process systematic S3 OLCI L2 flow implementation"""

import json
from typing import Any

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
    WorkflowType,
)
from rs_workflows.on_demand.sentinel3.process_s3l2olci_ondemand_flow import (
    OLCEFR_DEFAULT_BASE_NAME,
    OLCEFR_PRODUCT_TYPE,
    compute_s3l2olci_external_variables,
)
from rs_workflows.process_any_flow import ProcessingId, process_any
from rs_workflows.utils.prefect import get_logger


@flow(name="process-s3-l2-olci-systematic")
async def process_s3l2olci_systematic(payload: dict[str, Any]):
    """
    # Process SYSTEMATIC S3 L2 OLCI

    This flow is started from automation.
    It get the very first input from the 'event' that triggers the flow.
    Then it prepares the S3 L2 OLCI processing inputs.
    Finally it forwards to `process-any` with workflow set to "systematic".

    ---
    ## Workflow Steps

    1. *compute input products* — retrieve the stac items `S03OLCEFR` from the event that trigger this flow.
    2. *compute external variables* — external variables are extracted from the first item properties.
    3. *start the processing* — calls the flow `process-any`
    ---
    ## Parameters
    Parameter | Type | Default | Description |
    |---|---|---|---|
    | `payload` | dict[str, Any] | *required* | The event that trigger this flow. |

    ---
    """

    logger = get_logger()
    logger.info(f"Flow started from event with payload {payload}.")

    owner_id: str = payload["owner_id"]
    env_args: FlowEnvArgs = FlowEnvArgs(owner_id=owner_id)

    flow_env: FlowEnv = FlowEnv(env_args)

    with flow_env.start_span(__name__, f"process_systematic_s3l2olci_input"):

        input_products: list[FlowInputProduct] = []
        input_products.append(
            {
                "name": f"{OLCEFR_DEFAULT_BASE_NAME}",
                "item_id": payload["product"]["id"],
                "collection_name": payload["product"]["collection"],
            },
        )

        logger.info(
            "input_products:\n%s",
            json.dumps(input_products, indent=2, ensure_ascii=False),
        )

        external_variables = compute_s3l2olci_external_variables(
            payload["product"]["properties"]["start_datetime"],
            payload["product"]["properties"]["end_datetime"],
        )

        logger.info(
            "external_variables:\n%s",
            json.dumps(external_variables, indent=2, ensure_ascii=False),
        )

        await process_any(
            env_args=env_args,
            input_products=input_products,
            external_variables=external_variables,
            processing_id=ProcessingId.S3_OLCI_L2,
            workflow=WorkflowType.SYSTEMATIC,
        )
