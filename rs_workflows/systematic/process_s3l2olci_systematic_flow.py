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

from typing import Any

from prefect import flow

from rs_workflows.on_demand.common.product_processing import process_event_product
from rs_workflows.on_demand.sentinel3.process_s3l2olci_ondemand_flow import (
    process_s3l2olci_product,
)
from rs_workflows.utils.dpr import ProcessingId


@flow(name="process-s3-l2-olci-systematic")
async def process_s3l2olci_systematic(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    # Process SYSTEMATIC S3 L2 OLCI

    This flow is started from automation.
    It get the very first input from the 'event' that triggers the flow.
    Then it prepares the S3 L2 OLCI processing inputs.
    Finally it runs the processor with workflow set to "systematic".

    ---
    ## Workflow Steps

    1. *load the settings* — reads the `s3l2olci-default-setting` Prefect variable and checks that the Dask clusters
       are running.
    2. *compute input products* — retrieve the stac items `S03OLCEFR` from the event that trigger this flow.
    3. *compute external variables* — external variables are extracted from the first item properties.
    4. *start the processing* — calls the flow `process-generic`, then emits the events of the generated products.
    5. *request the quicklooks* — emits the `rs-python.s3-l2.quicklook-inputs-ready` event with the generated products.
    ---
    ## Parameters
    Parameter | Type | Default | Description |
    |---|---|---|---|
    | `payload` | dict[str, Any] | *required* | Payload of the `product.S03OLCEFR.generated` event. |

    ---
    """
    return await process_event_product(payload, ProcessingId.S3_OLCI_L2, process_s3l2olci_product)
