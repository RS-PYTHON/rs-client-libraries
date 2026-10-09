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

"""Process systematic and ondemand flow implementation"""

import datetime

from prefect import flow, task
from pystac import ItemCollection

from rs_client.stac.prip_client import PripClient
from rs_common.utils import create_valcover_filter
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, RetryConfig
from rs_workflows.staging_flow import staging_task
from rs_workflows.utils.prefect import get_logger
from typing import Any
from enum import Enum


from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnvArgs,
    FlowGeneratedProduct,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)

class ProcessingId(str, Enum):
    """
    Processing Identifier : usefull to determine configuration file pattern.
    """
    S3_L0 = "s3l0"
    S3_OLCI_L1 = "s3l1olci"
    S3_OLCI_L2 = "s3l2olci"
    

    

@flow(name="process-any")
async def process_any(
    env: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    processing_id: ProcessingId,
    workflow: WorkflowType = WorkflowType.ON_DEMAND,
):
    """
    # Import ADF from Object Storage

    Imports a set of *ADF files* into the *rs-catalog* from an object storage bucket.

    ---

    ## Workflow Steps

    1. *Download* — Retrieves the compressed ADF files from the object storage
    2. *Decompress* — Extracts the archive contents
    3. *Filter* — Selects only the relevant ADF files to import
    4. *Publish* — Pushes the selected files to the rs-catalog as STAC items

    ---

    ## Parameters

    | Parameter | Type | Default | Description |
    |---|---|---|---|
    | `configuration` | `dict` | *required* | JSON configuration (see format below) |
    | `owner` | `str` | `copernicus` | Name of the user triggering the flow |
    | `obs_id` | `str` | `PUBLICATION` | Object storage identifier for credentials |
    | `rehearsal_mode` | `bool` | `True` | If `True`, STAC items are *not* published |

    > *Note:* The collection where ADF files are published is derived from `product:type` by default,
    > but can be *overridden* via the configuration.

    ---

    ## Configuration Format example

    ```json
    {
        "input": {
            "bucket": "rs-f1-archive",
            "path": "S3_OL1/3.23/S3_OL1_3.23_2023-06-20/Ancillary_Data",
            "files": ["S3_OL1_3.23_2023-06-20_ADF.tar.gz"],
            "extract_pattern": "S3__*.tgz|S3A_*.tgz"
        },
        "output": {
            "additional_path": "",
            "collection": "adf-olci-baseline-3-23",
            "override": False
        }
    }
    ```
    ---
    """
    logger = get_logger()
    prefect_variable_name:str = f'{processing_id}-default-setting'
    logger.info(f"Read prefect variable named '{prefect_variable_name}'")

    
