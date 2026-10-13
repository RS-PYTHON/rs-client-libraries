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

"""Process systematic S3 OLCI L1 flow implementation"""

import datetime
import json

from prefect import flow, task
from pystac import ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_client.stac.prip_client import PripClient
from rs_common.utils import create_valcover_filter
from rs_workflows.catalog_flow import catalog_search_task
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnv,
    FlowEnvArgs,
    FlowGeneratedProduct,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    RetryConfig,
    WorkflowType,
)
from rs_workflows.on_demand.adf.convert_adf_set import compute_cql2
from rs_workflows.process_any_flow import ProcessingId, process_any
from rs_workflows.staging_flow import staging_task
from rs_workflows.utils.prefect import get_logger


@flow(name="process-s3l1olci-systematic")
async def process_s3l1olci_systematic():
    """
    This flow compute input and external variable from an event.
    Then it forwards to process-any  with workflow  set to "systematic".
    """
    logger = get_logger()

    # single_input_product.collection_name
    # single_input_product.item_id
