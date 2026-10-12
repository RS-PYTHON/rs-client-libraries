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

from prefect import flow, task
from pystac import ItemCollection

from rs_client.stac.prip_client import PripClient
from rs_common.utils import create_valcover_filter
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, RetryConfig
from rs_workflows.staging_flow import staging_task
from rs_workflows.utils.prefect import get_logger
from rs_workflows.process_any_flow import ProcessingId, process_any
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
from rs_workflows.on_demand.adf.convert_adf_set import compute_cql2
from rs_workflows.catalog_flow import catalog_search_task
from rs_client.stac.catalog_client import CatalogClient
import json



@flow(name="process-s3l1olci-ondemand")
async def process_s3l1olci_ondemand(
    env_args: FlowEnvArgs,
    single_input_product: FlowInputProduct
):
    """
    This flow compute other input and external variable from a single input.
    Then it forwards to process-any  with workflow  set to "on-demand".
    The "name" from FlowInputProduct will be ignored.
    """
    logger = get_logger()

    #single_input_product.collection_name
    #single_input_product.item_id

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, f"process_s3l1olci_ondemand"):
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        stac_item = catalog_client.get_item(single_input_product.collection_name, single_input_product.item_id)
        if stac_item is None:
            raise RuntimeError(f"❌ STAC item '{single_input_product.item_id}' not found in collection '{single_input_product.collection_name}'.")

        print(json.dumps(stac_item, indent=2, ensure_ascii=False))

        logger.info("STAC Item:\n%s",
                    json.dumps(stac_item.to_dict(), indent=2, ensure_ascii=False)
                    )
        logger.info("STAC Item:\n%s",
                    json.dumps(stac_item.to_dict()["properties"], indent=2, ensure_ascii=False)
                    )

        #logger.info(f"Start Catalog search: {catalog_cql2}")
        #cql2_filter_without_date = compute_cql2(cql2_query_name, dta, dtb, satellite)


    

    

