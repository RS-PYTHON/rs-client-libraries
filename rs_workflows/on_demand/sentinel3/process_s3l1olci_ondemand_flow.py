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
from pystac import ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_client.stac.prip_client import PripClient
from rs_common.utils import create_valcover_filter, strftime_millis
from rs_workflows.adf_flow import substitute_values
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


@flow(name="process-s3l1olci-ondemand")
async def process_s3l1olci_ondemand(env_args: FlowEnvArgs, single_input_product: FlowInputProduct):
    """
    This flow compute other input and external variable from a single input.
    Then it forwards to process-any  with workflow  set to "on-demand".
    The "name" from FlowInputProduct will be ignored.
    """
    logger = get_logger()

    # single_input_product.collection_name
    # single_input_product.item_id

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, f"process_s3l1olci_ondemand"):
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        stac_item = catalog_client.get_item(single_input_product.collection_name, single_input_product.item_id)
        if stac_item is None:
            raise RuntimeError(
                f"❌ STAC item '{single_input_product.item_id}' not found in collection '{single_input_product.collection_name}'.",
            )

        logger.info(
            "STAC Item properties:\n%s",
            json.dumps(stac_item.to_dict()["properties"], indent=2, ensure_ascii=False),
        )
        item: dict = stac_item.to_dict()

        satellite = item["properties"]["platform"]
        start_datetime_txt = item["properties"]["start_datetime"]
        end_datetime_txt = item["properties"]["end_datetime"]
        product_type = item["properties"]["product:type"]

        # We will extract the product duration in seconds
        start_datetime = datetime.datetime.fromisoformat(start_datetime_txt)
        end_datetime = datetime.datetime.fromisoformat(end_datetime_txt)
        duration_seconds = (end_datetime - start_datetime).total_seconds()

        logger.info("🧹 Computing cql2_filter")
        included_in_interval = compute_cql2("LatestIncludedWithSatellite", 1, 1, satellite)
        cql2_filter = substitute_values(
            included_in_interval,
            {
                "start_datetime": strftime_millis(start_datetime),
                "end_datetime": strftime_millis(end_datetime),
                "product_type": product_type,
                "satellite": satellite,
            },
        )
        logger.info(
            "stac_filter:\n%s",
            json.dumps(cql2_filter.get("filter"), indent=2, ensure_ascii=False),
        )
        logger.info(
            "stac_sortby:\n%s",
            json.dumps(cql2_filter.get("sortby"), indent=2, ensure_ascii=False),
        )
        logger.info(
            "stac_limit:\n%s",
            json.dumps(cql2_filter.get("limit"), indent=2, ensure_ascii=False),
        )

        found = catalog_client.search(
            datetime=start_datetime,
            method="POST",
            stac_filter=cql2_filter.get("filter"),
            max_items=cql2_filter.get("limit"),
            sortby=cql2_filter.get("sortby"),
            collections=[single_input_product.collection_name],
        )

        logger.info(
            "STAC Item properties:\n%s",
            json.dumps(found.to_dict(), indent=2, ensure_ascii=False),
        )
