# Copyright 2025 CS Group
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

"""Catalog flow implementation"""

import json
from datetime import datetime

from prefect import get_run_logger, task
from pystac import Asset, Item

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@task(name="Publish to catalog")
async def publish(
    env: FlowEnvArgs,
    catalog_collection_identifier: str,
    items: list[dict],
    s3_output_data: str,
):
    """
    Publish items to the catalog

    Args:
        env: Prefect flow environment
        catalog_collection_identifier: Catalog collection identifier where the items are staged
        items: Items to publish, as STAC dicts
        s3_output_data: S3 bucket location of the output processed products.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "publish-to-catalog"):
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        for feature_dict in items:
            try:
                item = Item(
                    id=feature_dict["stac_discovery"]["id"],
                    geometry=feature_dict["stac_discovery"]["geometry"],
                    bbox=feature_dict["stac_discovery"]["bbox"],
                    datetime=datetime.fromisoformat(
                        feature_dict["stac_discovery"]["properties"]["datetime"],
                    ),
                    properties=feature_dict["stac_discovery"]["properties"],
                )
                asset = Asset(href=f"{s3_output_data}/{item.id}.zarr.zip")
                item.assets = {f"{item.id}.zarr.zip": asset}
                catalog_client.add_item(catalog_collection_identifier, item)
            except Exception as e:
                raise RuntimeError(f"Exception while publishing: {json.dumps(feature_dict, indent=2)}") from e

    collections = catalog_client.get_collections()
    logger.info("\nCollections response:")
    for collection in collections:
        logger.info(f"ID: {collection.id}, Title: {collection.title}")

    logger.info("End catalog publishing")
