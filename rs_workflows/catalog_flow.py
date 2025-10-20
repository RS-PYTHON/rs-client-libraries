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

from prefect import flow, get_run_logger, task
from pystac import Asset, Item, ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

#################
# Catalog flows #
#################


@flow(name="Catalog search")
async def catalog_search(
    env: FlowEnvArgs,
    catalog_cql2: dict,
    error_if_empty: bool = False,
) -> ItemCollection | None:
    """
    Search Catalog items.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        catalog_cql2: CQL2 filter.
        error_if_empty: Raise a ValueError if the results are empty.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "catalog-search"):

        logger.info("Start Catalog search")
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        found = catalog_client.search(
            method="POST",
            stac_filter=catalog_cql2.get("filter"),
            max_items=catalog_cql2.get("limit"),
            sortby=catalog_cql2.get("sortby"),
        )
        if (not found) and error_if_empty:
            raise ValueError(
                f"No Catalog item found for CQL2 filter: {json.dumps(catalog_cql2, indent=2)}",
            )
        logger.info(f"Catalog search found {len(found)} results: {found}")  # type: ignore
        return found


#################
# Catalog tasks #
#################


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


@task(name="Catalog search")
async def catalog_search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await catalog_search.fn(*args, **kwargs)
