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

from prefect import flow, get_run_logger, task
from pystac import ItemCollection

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
    target_collections: dict,
    items,
):
    """
    Publish items to the catalog

    Args:
        env: Prefect flow environment
        collection: Catalog collection identifier where the items are published
        items: Items to publish, as STAC dicts or pystac.Items
    """
    logger = get_run_logger()
    flow_env = FlowEnv(env)

    catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()

    with flow_env.start_span(__name__, "publish-to-catalog"):
        for item in items:
            try:
                logger.info(item.to_dict())
                collection = "TEST_FLOW_OUTPUTS"
                catalog_client.add_item(collection, item)
            except Exception as e:
                raise RuntimeError(f"Exception while publishing: {json.dumps(item.to_dict(), indent=2)}") from e

    # list collections for logging
    collections = catalog_client.get_collections()
    logger.info("\nCollections response:")
    for collection in collections:
        logger.info(f"ID: {collection.id}, Title: {collection.title}")

    logger.info("End catalog publishing")


@task(name="Catalog search")
async def catalog_search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await catalog_search.fn(*args, **kwargs)
