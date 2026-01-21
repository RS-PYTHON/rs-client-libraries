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
from pystac import Item, ItemCollection

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
    target_collections,
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

    # Normalize target_collections into a single dict
    collections = (
        target_collections
        if isinstance(target_collections, dict)
        else {k: v for d in target_collections for k, v in d.items()}
    )

    catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()

    with flow_env.start_span(__name__, "publish-to-catalog"):
        for item in items:
            try:
                # Extract product type from STAC item
                product_type = (
                    item.properties["product:type"] if isinstance(item, Item) else item["properties"]["product:type"]
                )
                # Resolve destination collection
                target_collection = resolve_collection(product_type, collections)

                logger.info(
                    "Writing product %s to %s",
                    item.id if isinstance(item, Item) else item["id"],
                    target_collection,
                )
                # Publish item to catalog
                catalog_client.add_item(target_collection, item)

            except Exception as e:
                # Re-raise with full item context for easier debugging
                item = item.to_dict() if hasattr(item, "to_dict") else item
                raise RuntimeError(
                    f"Exception while publishing item: {json.dumps(item, indent=2)}",
                ) from e

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


@task(name="Get catalog item")
async def get_item(
    env: FlowEnvArgs,
    target_collection,
    item,
):
    """
    Get a catalog item by its ID.
    """
    flow_env = FlowEnv(env)
    catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
    return catalog_client.get_item(target_collection, item)


def resolve_collection(product_type: str, collections: dict) -> str:
    """
    Resolve the target catalog collection for a given product type.

    Lookup order:
    1. Exact match on product_type
    2. Wildcard fallback ("*"), if defined

    The function also validates that the resolved collection_id matches
    the product_type (case-insensitive), to catch invalid LUT definitions.

    :param product_type: STAC product type (e.g. "S03MWRL0_")
    :param collections: Mapping of product_type -> (collection_id, target_collection)
    :return: Target collection name where the item should be published
    :raises ValueError: If the product type cannot be resolved or LUT is inconsistent
    """
    # Try exact match first, then fallback to wildcard
    collection_id, target_collection = collections.get(product_type, collections.get("*", (None, None)))

    # No match found (neither exact nor wildcard)
    if not collection_id:
        raise ValueError(f"Product type unknown: {product_type}")

    # Sanity check: product type must match collection_id
    if product_type.casefold() != collection_id.casefold():
        raise ValueError(f"Product type unknown: {product_type}")

    return target_collection
