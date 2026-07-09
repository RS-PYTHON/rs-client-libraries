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

"""Helper task to interact with the rs-catalog."""

from datetime import datetime, timezone

from prefect import get_run_logger, task
from pystac import Item, ItemCollection

from rs_workflows.catalog_flow import publish
from rs_workflows.flow_utils import (
    DprProcessedItemMetadata,
    FlowEnv,
    FlowGeneratedProduct,
)


@task(name="Retrieve rs-catalog item from collection")
async def get_single_catalog_item(flow_env: FlowEnv, item_id: str, collections: list[str]) -> Item | None:
    """
    Get an item from a set of rs-catalog collections
    """
    logger = get_run_logger()
    result: Item | None = None

    # Try to retrieve the item in the collections
    item_collection: ItemCollection | None = await get_catalog_items(flow_env, [item_id], collections)

    count = 0
    if item_collection is not None:
        count = len(item_collection.items)
    if count == 1:
        # One item was found on the rs-catalog
        logger.info(
            f"✅ The STAC item 🧊 '{item_id}' has been found on the rs-catalog collections {', '.join(collections)}.",
        )
        if item_collection is not None:
            result = item_collection.items[0]
    else:
        logger.warning(
            f"⚠️ The STAC item 🧊 '{item_id}' was not found on the rs-catalog collections {', '.join(collections)}.",
        )

    return result


@task(name="Retrieve rs-catalog items from collections")
async def get_catalog_items(flow_env: FlowEnv, item_ids: list[str], collections: list[str]) -> ItemCollection | None:
    """
    Get items from a set of rs-catalog collections
    """
    get_run_logger().info(
        f"Search items {', '.join(item_ids)} in the collections {', '.join(collections)} from the rs-catalog.",
    )
    size = len(item_ids)
    return flow_env.rs_client.get_catalog_client().search(
        method="POST",
        collections=collections,
        ids=item_ids,
        max_items=size,
        limit=size,
    )


def is_evicted(item: Item) -> tuple[bool, datetime | None]:
    """
    Check if the item is evicted.
    """
    eviction_date_str: str = ""

    for asset in item.assets.values():
        if "eviction_datetime" in asset.extra_fields:
            eviction_date_str = asset.extra_fields["eviction_datetime"]
            break

    if eviction_date_str:
        eviction_date = datetime.fromisoformat(eviction_date_str.replace("Z", "+00:00"))
        return eviction_date <= datetime.now(timezone.utc), eviction_date

    return False, None


def is_published(item: Item) -> bool:
    """
    Check if the item is published.
    """
    published_date_str = item.properties.get("published")

    if published_date_str:
        published_date = datetime.fromisoformat(published_date_str.replace("Z", "+00:00"))
        return published_date <= datetime.now(timezone.utc)

    return False


@task
async def published_stac_item(flow_env: FlowEnv, item: Item, collection_name: str) -> None:
    """ "
    Push a STAC item into the rs-catalog.
    """
    logger = get_run_logger()
    logger.info(f"The STAC item 🧊 '{item.id}' will be published on the collection '{collection_name}'.")
    items_metadata: list[DprProcessedItemMetadata] = []
    publish_mapping: list[FlowGeneratedProduct] = []

    items_metadata.append(
        DprProcessedItemMetadata(
            output_product_id=item.id,
            product_type=item.properties.get("product:type"),
            stac_item=item,
        ),
    )

    publish_mapping.append(
        FlowGeneratedProduct(
            name=item.id,
            product_type=str(item.properties.get("product:type")),
            collection_name=collection_name,
        ),
    )

    logger.debug(f"items_metadata = {items_metadata}")
    logger.debug(f"publish_mapping = {publish_mapping}")
    await publish(
        flow_env.serialize(),
        publish_mapping,
        items_metadata,
    )
