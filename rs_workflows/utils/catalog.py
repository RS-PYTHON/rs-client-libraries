# Copyright 2023-2026 Airbus
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

from rs_workflows.flow_utils import FlowEnv
from prefect import get_run_logger, task
import os
import json
from pystac import Item, ItemCollection
from rs_client.stac.catalog_client import CatalogClient


@task(name="Retrieve rs-catalog item from collection")
async def get_single_catalog_item(
    flow_env:FlowEnv,
    id:str,
    collections:list[str]
)->Item:
    
    logger = get_run_logger()
    result: Item = None
    
    # Try to retrieve the session on the collection
    catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
    logger.info(f"Search item {id} on the collections {', '.join(collections)} from the  rs-catalog.")
    item_collection: ItemCollection = catalog_client.search(
        method="POST",
        collections=collections,
        ids=[id],
        limit=1,
    )
    
    if item_collection is not None:
        count = len(item_collection.items)
    if count == 1:
        # One  item  was found on the rs-catalog
        logger.info(
            f"✔️ The STAC item 🧊 '{id}' has been found on the rs-catalog collections {', '.join(collections)}.",
        )
        result = item_collection.items[0]
    else:
        logger.warning(
            f"❌ The STAC item 🧊 '{id}' was not found on the rs-catalog collections {', '.join(collections)}.",
        )
        
    return result
