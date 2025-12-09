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
import os
from datetime import datetime

from prefect import flow, get_run_logger, task
from pystac import Asset, Item, ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.payload_template import PayloadSchema  # , OutputProduct

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
    catalog_collection_identifier: list[dict],
    payload_file: PayloadSchema,
    items: list[dict],
):
    """
    Publish items to the catalog

    Args:
        env: Prefect flow environment
        catalog_collection_identifier: Catalog collection identifier where the items are staged
        payload_file: Payload file configuration for the dpr processor
        items: Items to publish, as STAC dicts
    """
    logger = get_run_logger()
    flow_env = FlowEnv(env)

    def extract_product_type_and_collection(cci: dict):
        cci_tuple = next(iter(cci.values()))
        if isinstance(cci_tuple, tuple):
            return cci_tuple[0], cci_tuple[1]
        return cci_tuple, cci_tuple

    def find_matching_output_product(output_dir: str):
        if not payload_file.io:
            return None

        for output_ps in payload_file.io.output_products:
            if output_dir == output_ps.id:
                return output_ps
        return None

    def build_item(feature_dict: dict) -> Item:
        sd = feature_dict["stac_discovery"]
        return Item(
            id=sd["id"],
            geometry=sd["geometry"],
            bbox=sd["bbox"],
            datetime=datetime.fromisoformat(sd["properties"]["datetime"]),
            properties=sd["properties"],
        )

    def build_asset(path: str, title: str) -> Asset:
        return Asset(
            href=path,
            title=title,
            media_type="application/vnd+zarr",
            roles=["data", "metadata"],
            extra_fields={
                "file:local_path": path,
                "auth:ref": "should be filled thanks to story RSPY-280",
            },
        )

    catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()

    with flow_env.start_span(__name__, "publish-to-catalog"):
        for feature_dict in items:
            try:
                sd_props = feature_dict["stac_discovery"]["properties"]
                feature_product_type = sd_props["product:type"].upper()

                for cci in catalog_collection_identifier:
                    product_type, collection = extract_product_type_and_collection(cci)

                    if feature_product_type != product_type.upper():
                        continue

                    output_dir = next(iter(cci))
                    output_ps = find_matching_output_product(output_dir)
                    if not output_ps:
                        continue

                    item = build_item(feature_dict)

                    title = f"{item.id}.zarr"
                    output_path = os.path.join(output_ps.path, title)

                    item.assets = {title: build_asset(output_path, title)}
                    catalog_client.add_item(collection, item)

            except Exception as e:
                raise RuntimeError(f"Exception while publishing: {json.dumps(feature_dict, indent=2)}") from e

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
