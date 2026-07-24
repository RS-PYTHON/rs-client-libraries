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

"""Shared configuration and product-mapping helpers for Sentinel-3 processing."""

from collections.abc import Awaitable
from typing import Any, cast

from prefect.variables import Variable
from pydantic import BaseModel, Field

from rs_workflows.on_demand.common.types import S3_PROCESSING_CONFIGURATION


class S3ProcessingOrchestrationSettings(BaseModel):
    """Environment-specific deployment and collection names for the S3 chain."""

    cadip_staging_deployment: str = Field(min_length=1)
    s3_l0_deployment: str = Field(min_length=1)
    s3_l1_olci_deployment: str = Field(min_length=1)
    cadip_collection: str = Field(min_length=1)
    staging_catalog_collection: str = Field(min_length=1)
    s3_l0_output_collection: str = Field(min_length=1)


async def read_s3_orchestration_settings() -> S3ProcessingOrchestrationSettings:
    """Load and validate the ``orchestration`` section of the unified S3 variable."""
    raw_settings = await cast(Awaitable[Any], Variable.get(S3_PROCESSING_CONFIGURATION, default={}))
    if not isinstance(raw_settings, dict):
        raise ValueError(f"Prefect variable {S3_PROCESSING_CONFIGURATION!r} must contain a dictionary")

    return S3ProcessingOrchestrationSettings.model_validate(raw_settings.get("orchestration"))


def build_olci_l1_input_products(
    l0_products: list[dict[str, Any]],
    input_collection: str,
) -> list[dict[str, str]]:
    """Convert full STAC items or compact event products into OLCI L1 inputs."""

    normalized_products: list[tuple[str, str]] = []
    for product in l0_products:
        if "id" in product and "properties" in product:
            product_type = product["properties"]["product:type"]
            item_id = product["id"]
        elif len(product) == 1:
            product_type, item_id = next(iter(product.items()))
        else:
            raise ValueError(
                "Invalid L0 product: expected a STAC item or a single "
                "{product_type: item_id} entry",
            )
        if not isinstance(product_type, str) or not isinstance(item_id, str):
            raise ValueError("Invalid L0 product: product type and item ID must be strings")
        normalized_products.append((product_type, item_id))

    olci_item_ids = [
        item_id
        for product_type, item_id in normalized_products
        if product_type == "S03OLCL0_"
    ]
    nav_item_ids = [
        item_id
        for product_type, item_id in normalized_products
        if product_type == "S03NATL0_"
    ]

    if len(olci_item_ids) < 3:
        raise ValueError(f"Expected at least 3 S03OLCL0_ products from L0, found {len(olci_item_ids)}")
    if not nav_item_ids:
        raise ValueError("Expected at least 1 S03NATL0_ product from L0, found 0")

    input_products = [
        {
            "name": f"S3OLCIL0_{index}",
            "item_id": item_id,
            "collection_name": input_collection,
        }
        for index, item_id in enumerate(olci_item_ids[:3], start=1)
    ]
    input_products.append(
        {
            "name": "S3NAVL0_1",
            "item_id": nav_item_ids[0],
            "collection_name": input_collection,
        },
    )
    return input_products
