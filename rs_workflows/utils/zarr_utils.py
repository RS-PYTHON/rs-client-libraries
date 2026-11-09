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

"""Format-agnostic (Zarr v2 and v3) helpers to access Zarr products."""

import os
from typing import Any
from urllib.parse import urlparse

import zarr
from pystac import Asset, Item
from zarr.errors import ContainsArrayError

# Media type of the STAC assets of the Zarr products
ZARR_MEDIA_TYPE = "application/vnd+zarr"


def is_zarr_asset(asset: Asset) -> bool:
    """Return True if the STAC asset is a Zarr product: Zarr media type or .zarr extension."""
    return asset.media_type == ZARR_MEDIA_TYPE or asset.href.rstrip("/").lower().endswith(".zarr")


def is_zarr_item(item: Item) -> bool:
    """Return True if the first asset of the STAC item, that is read by the processors, is a Zarr product."""
    first_asset = next(iter(item.assets.values()), None)
    return first_asset is not None and is_zarr_asset(first_asset)


def check_zarr_input(item: Item, input_name: str) -> None:
    """
    Check that the STAC item of an input product that the processor reads in the Zarr format, is in the Zarr format.

    Without this check, the processor cannot open the product, e.g. in the legacy SAFE format, and fails with an
    unclear error, e.g. EOPF ignores the optional input products that it cannot open. The reader (engine) of the
    processor tasktable cannot be used to know if the product is read in the Zarr format: cpm_zarr is also given for
    raw inputs, e.g. the CADU chunks.

    Args:
        item: STAC item of the input product.
        input_name: Name of the input product in the processor tasktable.

    Raises:
        ValueError: If the STAC item is not in the Zarr format.
    """
    if is_zarr_item(item):
        return
    first_asset = next(iter(item.assets.values()), None)
    raise ValueError(
        f"The input product {input_name!r} is read in the Zarr format by the processor, but the STAC item "
        f"{item.id!r} of the {item.collection_id!r} collection is not in the Zarr format "
        f"(first asset: {first_asset.href if first_asset else None!r}). Give the ID of a product in the Zarr format, "
        "e.g. converted from the legacy SAFE format by the convert-safe flow.",
    )


def get_zarr_storage_options(zarr_uri: str) -> dict[str, Any] | None:
    """Return the fsspec storage options needed to access the given Zarr store, if any."""
    if urlparse(zarr_uri).scheme.lower() != "s3":
        return None
    # Use the same object-storage credentials as the other workflow S3 accesses.
    return {
        "key": os.environ["S3_ACCESSKEY"],
        "secret": os.environ["S3_SECRETKEY"],
        "client_kwargs": {
            "endpoint_url": os.environ["S3_ENDPOINT"],
            "region_name": os.environ["S3_REGION"],
        },
    }


def open_zarr_group(zarr_location: str) -> zarr.Group:
    """Open a Zarr group in read-only mode, the Zarr format being detected by the zarr library.

    Args:
        zarr_location: Local path or S3 URI of the Zarr store root.
    """
    zarr_location = zarr_location.rstrip("/")
    return zarr.open_group(
        store=zarr_location,
        mode="r",
        zarr_format=None,
        storage_options=get_zarr_storage_options(zarr_location),
        # Only the root group metadata is needed, so don't require/read consolidated metadata.
        use_consolidated=False,
    )


def is_zarr_group(zarr_location: str) -> bool:
    """Return True if the given location is a Zarr group, whatever its Zarr format.

    Args:
        zarr_location: Local path or S3 URI of a potential Zarr store root.
    """
    try:
        open_zarr_group(zarr_location)
    # GroupNotFoundError is a FileNotFoundError. A Zarr array is not a Zarr group either.
    except (FileNotFoundError, NotADirectoryError, ContainsArrayError):
        return False
    return True


def read_zarr_attributes(zarr_location: str) -> dict[str, Any]:
    """Read the root group attributes of a Zarr store, whatever its Zarr format.

    Args:
        zarr_location: Local path or S3 URI of the Zarr store root.
    """
    return dict(open_zarr_group(zarr_location).attrs)
