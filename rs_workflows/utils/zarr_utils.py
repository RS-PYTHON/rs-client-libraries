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

from rs_workflows.utils.prefect import get_logger

# Group-level metadata files: "zarr.json" for Zarr v3, ".zattrs"/".zgroup" for Zarr v2.
ZARR_METADATA_FILES = ("zarr.json", ".zattrs", ".zgroup")


def get_zarr_root(zarr_location: str) -> str:
    """Return the Zarr store root from either the store root or one of its root metadata files."""
    zarr_location = zarr_location.rstrip("/")
    for metadata_file in ZARR_METADATA_FILES:
        if zarr_location.endswith(f"/{metadata_file}"):
            return zarr_location.removesuffix(f"/{metadata_file}")
    return zarr_location


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
    """Open a Zarr v2 or v3 group in read-only mode, the format being detected automatically.

    Args:
        zarr_location: Local path or S3 URI of the Zarr store root (or of one of its root metadata files).
    """
    zarr_uri = get_zarr_root(str(zarr_location))
    return zarr.open_group(
        store=zarr_uri,
        mode="r",
        zarr_format=None,
        storage_options=get_zarr_storage_options(zarr_uri),
        # Only the root group metadata is needed, so don't require/read consolidated metadata.
        use_consolidated=False,
    )


def read_zarr_attributes(zarr_location: str) -> dict[str, Any]:
    """Read the root group attributes of a Zarr v2 (.zattrs) or v3 (zarr.json) store.

    Args:
        zarr_location: Local path or S3 URI of the Zarr store root (or of one of its root metadata files).
    """
    logger = get_logger()
    logger.info("===== COUCOU DE ZARR UTILS =====")
    return dict(open_zarr_group(zarr_location).attrs)
