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

"""Code shared by the Sentinel-3 OLCI quicklook flows."""

import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from PIL import Image
from prefect import get_run_logger
from rasterio.control import GroundControlPoint
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject
from sentineltoolbox.api import S3BucketCredentials, open_datatree

from rs_common import prefect_utils
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

JPEG_MEDIA_TYPE = "image/jpeg"
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"
ZARR_MEDIA_TYPE = "application/vnd+zarr"
PROJECTION_EXTENSION = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
# Use one browser-friendly CRS for every georeferenced quicklook.
QUICKLOOK_CRS = "EPSG:4326"
# Downsample the source arrays by this factor to keep quicklook generation fast and lightweight.
QUICKLOOK_DOWNSAMPLING_STEP = 4


def normalize_channel(values):
    """Scale and clip a channel to [0, 1] using its 2nd and 98th percentiles."""
    vmin, vmax = np.nanpercentile(values, [2, 98])
    # Resulting NaNs are converted to zero intensity when the caller builds the uint8 image.
    return np.clip((values - vmin) / (vmax - vmin), 0, 1)


def get_zarr_href(item) -> str:
    """Return the S3 href of the item's Zarr asset."""
    # Accept either the declared Zarr media type or a conventional .zarr suffix.
    for asset in item.assets.values():
        href = asset.href.rstrip("/")
        if asset.media_type == ZARR_MEDIA_TYPE or href.lower().endswith(".zarr"):
            if not href.lower().startswith("s3://"):
                raise ValueError(f"The Zarr asset must use an S3 href, found: {href!r}")
            return href
    raise ValueError(f"Catalog item {item.id!r} has no Zarr asset")


def select_downsampled_geolocation(measurements):
    """Return the row/column selection and the downsampled longitude/latitude arrays."""
    longitude = measurements.longitude
    latitude = measurements.latitude
    row_dimension, column_dimension = longitude.dims

    # Rows corrupted by the S3-OLCI processor have zero longitude and latitude ("Null Island").
    first_longitude = longitude.isel({column_dimension: 0}).values
    first_latitude = latitude.isel({column_dimension: 0}).values
    good_rows = ~((first_longitude == 0) & (first_latitude == 0))
    if not good_rows.any():
        raise ValueError("The OLCI product contains no valid geolocation rows")

    # Select before loading values so full-resolution EFR arrays stay out of memory.
    selected_rows = np.flatnonzero(good_rows)[::QUICKLOOK_DOWNSAMPLING_STEP]
    selection = {
        row_dimension: selected_rows,
        column_dimension: slice(None, None, QUICKLOOK_DOWNSAMPLING_STEP),
    }
    lon = longitude.isel(selection).values
    lat = latitude.isel(selection).values
    return selection, lon, lat


def write_georeferenced_cog(cog_path: Path, lon, lat, rgb, visible=None) -> None:
    """Write the georeferenced COG; when ``visible`` is given, pixels without data are declared nodata (0)."""
    # Ignore any remaining invalid coordinates when defining the COG bounds.
    valid_geo = np.isfinite(lon) & np.isfinite(lat)
    if not valid_geo.any():
        raise ValueError("The OLCI product contains no valid coordinates")

    # Georeference the swath directly in longitude/latitude for broad map-client support.
    x, y = lon, lat
    xmin, xmax = float(np.min(x[valid_geo])), float(np.max(x[valid_geo]))
    ymin, ymax = float(np.min(y[valid_geo])), float(np.max(y[valid_geo]))

    gcp_rows = np.linspace(0, lon.shape[0] - 1, min(20, lon.shape[0]), dtype=int)
    gcp_cols = np.linspace(0, lon.shape[1] - 1, min(20, lon.shape[1]), dtype=int)
    # Build sparse control points from the swath grid in geographic coordinates.
    gcps = [
        GroundControlPoint(row=int(row), col=int(col), x=float(x[row, col]), y=float(y[row, col]))
        for row in gcp_rows
        for col in gcp_cols
        if valid_geo[row, col]
    ]
    if not gcps:
        raise ValueError("Could not build ground control points for the OLCI product")

    # Keep the downsampled source dimensions on the regular geographic grid.
    dst_width = lon.shape[1]
    dst_height = lon.shape[0]
    dst_transform = from_bounds(xmin, ymin, xmax, ymax, dst_width, dst_height)

    # Warp through the control points and write the georeferenced image directly as a COG.
    with rasterio.open(
        cog_path,
        "w",
        driver="COG",
        height=dst_height,
        width=dst_width,
        count=3,
        dtype="uint8",
        crs=QUICKLOOK_CRS,
        transform=dst_transform,
        compress="deflate",
        # Only the masked (L2) case declares nodata; the unmasked call keeps its original arguments.
        **({} if visible is None else {"nodata": 0}),
    ) as destination:
        if visible is None:
            reproject(
                source=np.moveaxis(rgb, 2, 0),
                destination=rasterio.band(destination, [1, 2, 3]),
                gcps=gcps,
                # GCP coordinates are already expressed in the destination CRS units.
                src_crs=QUICKLOOK_CRS,
                dst_transform=dst_transform,
                dst_crs=QUICKLOOK_CRS,
                resampling=Resampling.bilinear,
            )
        else:
            # Warp into arrays first: pixels without data must be zeroed before writing the COG.
            warped = np.zeros((3, dst_height, dst_width), dtype="uint8")
            reproject(
                source=np.moveaxis(rgb, 2, 0),
                destination=warped,
                gcps=gcps,
                src_crs=QUICKLOOK_CRS,
                dst_transform=dst_transform,
                dst_crs=QUICKLOOK_CRS,
                resampling=Resampling.bilinear,
            )
            # Warp the data availability without interpolation, so it stays strictly 0 or 1.
            warped_visible = np.zeros((dst_height, dst_width), dtype="uint8")
            reproject(
                source=visible.astype("uint8"),
                destination=warped_visible,
                gcps=gcps,
                src_crs=QUICKLOOK_CRS,
                dst_transform=dst_transform,
                dst_crs=QUICKLOOK_CRS,
                resampling=Resampling.nearest,
            )
            # Bilinear blending leaves dark non-zero pixels around the data: set them to the nodata value.
            warped[:, warped_visible == 0] = 0
            destination.write(warped)


def save_quicklooks(output_dir: Path, lon, lat, rgb, visible=None) -> tuple[Path, Path]:
    """Save an unprojected JPEG and a georeferenced COG from the same RGB pixels."""
    jpeg_path = output_dir / "quicklook.jpg"
    cog_path = output_dir / "quicklook.tif"
    jpeg = rgb
    if visible is not None:
        # JPEG has no transparency; whiten missing pixels without changing the COG input.
        jpeg = rgb.copy()
        jpeg[~visible] = 255
    Image.fromarray(jpeg).save(jpeg_path, quality=90)
    write_georeferenced_cog(cog_path, lon, lat, rgb, visible)
    return jpeg_path, cog_path


async def generate_quicklooks(
    owner_id: str,
    published_items: list[dict[str, Any]],
    span_module: str,
    span_name: str,
    write_quicklooks: Callable[[Any, Path], tuple[Path, Path]],
) -> dict[str, dict[str, str]]:
    """Generate, upload and register quicklooks for the published catalog items."""
    if not published_items:
        raise ValueError("At least one published catalog item is required")

    logger = get_run_logger()
    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_id))
    with flow_env.start_span(span_module, span_name):
        catalog_client = flow_env.rs_client.get_catalog_client()
        s3_credentials = S3BucketCredentials(
            key=os.environ["S3_ACCESSKEY"],
            secret=os.environ["S3_SECRETKEY"],
            endpoint_url=os.environ["S3_ENDPOINT"],
            region_name=os.environ["S3_REGION"],
        )
        results: dict[str, dict[str, str]] = {}

        # Process items sequentially to keep a single product in memory at a time.
        for published_item in published_items:
            # The upstream processing flow returns each published item's ID and target collection.
            item_id = published_item.get("id")
            collection_id = published_item.get("collection")
            if not isinstance(item_id, str) or not isinstance(collection_id, str):
                raise ValueError("Each published item must contain string 'id' and 'collection' fields")

            # Read the catalog item first because it contains the source Zarr location.
            item = catalog_client.get_item(collection_id, item_id, owner_id=owner_id)
            if item is None:
                raise ValueError(f"Catalog item {item_id!r} was not found in collection {collection_id!r}")

            product_href = get_zarr_href(item)
            logger.info("Generating quicklooks for %s", product_href)
            # Open the generated product directly from its Zarr asset in object storage.
            product = open_datatree(product_href, credentials=s3_credentials)

            # Local files are temporary and are removed after their upload completes.
            with tempfile.TemporaryDirectory() as temporary_dir:
                jpeg_path, cog_path = write_quicklooks(product.measurements, Path(temporary_dir))
                # Store quicklooks under the source product prefix in object storage.
                jpeg_href = f"{product_href}/quicklook.jpg"
                cog_href = f"{product_href}/quicklook.tif"
                await prefect_utils.s3_upload_file(jpeg_path, jpeg_href)
                await prefect_utils.s3_upload_file(cog_path, cog_href)

            # Release the current product before opening the next one.
            del product

            # Describe both uploaded files as STAC thumbnail assets.
            assets = {
                "quicklook.jpg": {
                    "href": jpeg_href,
                    "roles": ["thumbnail"],
                    "type": JPEG_MEDIA_TYPE,
                },
                "quicklook.tif": {
                    "href": cog_href,
                    "roles": ["thumbnail"],
                    "type": COG_MEDIA_TYPE,
                    "proj:code": QUICKLOOK_CRS,
                },
            }
            # Keep existing extensions and declare the projection metadata added above.
            stac_extensions = list(item.stac_extensions)
            if PROJECTION_EXTENSION not in stac_extensions:
                stac_extensions.append(PROJECTION_EXTENSION)
            catalog_client.patch_item(
                collection_id,
                item_id,
                {"assets": assets, "stac_extensions": stac_extensions},
                owner_id=owner_id,
            )
            logger.info("Quicklooks added to catalog item %s", item_id)
            results[item_id] = {
                "quicklook.jpg": jpeg_href,
                "quicklook.tif": cog_href,
            }

        return results
