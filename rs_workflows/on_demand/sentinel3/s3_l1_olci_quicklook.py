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

"""Generate quicklooks for catalogued Sentinel-3 OLCI Level-1 products."""

import os
import tempfile
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger

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


def build_rgb(measurements):
    """Build a downsampled uint8 RGB array from the OLCI radiance bands."""
    import numpy as np  # pylint: disable=import-outside-toplevel

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

    def quicklook_band(band):
        values = band.isel(selection).values.astype("float32")
        # Clip outliers before scaling the radiance values to the display range.
        vmin, vmax = np.nanpercentile(values, [2, 98])
        if not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
            raise ValueError(f"Invalid radiance percentile range: {vmin=}, {vmax=}")
        return np.clip((values - vmin) / (vmax - vmin), 0, 1)

    rgb = np.stack(
        # Map the OLCI red, green and blue radiance bands to RGB channels.
        [
            quicklook_band(measurements.oa08_radiance),
            quicklook_band(measurements.oa06_radiance),
            quicklook_band(measurements.oa04_radiance),
        ],
        axis=2,
    )
    # Render any remaining invalid pixels as black in the output images.
    return lon, lat, np.nan_to_num(rgb * 255, nan=0.0).astype("uint8")


def write_quicklooks(measurements, output_dir: Path) -> tuple[Path, Path]:
    """Write the unprojected JPEG and georeferenced COG quicklooks."""
    import numpy as np  # pylint: disable=import-outside-toplevel
    import rasterio  # pylint: disable=import-outside-toplevel
    from PIL import Image  # pylint: disable=import-outside-toplevel
    from rasterio.control import (
        GroundControlPoint,  # pylint: disable=import-outside-toplevel
    )
    from rasterio.transform import (
        from_bounds,  # pylint: disable=import-outside-toplevel
    )
    from rasterio.warp import (  # pylint: disable=import-outside-toplevel
        Resampling,
        reproject,
    )

    lon, lat, rgb = build_rgb(measurements)
    jpeg_path = output_dir / "quicklook.jpg"
    cog_path = output_dir / "quicklook.tif"
    # Keep the source swath grid unchanged for a plain JPEG preview.
    Image.fromarray(rgb).save(jpeg_path, quality=90)

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
    ) as destination:
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

    return jpeg_path, cog_path


@flow(name="generate-s3-l1-olci-quicklooks")
async def generate_s3l1_olci_quicklooks(
    owner_id: str,
    published_items: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    """Generate, upload and register quicklooks for S3L1 OLCI items."""
    if not published_items:
        raise ValueError("At least one published catalog item is required")

    # Load runner-only scientific dependencies when the Prefect flow starts.
    from sentineltoolbox.api import (  # pylint: disable=import-outside-toplevel
        S3BucketCredentials,
        open_datatree,
    )

    logger = get_run_logger()
    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_id))
    with flow_env.start_span(__name__, "generate-s3-l1-olci-quicklooks"):
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
            results[item_id] = {name: asset["href"] for name, asset in assets.items()}

        return results
