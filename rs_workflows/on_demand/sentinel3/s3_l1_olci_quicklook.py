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

"""Generate quicklooks for a catalogued Sentinel-3 OLCI Level-1 product."""

import os
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger

from rs_common import prefect_utils
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

JPEG_MEDIA_TYPE = "image/jpeg"
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"
ZARR_MEDIA_TYPE = "application/vnd+zarr"


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


def build_rgb(measurements, step: int):
    """Build a downsampled uint8 RGB array from the OLCI radiance bands."""
    import numpy as np  # pylint: disable=import-outside-toplevel

    lon_full = measurements.longitude.values
    lat_full = measurements.latitude.values

    # Rows corrupted by the S3-OLCI processor have zero longitude and latitude ("Null Island").
    good_rows = ~((lon_full[:, 0] == 0) & (lat_full[:, 0] == 0))
    if not good_rows.any():
        raise ValueError("The OLCI product contains no valid geolocation rows")

    # Downsample the valid swath to keep quicklook generation fast and lightweight.
    lon = lon_full[good_rows][::step, ::step]
    lat = lat_full[good_rows][::step, ::step]

    def quicklook_band(band):
        values = band.values[good_rows][::step, ::step].astype("float32")
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


def write_quicklooks(measurements, output_dir: Path, step: int) -> tuple[Path, Path]:
    """Write the unprojected JPEG and projected COG quicklooks."""
    import numpy as np  # pylint: disable=import-outside-toplevel
    import rasterio  # pylint: disable=import-outside-toplevel
    from PIL import Image  # pylint: disable=import-outside-toplevel
    from pyproj import CRS, Transformer  # pylint: disable=import-outside-toplevel
    from rasterio.control import GroundControlPoint  # pylint: disable=import-outside-toplevel
    from rasterio.transform import from_bounds  # pylint: disable=import-outside-toplevel
    from rasterio.warp import Resampling, reproject  # pylint: disable=import-outside-toplevel

    lon, lat, rgb = build_rgb(measurements, step)
    jpeg_path = output_dir / "quicklook.jpg"
    cog_path = output_dir / "quicklook.tif"
    # Keep the source swath grid unchanged for a plain JPEG preview.
    Image.fromarray(rgb).save(jpeg_path, quality=90)

    # Ignore any remaining invalid coordinates when centring and framing the projection.
    valid_geo = np.isfinite(lon) & np.isfinite(lat)
    if not valid_geo.any():
        raise ValueError("The OLCI product contains no valid coordinates")

    central_longitude = float(np.mean(lon[valid_geo]))
    central_latitude = float(np.mean(lat[valid_geo]))
    # Use the notebook's stereographic projection, centred on the product footprint.
    dst_crs = CRS.from_proj4(
        f"+proj=stere +lat_0={central_latitude} +lon_0={central_longitude} " "+datum=WGS84 +units=m +no_defs",
    )
    # Project the pixel coordinates to tightly frame the diagonal satellite swath.
    transformer = Transformer.from_crs("EPSG:4326", dst_crs, always_xy=True)
    x, y = transformer.transform(lon, lat)
    # Exclude projection failures from the output bounds and control points.
    valid_xy = np.isfinite(x) & np.isfinite(y)
    xmin, xmax = float(np.min(x[valid_xy])), float(np.max(x[valid_xy]))
    ymin, ymax = float(np.min(y[valid_xy])), float(np.max(y[valid_xy]))

    gcp_rows = np.linspace(0, lon.shape[0] - 1, min(20, lon.shape[0]), dtype=int)
    gcp_cols = np.linspace(0, lon.shape[1] - 1, min(20, lon.shape[1]), dtype=int)
    # Build sparse control points from the swath grid in projected metric coordinates.
    gcps = [
        GroundControlPoint(row=int(row), col=int(col), x=float(x[row, col]), y=float(y[row, col]))
        for row in gcp_rows
        for col in gcp_cols
        if valid_xy[row, col]
    ]
    if not gcps:
        raise ValueError("Could not build ground control points for the OLCI product")

    # Cover the swath bounds with a regular grid at roughly the downsampled source resolution.
    resolution = max((xmax - xmin) / lon.shape[1], (ymax - ymin) / lon.shape[0])
    dst_width = max(1, int((xmax - xmin) / resolution))
    dst_height = max(1, int((ymax - ymin) / resolution))
    dst_transform = from_bounds(xmin, ymin, xmax, ymax, dst_width, dst_height)

    # Reproject through the control points and write the projected image directly as a COG.
    with rasterio.open(
        cog_path,
        "w",
        driver="COG",
        height=dst_height,
        width=dst_width,
        count=3,
        dtype="uint8",
        crs=dst_crs,
        transform=dst_transform,
        compress="deflate",
    ) as destination:
        reproject(
            source=np.moveaxis(rgb, 2, 0),
            destination=rasterio.band(destination, [1, 2, 3]),
            gcps=gcps,
            # GCP coordinates are already expressed in the destination CRS units.
            src_crs=dst_crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            resampling=Resampling.bilinear,
        )

    return jpeg_path, cog_path


@flow(name="generate-s3-l1-olci-quicklooks")
async def generate_s3l1_olci_quicklooks(
    owner_id: str,
    collection_id: str,
    item_id: str,
    step: int = 4,
    register_assets: bool = True,
) -> dict[str, str]:
    """Generate, upload and register quicklooks for an existing S3L1 OLCI item."""
    if step < 1:
        raise ValueError("The quicklook downsampling step must be greater than zero")

    # Heavy scientific dependencies are loaded only by the Prefect runner.
    from sentineltoolbox.api import (  # pylint: disable=import-outside-toplevel
        S3BucketCredentials,
        open_datatree,
    )

    logger = get_run_logger()
    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_id))
    with flow_env.start_span(__name__, "generate-s3-l1-olci-quicklooks"):
        catalog_client = flow_env.rs_client.get_catalog_client()
        # Read the catalog item first because it contains the source Zarr location.
        item = catalog_client.get_item(collection_id, item_id, owner_id=owner_id)
        if item is None:
            raise ValueError(f"Catalog item {item_id!r} was not found in collection {collection_id!r}")

        product_href = get_zarr_href(item)
        logger.info("Generating quicklooks for %s", product_href)
        # Open the generated product directly from its Zarr asset in object storage.
        product = open_datatree(
            product_href,
            credentials=S3BucketCredentials(
                key=os.environ["S3_ACCESSKEY"],
                secret=os.environ["S3_SECRETKEY"],
                endpoint_url=os.environ["S3_ENDPOINT"],
                region_name=os.environ["S3_REGION"],
            ),
        )

        # Local files are temporary and are removed after their upload completes.
        with tempfile.TemporaryDirectory() as temporary_dir:
            jpeg_path, cog_path = write_quicklooks(product.measurements, Path(temporary_dir), step)
            # Store quicklooks next to the source product in object storage.
            jpeg_href = f"{product_href}/quicklook.jpg"
            cog_href = f"{product_href}/quicklook.tif"
            await prefect_utils.s3_upload_file(jpeg_path, jpeg_href)
            await prefect_utils.s3_upload_file(cog_path, cog_href)

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
            },
        }
        # Generation can be tested independently while production keeps registration enabled.
        if register_assets:
            catalog_client.patch_item(
                collection_id,
                item_id,
                {"assets": assets, "properties": {}},
                owner_id=owner_id,
            )
            logger.info("Quicklooks added to catalog item %s", item_id)
        else:
            logger.info("Quicklooks generated without catalog registration for item %s", item_id)
        return {name: asset["href"] for name, asset in assets.items()}
