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

"""Exercise quicklook generation through the flow, with external services and rasterio mocked."""

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import numpy as np
import pytest
import xarray as xr
from PIL import Image
from pystac import Asset, Item

# Allow collection without runner-only packages; restore the module registry afterwards.
with patch.dict(
    "sys.modules",
    {
        name: MagicMock()
        for name in (
            "sentineltoolbox",
            "sentineltoolbox.api",
            "rasterio",
            "rasterio.control",
            "rasterio.transform",
            "rasterio.warp",
        )
    },
):
    from rs_workflows.on_demand.sentinel3 import olci_quicklook_common as common
    from rs_workflows.on_demand.sentinel3 import s3_l1_olci_quicklook as quicklook

OWNER = "test-owner"
COLLECTION = "olci-l1"
PRODUCT_HREF = "s3://test-bucket/product.zarr"
PUBLISHED_ITEMS = [{"id": "product", "collection": COLLECTION}]
FILE_EXTENSION = "https://stac-extensions.github.io/file/v2.1.0/schema.json"


@pytest.fixture(name="quicklook_context")
def _quicklook_context(mocker, monkeypatch):
    """Keep the image calculations real while replacing runner dependencies and remote I/O."""
    # Build a small swath with distinct band patterns to check RGB channel mapping and scaling.
    rows, columns = np.indices((9, 8))
    radiance = (rows * 8 + columns).astype(float)
    measurements = xr.Dataset(
        {
            "longitude": (("rows", "columns"), 10.0 + columns),
            "latitude": (("rows", "columns"), 50.0 - rows),
            "oa08_radiance": (("rows", "columns"), radiance),
            "oa06_radiance": (("rows", "columns"), -radiance),
            "oa04_radiance": (("rows", "columns"), columns.astype(float)),
        },
    )
    # This row must be removed before downsampling the remaining rows.
    measurements.longitude.values[0, :] = 0
    measurements.latitude.values[0, :] = 0

    # Put an unrelated asset first so Zarr discovery must select the product asset.
    item = Item("product", None, None, datetime(2025, 6, 12, tzinfo=timezone.utc), {})
    item.stac_extensions = [FILE_EXTENSION]
    item.add_asset("metadata", Asset("s3://test-bucket/metadata.json", media_type="application/json"))
    item.add_asset("product", Asset(PRODUCT_HREF + "/", media_type=common.ZARR_MEDIA_TYPE))

    # Replace Prefect setup and catalog access, and supply harmless S3 credentials.
    flow_env = mocker.patch.object(common, "FlowEnv").return_value
    catalog = flow_env.rs_client.get_catalog_client.return_value
    catalog.get_item.return_value = item
    mocker.patch.object(common, "get_run_logger")
    for name, value in {
        "S3_ACCESSKEY": "testing",
        "S3_SECRETKEY": "testing",
        "S3_ENDPOINT": "https://s3.test",
        "S3_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)

    # Patch the imported symbols where they are used, with fresh mocks for each test.
    toolbox = MagicMock()
    toolbox.api.open_datatree.return_value = SimpleNamespace(measurements=measurements)
    rasterio = MagicMock()
    for name, replacement in {
        "S3BucketCredentials": toolbox.api.S3BucketCredentials,
        "open_datatree": toolbox.api.open_datatree,
        "rasterio": rasterio,
        "GroundControlPoint": rasterio.control.GroundControlPoint,
        "from_bounds": rasterio.transform.from_bounds,
        "Resampling": rasterio.warp.Resampling,
        "reproject": rasterio.warp.reproject,
    }.items():
        mocker.patch.object(common, name, replacement)
    # Capture uploads without contacting S3; expose the setup for each test to customize.
    upload = mocker.patch.object(common.prefect_utils, "s3_upload_file", new_callable=AsyncMock)
    return SimpleNamespace(
        item=item,
        measurements=measurements,
        catalog=catalog,
        toolbox=toolbox.api,
        rasterio=rasterio,
        upload=upload,
    )


@pytest.mark.parametrize("projection_present", [False, True])
async def test_generate_quicklooks(quicklook_context, projection_present):
    """Generate RGB/JPEG, request a georeferenced COG, upload both and register their assets."""
    # Prepare an item with or without existing projection metadata.
    ctx = quicklook_context
    if projection_present:
        ctx.item.stac_extensions.append(common.PROJECTION_EXTENSION)
        # Also exercise Zarr detection by suffix, without a declared media type.
        ctx.item.assets["product"].media_type = None

    # Inspect the real JPEG during upload, before the flow removes its temporary directory.
    async def inspect_upload(path, href):
        assert href == f"{PRODUCT_HREF}/{path.name}"
        if path.suffix == ".jpg":
            with Image.open(path) as image:
                assert image.format == "JPEG"
                assert image.mode == "RGB"
                assert image.size == (2, 2)

    ctx.upload.side_effect = inspect_upload
    # Execute the flow body so Zarr discovery, RGB calculation and image writing all run.
    result = await quicklook.generate_s3l1_olci_quicklooks.fn(OWNER, PUBLISHED_ITEMS)

    # Check that the catalog lookup and product read use the expected owner, href and credentials.
    ctx.catalog.get_item.assert_called_once_with(COLLECTION, "product", owner_id=OWNER)
    ctx.toolbox.S3BucketCredentials.assert_called_once_with(
        key="testing",
        secret="testing",
        endpoint_url="https://s3.test",
        region_name="us-east-1",
    )
    ctx.toolbox.open_datatree.assert_called_once_with(
        PRODUCT_HREF,
        credentials=ctx.toolbox.S3BucketCredentials.return_value,
    )
    # Check both upload destinations and cleanup of the shared temporary directory.
    jpeg_path, cog_path = (entry.args[0] for entry in ctx.upload.await_args_list)
    assert jpeg_path.name == "quicklook.jpg"
    assert cog_path.name == "quicklook.tif"
    assert jpeg_path.parent == cog_path.parent
    assert not jpeg_path.parent.exists()
    ctx.upload.assert_has_awaits(
        [call(jpeg_path, f"{PRODUCT_HREF}/quicklook.jpg"), call(cog_path, f"{PRODUCT_HREF}/quicklook.tif")],
    )

    # Check COG settings and control points derived from the filtered, downsampled coordinates.
    ctx.rasterio.transform.from_bounds.assert_called_once_with(10.0, 45.0, 14.0, 49.0, 2, 2)
    transform = ctx.rasterio.transform.from_bounds.return_value
    ctx.rasterio.open.assert_called_once_with(
        cog_path,
        "w",
        driver="COG",
        height=2,
        width=2,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
        compress="deflate",
    )
    ctx.rasterio.control.GroundControlPoint.assert_has_calls(
        [
            call(row=0, col=0, x=10.0, y=49.0),
            call(row=0, col=1, x=14.0, y=49.0),
            call(row=1, col=0, x=10.0, y=45.0),
            call(row=1, col=1, x=14.0, y=45.0),
        ],
    )
    # Inspect the real RGB array and projection arguments passed to the mocked reprojection.
    ctx.rasterio.warp.reproject.assert_called_once()
    warp_args = ctx.rasterio.warp.reproject.call_args.kwargs
    rgb = warp_args["source"]
    assert rgb.shape == (3, 2, 2)
    assert rgb.dtype == np.uint8
    np.testing.assert_array_equal(rgb[:, 0, 0], [0, 255, 0])
    np.testing.assert_array_equal(rgb[:, 1, 1], [255, 0, 255])
    assert warp_args["src_crs"] == warp_args["dst_crs"] == "EPSG:4326"
    assert warp_args["dst_transform"] is transform
    assert warp_args["resampling"] is ctx.rasterio.warp.Resampling.bilinear

    # Check thumbnail metadata, preservation of the file extension and a single projection extension.
    assets = {
        "quicklook.jpg": {
            "href": f"{PRODUCT_HREF}/quicklook.jpg",
            "roles": ["thumbnail"],
            "type": "image/jpeg",
        },
        "quicklook.tif": {
            "href": f"{PRODUCT_HREF}/quicklook.tif",
            "roles": ["thumbnail"],
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "proj:code": "EPSG:4326",
        },
    }
    ctx.catalog.patch_item.assert_called_once_with(
        COLLECTION,
        "product",
        {"assets": assets, "stac_extensions": [FILE_EXTENSION, common.PROJECTION_EXTENSION]},
        owner_id=OWNER,
    )
    assert result == {"product": {name: asset["href"] for name, asset in assets.items()}}


@pytest.mark.parametrize(
    "case, message",
    [
        ("empty", "At least one published catalog item"),
        ("missing_id", "string 'id' and 'collection'"),
        ("invalid_collection", "string 'id' and 'collection'"),
        ("not_found", "was not found"),
        ("no_zarr", "has no Zarr asset"),
        ("non_s3", "must use an S3 href"),
    ],
)
async def test_generate_quicklooks_rejects_invalid_items(quicklook_context, case, message):
    """Reject invalid catalog inputs before reading the product or publishing quicklooks."""
    # Introduce the selected input error into an otherwise valid catalog setup.
    ctx = quicklook_context
    published: list[dict[str, Any]] = PUBLISHED_ITEMS
    if case == "empty":
        published = []
    elif case == "missing_id":
        published = [{"collection": COLLECTION}]
    elif case == "invalid_collection":
        published = [{"id": "product", "collection": 123}]
    elif case == "not_found":
        ctx.catalog.get_item.return_value = None
    elif case == "no_zarr":
        ctx.item.assets.pop("product")
    elif case == "non_s3":
        ctx.item.assets["product"].href = "https://example.test/product.zarr"

    # Call the flow and check that its validation reports the expected input error.
    with pytest.raises(ValueError, match=message):
        await quicklook.generate_s3l1_olci_quicklooks.fn(OWNER, published)

    # Invalid items must stop processing before any product read, upload or catalog update.
    ctx.toolbox.open_datatree.assert_not_called()
    ctx.upload.assert_not_awaited()
    ctx.catalog.patch_item.assert_not_called()


@pytest.mark.parametrize(
    "case, message",
    [
        ("zero_geolocation", "no valid geolocation rows"),
        ("nan_coordinates", "no valid coordinates"),
    ],
)
async def test_generate_quicklooks_rejects_invalid_measurements(quicklook_context, case, message):
    """Exercise helper validation through the flow with malformed source arrays."""
    # Corrupt geolocation while keeping catalog discovery valid.
    ctx = quicklook_context
    if case == "zero_geolocation":
        ctx.measurements.longitude.values[:] = 0
        ctx.measurements.latitude.values[:] = 0
    elif case == "nan_coordinates":
        ctx.measurements.longitude.values[:] = np.nan
        ctx.measurements.latitude.values[:] = np.nan

    # Reach the internal image helpers through the flow and check their validation error.
    with pytest.raises(ValueError, match=message):
        await quicklook.generate_s3l1_olci_quicklooks.fn(OWNER, PUBLISHED_ITEMS)

    # The product was read, but invalid measurements must prevent COG writing and publication.
    ctx.toolbox.open_datatree.assert_called_once()
    ctx.rasterio.open.assert_not_called()
    ctx.upload.assert_not_awaited()
    ctx.catalog.patch_item.assert_not_called()


async def test_generate_quicklooks_accepts_constant_radiance(quicklook_context):
    """A constant channel becomes zero intensity without preventing publication."""
    ctx = quicklook_context
    ctx.measurements.oa08_radiance.values[:] = 1
    # Equal percentiles produce NaNs during scaling, which are then converted to zero.
    with np.errstate(invalid="ignore", divide="ignore"):
        await quicklook.generate_s3l1_olci_quicklooks.fn(OWNER, PUBLISHED_ITEMS)

    rgb = ctx.rasterio.warp.reproject.call_args.kwargs["source"]
    assert np.all(rgb[0] == 0)
    assert ctx.upload.await_count == 2
    ctx.catalog.patch_item.assert_called_once()


async def test_generate_quicklooks_does_not_patch_after_upload_failure(quicklook_context):
    """Do not register incomplete quicklooks if the second upload fails; clean temporary files."""
    # Let the JPEG upload succeed, then simulate a failure when uploading the COG.
    ctx = quicklook_context
    ctx.upload.side_effect = [None, RuntimeError("Upload failed")]

    # Execute the flow and check that the upload failure propagates to the caller.
    with pytest.raises(RuntimeError, match="Upload failed"):
        await quicklook.generate_s3l1_olci_quicklooks.fn(OWNER, PUBLISHED_ITEMS)

    # Both uploads were attempted; the catalog stays untouched and temporary files are removed.
    assert ctx.upload.await_count == 2
    ctx.catalog.patch_item.assert_not_called()
    assert not ctx.upload.await_args_list[0].args[0].parent.exists()
