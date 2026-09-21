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

from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image
from prefect import flow

from rs_workflows.on_demand.sentinel3.olci_quicklook_common import (
    generate_quicklooks,
    select_downsampled_geolocation,
    write_georeferenced_cog,
)


def build_rgb(measurements):
    """Build a downsampled uint8 RGB array from the OLCI radiance bands."""
    selection, lon, lat = select_downsampled_geolocation(measurements)

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
    lon, lat, rgb = build_rgb(measurements)
    jpeg_path = output_dir / "quicklook.jpg"
    cog_path = output_dir / "quicklook.tif"
    # Keep the source swath grid unchanged for a plain JPEG preview.
    Image.fromarray(rgb).save(jpeg_path, quality=90)

    write_georeferenced_cog(cog_path, lon, lat, rgb)

    return jpeg_path, cog_path


@flow(name="generate-s3-l1-olci-quicklooks")
async def generate_s3l1_olci_quicklooks(
    owner_id: str,
    published_items: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    """Generate, upload and register quicklooks for S3L1 OLCI items."""
    return await generate_quicklooks(
        owner_id,
        published_items,
        __name__,
        "generate-s3-l1-olci-quicklooks",
        write_quicklooks,
    )
