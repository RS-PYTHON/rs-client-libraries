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

"""Generate quicklooks for catalogued Sentinel-3 OLCI Level-2 products."""

from pathlib import Path
from typing import Any

import numpy as np
from prefect import flow

from rs_workflows.on_demand.sentinel3.olci_quicklook_common import (
    generate_quicklooks,
    normalize_channel,
    save_quicklooks,
    select_downsampled_geolocation,
)

# Map the OLCI L2 variables to the red, green and blue channels.
L2_CHANNELS = ("otci", "gifapar", "iwv")


def stretch_channel(values):
    """Scale one channel to uint8 between its 2nd and 98th percentiles; also return its valid pixels."""
    valid = np.isfinite(values)
    samples = np.where(valid, values, np.nan)
    normalized = normalize_channel(samples)
    scaled = np.nan_to_num(normalized * 255, nan=0.0).astype("uint8")
    return scaled, valid


def build_rgb(measurements):
    """Build a downsampled uint8 RGB array and its data availability mask from the OLCI L2 variables."""
    selection, lon, lat = select_downsampled_geolocation(measurements)

    rgb = np.zeros((*lon.shape, 3), dtype="uint8")
    valid = np.zeros(rgb.shape, dtype=bool)
    for channel, name in enumerate(L2_CHANNELS):
        # A variable missing from the product leaves its channel at 0.
        if name not in measurements:
            continue
        values = measurements[name].isel(selection).values.astype("float32")
        rgb[:, :, channel], valid[:, :, channel] = stretch_channel(values)
    # Pixels without data in every channel are transparent in the COG and white in the JPEG.
    return lon, lat, rgb, valid.any(axis=2)


def write_quicklooks(measurements, output_dir: Path) -> tuple[Path, Path]:
    """Write the unprojected JPEG and georeferenced COG quicklooks."""
    lon, lat, rgb, visible = build_rgb(measurements)
    return save_quicklooks(output_dir, lon, lat, rgb, visible)


@flow(name="generate-s3-l2-olci-quicklooks")
async def generate_s3l2_olci_quicklooks(
    owner_id: str,
    published_items: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    """Generate, upload and register quicklooks for S3L2 OLCI items."""
    return await generate_quicklooks(
        owner_id,
        published_items,
        __name__,
        "generate-s3-l2-olci-quicklooks",
        write_quicklooks,
    )
