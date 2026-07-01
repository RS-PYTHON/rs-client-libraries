#!/usr/bin/env python
# coding: utf8
#
# Vendored from the ESA EOPF Copernicus project (CPM ADF auxiliary-data-file):
# https://gitlab.eopf.copernicus.eu/cpm/adf-auxiliary-data-file/-/tree/main/scripts/
#
# Copyright 2022-2026 ESA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Generation script for WATER
"""

import os.path as osp
import struct
from datetime import datetime
from os import environ
from pathlib import Path

import cop_dem_utils as cdu
import dask
import dask.array as da
import numpy as np
import rasterio
import xarray as xr
from rasterio.shutil import copy as rio_copy
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window
from util import REF_DATE_FORMAT, duplicate_longitude_when_needed, gen_static_adf_name


def convert_surface_classification_map(input_file: str, output_file: str):
    """
    Read the binary ADF S3__SR_2_SURFAX and dump it to GeoTiff
    """
    nb_row_SCM = 21600
    nb_col_SCM = 43200
    with open(input_file, "rb") as file_SCM:
        header_SCM = file_SCM.read(1392)
    print("Number of grid points in longitude:", int.from_bytes(header_SCM[1332:1335], "little"))
    print("Number of grid points in latitude:", int.from_bytes(header_SCM[1336:1339], "little"))
    print("First longitude value [arcmin] :", struct.unpack("<d", header_SCM[1352:1360]))
    print("Last longitude value  [arcmin] :", struct.unpack("<d", header_SCM[1360:1368]))
    print("First latitude value  [arcmin] :", struct.unpack("<d", header_SCM[1376:1384]))
    print("Last latitude value   [arcmin] :", struct.unpack("<d", header_SCM[1384:1392]))
    # read data
    data_SCM = np.memmap(input_file, dtype="uint8", mode="r", offset=1392, shape=(nb_col_SCM, nb_row_SCM))
    print(type(data_SCM), data_SCM.dtype, data_SCM.ndim, data_SCM.shape, data_SCM.size)
    # ~ print(np.histogram(data_SCM, bins=np.arange(8)))
    transform = rasterio.Affine(0.0083333333, 0.0, -180, 0.0, -0.0083333333, 89.99999928000003)
    with rasterio.open(
        output_file,
        "w",
        driver="GTIFF",
        height=data_SCM.shape[1],
        width=data_SCM.shape[0],
        count=1,
        dtype=rasterio.uint8,
        nodata=255,
        crs="EPSG:4326",
        transform=transform,
    ) as temp_data:
        data = np.transpose(data_SCM.astype(rasterio.uint8), axes=(1, 0))
        data = np.flip(data, axis=0)
        temp_data.write(data, 1)
        # ~ print(np.histogram(data, bins=np.arange(8)))


@dask.delayed
def merge_layers(layers: list, window: Window, prg):
    """
    Merge the 6 water mask layers into a common bitmask

    :param layers: list of input rasterio Datasets, in the order [LWM, CLM, TRM, OOM, MLM, SURFAX]
    :param window: Chunk window to process
    """
    if len(layers) != 6:
        raise RuntimeError(f"Expected 6 layers, got {len(layers)}")
    # open datasets
    lwm_ds = rasterio.open(layers[0])
    clm_ds = rasterio.open(layers[1])
    trm_ds = rasterio.open(layers[2])
    oom_ds = rasterio.open(layers[3])
    mlm_ds = rasterio.open(layers[4])
    surf_ds = rasterio.open(layers[5])

    # read data from each layer for given chunk window
    lwm = lwm_ds.read(1, window=window)  # data1
    clm = clm_ds.read(1, window=window)  # data2
    trm = trm_ds.read(1, window=window)  # data3
    oom = oom_ds.read(1, window=window)  # data4
    mlm = mlm_ds.read(1, window=window)  # data5
    surf = surf_ds.read(1, window=window)  # data6

    output = np.zeros((window.height, window.width), dtype="uint16")

    output[oom == 0] = np.uint16(2**0)  # land-sea mask contribution recorded on bit n°0 for "open ocean and sea"
    output[lwm == 1] |= np.uint16(2**3)  # land-water mask contribution recorded on bit n°3 for "dry land"
    output[mlm == 2] |= np.uint16(2**7)  # marine-land mask contribution recorded on bit n°7 for "coastal area"
    output[surf == 6] |= np.uint16(2**1)  # salted basin contribution recorded on bit n°1 for "salted basin"
    output[surf == 2] |= np.uint16(2**2)  # fresh water contribution recorded on bit n°2 for "fresh water"
    output[surf == 3] |= np.uint16(2**4)  # aquatic vegetation contribution recorded on bit n°4 for "aquatic vegetation"
    output[surf == 4] |= np.uint16(
        2**5,
    )  # continental ice contribution recorded on bit n°5 for "continental ice and snow"
    output[surf == 5] |= np.uint16(2**6)  # floating ice contribution recorded on bit n°6 for "floating ice"
    output[clm == 1] |= np.uint16(2**8)  # coast-line mask contribution recorded on bit n°8 for “coast line”
    output[trm == 1] |= np.uint16(2**9)  # tide-area mask contribution recorded on bit n°9 for “tidal area”

    lwm_ds.close()
    clm_ds.close()
    trm_ds.close()
    oom_ds.close()
    mlm_ds.close()
    surf_ds.close()

    prg.one_more()

    return output


in_dir = Path(environ.get("ADF_INPUT", "../../shared/ADF"))

# Generation of the raster
# ~ Data1 : land_water_bitmask_geo_bc_tiled.tif (S3__AX___LWM_AX)
# ~ Data2 : coastline_bitmask_geo_bc_tiled.tif (S3__AX___CLM_AX)
# ~ Data3 : tidal_regions_bitmask_geo_bc_tiled.tif (S3__AX___TRM_AX)
# ~ Data4 : open_ocean_bitmask_geo_bc_tiled.tif (S3__AX___OOM_AX)
# ~ Data5 : LandMarineMaskFit_LZW.tif (S3__SR_2_MLM_AX)
# ~ Data6 : SurfaceClassificationFit_LZW.tif (S3__SR_2_SURFAX)
# read S3__AX___LWM_AX
lwm_path = (
    in_dir
    / "S3__AX___LWM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3"
    / "land_water_bitmask_geo_bc_tiled.tif"
)
with rasterio.open(lwm_path) as data1:
    full_width = data1.width
    full_height = data1.height
    full_transform = data1.transform
    geotransform = data1.transform.to_gdal()

# read S3__AX___CLM_AX
clm_path = (
    in_dir
    / "S3__AX___CLM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3"
    / "coastline_bitmask_geo_bc_tiled.tif"
)
# ~ data2 = rasterio.open(clm_path)

# read S3__AX___TRM_AX
trm_path = (
    in_dir
    / "S3__AX___TRM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3"
    / "tidal_regions_bitmask_geo_bc_tiled.tif"
)
# ~ data3 = rasterio.open(trm_path)

# read S3__AX___OOM_AX
oom_path = (
    in_dir
    / "S3__AX___OOM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3"
    / "open_ocean_bitmask_geo_bc_tiled.tif"
)
# ~ data4 = rasterio.open(oom_path)

# read S3__SR_2_MLM_AX and resample
mlm_path = (
    in_dir
    / "S3__SR_2_MLM_AX_20160216T000000_20991231T235959_20200512T120000___________________MPC_O_AL_004.SEN3"
    / "LandMarineMask_2p2_km.nc"
)
mlm_vrt_path = mlm_path.parent / "LandMarineMask_2p2_km_warp.vrt"
with rasterio.open(mlm_path) as raw_data5:
    with WarpedVRT(
        raw_data5,
        height=full_height,
        width=full_width,
        transform=full_transform,
        resampling=rasterio.enums.Resampling.nearest,
    ) as data5:
        rio_copy(data5, mlm_vrt_path, driver="VRT")

# read S3__SR_2_SURFAX
surfax_path = (
    in_dir
    / "S3__SR_2_SURFAX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3"
    / "SurfaceClassification.dat"
)
surfax_tif_path = surfax_path.parent / "SurfaceClassification.tif"
surfax_vrt_path = surfax_path.parent / "SurfaceClassification_warp.vrt"
if not surfax_tif_path.is_file():
    convert_surface_classification_map(surfax_path, surfax_tif_path)

with rasterio.open(surfax_tif_path) as raw_data6:
    with WarpedVRT(
        raw_data6,
        height=full_height,
        width=full_width,
        transform=full_transform,
        resampling=rasterio.enums.Resampling.nearest,
    ) as data6:
        rio_copy(data6, surfax_vrt_path, driver="VRT")

layers = [lwm_path, clm_path, trm_path, oom_path, mlm_vrt_path, surfax_vrt_path]
chunk_height = full_height // 100 + 1
chunk_width = (full_width + 2) // 100 + 1
all_lines = []
prg = cdu.Progression("Merge layers", 100 * 100)

for yoff in range(0, full_height, chunk_height):
    cur_line = []
    for xoff in range(0, full_width + 1, chunk_width):
        # handle a shift of 1 pixel to account for duplicated column at the start
        cur_window = Window(xoff - 1, yoff, chunk_width, chunk_height).crop(full_height, full_width)
        chunk = da.from_delayed(
            merge_layers(layers, cur_window, prg),
            dtype=np.dtype("uint16"),
            shape=(cur_window.height, cur_window.width),
        )
        cur_line.append(chunk)
    # duplicate first and last column
    first_col = cur_line[0][:, 0:1]
    last_col = cur_line[-1][:, -1:]
    first_chunk_extended = da.concatenate([last_col, cur_line[0]], axis=1)
    last_chunk_extended = da.concatenate([cur_line[-1], first_col], axis=1)
    cur_line[0] = da.rechunk(first_chunk_extended, chunks=(-1, -1))
    cur_line[-1] = da.rechunk(last_chunk_extended, chunks=(-1, -1))

    # concatenate this chunk row
    all_lines.append(da.concatenate(cur_line, axis=1))
water_map = da.concatenate(all_lines, axis=0)

# Build axes

# WARNING: the georeference of the input classification mask uses AREA convention. Hence the
# first longitude is attached to the upper-left corner of first pixel. For the zarr ADF,
# the grid convention is preferred: coordinates attached to the pixel center.
# Also, since the target sampling is 10 arcseconds, the coordinates will be expressed in arcseconds,
# not in degrees. The first longitude observed was -180.00000033902793°, the small shift with -180°
# will be discarded as it represents only 4cm at equator, for data sampled at 300m.

# Verify the assumptions on coordinates
if not np.allclose(geotransform[0], -180.0, rtol=0, atol=1e-6):
    raise RuntimeError(f"Expected longitude start at -180°, got {geotransform[0]}")
if not np.allclose(geotransform[1] * 3600, 10, rtol=0, atol=1e-6):
    raise RuntimeError(f"Expected longitude step of 10 arcsec, got {geotransform[1] * 3600}")
if not np.allclose(geotransform[3], 90.0, rtol=0, atol=1e-6):
    raise RuntimeError(f"Expected latitude start at 90°, got {geotransform[3]}")
if not np.allclose(geotransform[5] * 3600, -10, rtol=0, atol=1e-6):
    raise RuntimeError(f"Expected latitude step of -10 arcsec, got {geotransform[5] * 3600}")

full_width += 2  # One column is added at the start and at the end
# Convert upper-left corner origin (-180, 90) to pixel center, in arcseconds
coord_lon = np.arange(-180 * 3600 - 5, -180 * 3600 - 5 + full_width * 10, 10, dtype="int32")
coord_lat = np.arange(90 * 3600 - 5, 90 * 3600 - 5 - full_height * 10, -10, dtype="int32")

# Create xr.Dataset
root_attrs = {
    "stac_version": "1.1.0",
    "stac_extensions": ["https://stac-extensions.github.io/product/v1.0.0/schema.json"],
    "type": "Feature",
    "properties": {
        "description": (
            "water-related classification mask; "
            "spatial resolution is 10 x 10 arcseconds; "
            "bit meaning is 0 = open ocean or semi-enclosed sea; "
            "1 = salted basin or enclosed sea; "
            "2 = continental fresh water; "
            "3 = dry land; "
            "4 = aquatic vegetation; "
            "5 = continental ice and snow; "
            "6 = floating ice; "
            "7 = coastal area; "
            "8 = coast line; "
            "9 = significant tidal activity; "
            "number of grid points in longitude is 129600; "
            "number of grid points in latitude is 64800; "
            "first longitude value is -180 deg; "
            "last longitude value is +180 deg; "
            "first latitude value is +90 deg; "
            "last latitude value is -90 deg"
        ),
        "product:type": "ADF_WATER",
    },
}

water_attrs = {"unit": "bitmask"}

ds = xr.Dataset(
    {"water_class": (("latitude", "longitude"), water_map, water_attrs)},
    coords={
        "longitude": ("longitude", coord_lon),
        "latitude": ("latitude", coord_lat),
    },
    attrs=root_attrs,
)

ds.coords["longitude"].attrs = {
    "long_name": "longitudes of water mask",
    "units": "arcseconds",
    "valid_max": 180 * 3600,
    "valid_min": -180 * 3600,
}
ds.coords["latitude"].attrs = {
    "long_name": "latitudes of water mask",
    "units": "arcseconds",
    "valid_max": 90 * 3600,
    "valid_min": -90 * 3600,
}

# manage column duplication
ds = duplicate_longitude_when_needed(ds)

# dump to zarr
file_name = gen_static_adf_name("S00__ADF_WATER")
ds.attrs["id"] = Path(file_name).stem
ds.attrs["properties"]["created"] = datetime.today().strftime(REF_DATE_FORMAT)

out_dir = environ.get("ADF_OUTPUT", "../../scratch")
out_path = osp.join(out_dir, file_name)
ds.to_zarr(out_path)
