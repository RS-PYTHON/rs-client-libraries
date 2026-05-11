#!/usr/bin/env python
# coding: utf8
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
Generation script for GETAS
"""

import os.path as osp
from datetime import datetime
from os import environ, listdir
from pathlib import Path, PurePath

import dask
import dask.array as da
import numpy as np
import s3fs
import xarray as xr
from util import REF_DATE_FORMAT, duplicate_first_longitude, gen_static_adf_name

s3 = None

adf_dir = PurePath(environ.get("ADF_INPUT", "../../shared/ADF"))
input_folder = PurePath(osp.join(adf_dir, "getasse30v1/ACE_V1_041203"))
input_folder = (
    adf_dir / "S3__AX___DEM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3"
)


@dask.delayed
def get_tile(path_to_tile):
    global s3

    if osp.isfile(path_to_tile):
        with open(path_to_tile, mode="rb") as tile_in:
            data = tile_in.read()
        dt = np.dtype("float32")
    else:
        if s3 is None:
            s3 = s3fs.S3FileSystem(
                key=environ["S3_INPUT_KEY"],
                secret=environ["S3_INPUT_SECRET"],
                client_kwargs={"endpoint_url": environ["S3_INPUT_URL"], "region_name": environ["S3_INPUT_REGION"]},
            )
        with s3.open(path_to_tile, mode="rb") as d30:
            data = d30.read()
        dt = np.dtype("int16")

    dt = dt.newbyteorder(">")

    array = np.frombuffer(data, dtype=dt).reshape(1800, 1800)
    array = np.flip(array, axis=0)

    return array


# Create and classify the GETAS files by latitude and longitude using a dict
# ~ s3_keys = s3.ls("eopf/cpm/test_data/DEM_GETAS_30_ESA/")
s3_keys = [input_folder / tile for tile in listdir(input_folder) if tile.endswith(".GETASSE30")]


tiles_list = {"N": {}, "S": {}}
for filename in s3_keys:
    stem = filename.stem
    lat_num = stem[:2]
    lat_ori = stem[2:3]
    lon_ori = stem[-1:]

    if lat_num not in tiles_list[lat_ori]:
        tiles_list[lat_ori][lat_num] = {"E": [], "W": []}
    tiles_list[lat_ori][lat_num][lon_ori].append(filename)

# RESULT (only stems)
# {'N': {'00': {'E': ['00N000E', '00N015E', '00N030E', '00N045E', '00N060E', '00N075E', '00N090E', '00N105E', '00N120E', '00N135E', '00N150E', '00N165E'],   # noqa: E501
#               'W': ['00N015W', '00N030W', '00N045W', '00N060W', '00N075W', '00N090W', '00N105W', '00N120W', '00N135W', '00N150W', '00N165W', '00N180W']},  # noqa: E501
#        '15': {'E': ['15N000E', '15N015E', '15N030E', '15N045E', '15N060E', '15N075E', '15N090E', '15N105E', '15N120E', '15N135E', '15N150E', '15N165E'],   # noqa: E501
#               'W': ['15N015W', '15N030W', '15N045W', '15N060W', '15N075W', '15N090W', '15N105W', '15N120W', '15N135W', '15N150W', '15N165W', '15N180W']},  # noqa: E501
#        '30': {'E': ['30N000E', '30N015E', '30N030E', '30N045E', '30N060E', '30N075E', '30N090E', '30N105E', '30N120E', '30N135E', '30N150E', '30N165E'],   # noqa: E501
#               'W': ['30N015W', '30N030W', '30N045W', '30N060W', '30N075W', '30N090W', '30N105W', '30N120W', '30N135W', '30N150W', '30N165W', '30N180W']},  # noqa: E501
#        '45': {'E': ['45N000E', '45N015E', '45N030E', '45N045E', '45N060E', '45N075E', '45N090E', '45N105E', '45N120E', '45N135E', '45N150E', '45N165E'],   # noqa: E501
#               'W': ['45N015W', '45N030W', '45N045W', '45N060W', '45N075W', '45N090W', '45N105W', '45N120W', '45N135W', '45N150W', '45N165W', '45N180W']},  # noqa: E501
#        '60': {'E': ['60N000E', '60N015E', '60N030E', '60N045E', '60N060E', '60N075E', '60N090E', '60N105E', '60N120E', '60N135E', '60N150E', '60N165E'],   # noqa: E501
#               'W': ['60N015W', '60N030W', '60N045W', '60N060W', '60N075W', '60N090W', '60N105W', '60N120W', '60N135W', '60N150W', '60N165W', '60N180W']},  # noqa: E501
#        '75': {'E': ['75N000E', '75N015E', '75N030E', '75N045E', '75N060E', '75N075E', '75N090E', '75N105E', '75N120E', '75N135E', '75N150E', '75N165E'],   # noqa: E501
#               'W': ['75N015W', '75N030W', '75N045W', '75N060W', '75N075W', '75N090W', '75N105W', '75N120W', '75N135W', '75N150W', '75N165W', '75N180W']}}, # noqa: E501
#  'S': {'15': {'E': ['15S000E', '15S015E', '15S030E', '15S045E', '15S060E', '15S075E', '15S090E', '15S105E', '15S120E', '15S135E', '15S150E', '15S165E'],   # noqa: E501
#               'W': ['15S015W', '15S030W', '15S045W', '15S060W', '15S075W', '15S090W', '15S105W', '15S120W', '15S135W', '15S150W', '15S165W', '15S180W']},  # noqa: E501
#        '30': {'E': ['30S000E', '30S015E', '30S030E', '30S045E', '30S060E', '30S075E', '30S090E', '30S105E', '30S120E', '30S135E', '30S150E', '30S165E'],   # noqa: E501
#               'W': ['30S015W', '30S030W', '30S045W', '30S060W', '30S075W', '30S090W', '30S105W', '30S120W', '30S135W', '30S150W', '30S165W', '30S180W']},  # noqa: E501
#        '45': {'E': ['45S000E', '45S015E', '45S030E', '45S045E', '45S060E', '45S075E', '45S090E', '45S105E', '45S120E', '45S135E', '45S150E', '45S165E'],   # noqa: E501
#               'W': ['45S015W', '45S030W', '45S045W', '45S060W', '45S075W', '45S090W', '45S105W', '45S120W', '45S135W', '45S150W', '45S165W', '45S180W']},  # noqa: E501
#        '60': {'E': ['60S000E', '60S015E', '60S030E', '60S045E', '60S060E', '60S075E', '60S090E', '60S105E', '60S120E', '60S135E', '60S150E', '60S165E'],   # noqa: E501
#               'W': ['60S015W', '60S030W', '60S045W', '60S060W', '60S075W', '60S090W', '60S105W', '60S120W', '60S135W', '60S150W', '60S165W', '60S180W']},  # noqa: E501
#        '75': {'E': ['75S000E', '75S015E', '75S030E', '75S045E', '75S060E', '75S075E', '75S090E', '75S105E', '75S120E', '75S135E', '75S150E', '75S165E'],   # noqa: E501
#               'W': ['75S015W', '75S030W', '75S045W', '75S060W', '75S075W', '75S090W', '75S105W', '75S120W', '75S135W', '75S150W', '75S165W', '75S180W']},  # noqa: E501
#        '90': {'E': ['90S000E', '90S015E', '90S030E', '90S045E', '90S060E', '90S075E', '90S090E', '90S105E', '90S120E', '90S135E', '90S150E', '90S165E'],   # noqa: E501
#               'W': ['90S015W', '90S030W', '90S045W', '90S060W', '90S075W', '90S090W', '90S105W', '90S120W', '90S135W', '90S150W', '90S165W', '90S180W']}}} # noqa: E501


# Transfom the dict into an ordered (defined by us) array: combine N with S, reversing the N latitudes to start
# from 75 down to 00, and combine the longitudes for each latitud reversing W with E, meaning each longitud starts
# from 180W down to 015W and transitions to 000E up to 165E
final_array = []
for lat_num in sorted(tiles_list["N"].keys(), reverse=True):
    final_array.append(
        sorted(tiles_list["N"][lat_num]["W"], reverse=True) + sorted(tiles_list["N"][lat_num]["E"]),
    )

for lat_num in sorted(tiles_list["S"].keys()):
    final_array.append(
        sorted(tiles_list["S"][lat_num]["W"], reverse=True) + sorted(tiles_list["S"][lat_num]["E"]),
    )

# RESULT (only stems)
# [['75N180W', '75N165W', '75N150W', '75N135W', '75N120W', '75N105W', '75N090W', '75N075W', '75N060W', '75N045W', '75N030W', '75N015W', '75N000E', '75N015E', '75N030E', '75N045E', '75N060E', '75N075E', '75N090E', '75N105E', '75N120E', '75N135E', '75N150E', '75N165E'],  # noqa: E501
#  ['60N180W', '60N165W', '60N150W', '60N135W', '60N120W', '60N105W', '60N090W', '60N075W', '60N060W', '60N045W', '60N030W', '60N015W', '60N000E', '60N015E', '60N030E', '60N045E', '60N060E', '60N075E', '60N090E', '60N105E', '60N120E', '60N135E', '60N150E', '60N165E'],  # noqa: E501
#  ['45N180W', '45N165W', '45N150W', '45N135W', '45N120W', '45N105W', '45N090W', '45N075W', '45N060W', '45N045W', '45N030W', '45N015W', '45N000E', '45N015E', '45N030E', '45N045E', '45N060E', '45N075E', '45N090E', '45N105E', '45N120E', '45N135E', '45N150E', '45N165E'],  # noqa: E501
#  ['30N180W', '30N165W', '30N150W', '30N135W', '30N120W', '30N105W', '30N090W', '30N075W', '30N060W', '30N045W', '30N030W', '30N015W', '30N000E', '30N015E', '30N030E', '30N045E', '30N060E', '30N075E', '30N090E', '30N105E', '30N120E', '30N135E', '30N150E', '30N165E'],  # noqa: E501
#  ['15N180W', '15N165W', '15N150W', '15N135W', '15N120W', '15N105W', '15N090W', '15N075W', '15N060W', '15N045W', '15N030W', '15N015W', '15N000E', '15N015E', '15N030E', '15N045E', '15N060E', '15N075E', '15N090E', '15N105E', '15N120E', '15N135E', '15N150E', '15N165E'],  # noqa: E501
#  ['00N180W', '00N165W', '00N150W', '00N135W', '00N120W', '00N105W', '00N090W', '00N075W', '00N060W', '00N045W', '00N030W', '00N015W', '00N000E', '00N015E', '00N030E', '00N045E', '00N060E', '00N075E', '00N090E', '00N105E', '00N120E', '00N135E', '00N150E', '00N165E'],  # noqa: E501
#  ['15S180W', '15S165W', '15S150W', '15S135W', '15S120W', '15S105W', '15S090W', '15S075W', '15S060W', '15S045W', '15S030W', '15S015W', '15S000E', '15S015E', '15S030E', '15S045E', '15S060E', '15S075E', '15S090E', '15S105E', '15S120E', '15S135E', '15S150E', '15S165E'],  # noqa: E501
#  ['30S180W', '30S165W', '30S150W', '30S135W', '30S120W', '30S105W', '30S090W', '30S075W', '30S060W', '30S045W', '30S030W', '30S015W', '30S000E', '30S015E', '30S030E', '30S045E', '30S060E', '30S075E', '30S090E', '30S105E', '30S120E', '30S135E', '30S150E', '30S165E'],  # noqa: E501
#  ['45S180W', '45S165W', '45S150W', '45S135W', '45S120W', '45S105W', '45S090W', '45S075W', '45S060W', '45S045W', '45S030W', '45S015W', '45S000E', '45S015E', '45S030E', '45S045E', '45S060E', '45S075E', '45S090E', '45S105E', '45S120E', '45S135E', '45S150E', '45S165E'],  # noqa: E501
#  ['60S180W', '60S165W', '60S150W', '60S135W', '60S120W', '60S105W', '60S090W', '60S075W', '60S060W', '60S045W', '60S030W', '60S015W', '60S000E', '60S015E', '60S030E', '60S045E', '60S060E', '60S075E', '60S090E', '60S105E', '60S120E', '60S135E', '60S150E', '60S165E'],  # noqa: E501
#  ['75S180W', '75S165W', '75S150W', '75S135W', '75S120W', '75S105W', '75S090W', '75S075W', '75S060W', '75S045W', '75S030W', '75S015W', '75S000E', '75S015E', '75S030E', '75S045E', '75S060E', '75S075E', '75S090E', '75S105E', '75S120E', '75S135E', '75S150E', '75S165E'],  # noqa: E501
#  ['90S180W', '90S165W', '90S150W', '90S135W', '90S120W', '90S105W', '90S090W', '90S075W', '90S060W', '90S045W', '90S030W', '90S015W', '90S000E', '90S015E', '90S030E', '90S045E', '90S060E', '90S075E', '90S090E', '90S105E', '90S120E', '90S135E', '90S150E', '90S165E']]  # noqa: E501

# Transform the multi dimensional array into a dask.array:
#   First convert all values (paths to GETAS files) into a 'get_tile' delayed task
#   Second concatenate all the columns from each latitude
#   Third concatenate all the rows (different latitudes) into one single da
for i, row in enumerate(final_array):
    for j, _ in enumerate(row):
        final_array[i][j] = da.from_delayed(
            (get_tile)(final_array[i][j]),
            (1800, 1800),
            dtype=np.dtype(">i2"),
        )  # 1 step
    final_array[i] = da.concatenate(final_array[i], axis=1)  # 2 step
final_array = da.concatenate(final_array, axis=0)  # 3 step

# RESULT
# da<concatenate, shape=(21600, 43200), dtype=int16, chunksize=(1800, 1800), chunktype=numpy.ndarray>


# Create the xr.Dataset
file_name = gen_static_adf_name("S00__ADF_GETAS")
out_dir = environ.get("ADF_OUTPUT", "../../scratch")
output_path = osp.join(out_dir, file_name)

root_attrs = {
    "stac_version": "1.1.0",
    "stac_extensions": ["https://stac-extensions.github.io/product/v1.0.0/schema.json"],
    "id": Path(file_name).stem,
    "type": "Feature",
    "properties": {
        "created": datetime.today().strftime(REF_DATE_FORMAT),
        "description": (
            "Global Earth Topography And Sea Surface Elevation with spatial resolution of 30 arcseconds, version 3"
        ),
        "product:type": "ADF_GETAS",
    },
}

getas_attrs = {"units": "m"}

output_data = xr.Dataset(
    {"getasse_height": (("latitude", "longitude"), final_array, getas_attrs)},
    coords={
        "longitude": ("longitude", np.arange(start=-648000 + 15, stop=648000 + 15, step=30, dtype=np.int32)),
        "latitude": ("latitude", np.arange(start=324000 - 15, stop=-324000 - 15, step=-30, dtype=np.int32)),
    },
    attrs=root_attrs,
)

output_data.coords["longitude"].attrs = {"long_name": "longitudes of getasse", "units": "arcseconds"}
output_data.coords["latitude"].attrs = {"long_name": "latitudes of getasse", "units": "arcseconds"}

# Duplicate first column
output_data = duplicate_first_longitude(output_data)

# Dump to Zarr File
output_data.to_zarr(output_path)

print(file_name)
