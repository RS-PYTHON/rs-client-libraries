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
Generation script for S00__ADF_ECMWF
"""
import argparse
import shutil
from datetime import datetime
from os import environ
from pathlib import Path

import dask
import numpy as np
import pandas as pd
import xarray as xr
from util import (  # set_model_levels_coefficients,
    REF_DATE_FORMAT,
    clean_attributes,
    gen_dynamic_adf_name,
    get_all_grib_files,
    get_all_mf_ref_times,
    group_mx_files_by_dates,
    reshape_dataset,
)


def correct_attrs(input_data, dict_of_vars):
    # Apply some corrections on some attributes for each variable
    for var in list(dict_of_vars.values()):
        clean_attributes(input_data[var].attrs)
        # Data type conversion
        if input_data[var].data.dtype == "float64":
            input_data[var] = input_data[var].astype("float32")
        # Corrections on variable attributes
        attrs = input_data[var].attrs
        attrs.pop("GRIB_pl", False)

        if attrs.get("standard_name", None) == "unknown":
            attrs.pop("standard_name")

        for attr in attrs:
            attrs[attr] = "Kelvin" if attrs[attr] == "K" else attrs[attr]
            attrs[attr] = 1 if attrs[attr] == "(0 - 1)" else attrs[attr]


# Data source paths on the S3 bucket. You have to provide the following files in input of this script:
# AX___MF1_AX -> common:common/S3BOLCIdataset/S3__AX___MF1_AX_20210121T000000_20210121T120000_20210120T180707___________________ECW_O_NR_001.SEN3.zip # noqa: E501
# AX___MF2_AX -> common:common/S3BOLCIdataset/S3__AX___MF2_AX_20210121T000000_20210121T120000_20210120T180720___________________ECW_O_NR_001.SEN3.zip # noqa: E501


def open_mf1_product(mf1_path):
    # Alternative way to open the GRIB file and avoid the conflicting "r" variable: load separately "q",
    # "t" (which are isobaric) and all surface based  and depthBelowLandLayer measurement
    input_data_mf1_q = xr.open_dataset(mf1_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "q"}})
    input_data_mf1_t = xr.open_dataset(mf1_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "t"}})
    input_data_mf1_r = xr.open_dataset(mf1_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "r"}})
    input_data_mf1_surf = xr.open_dataset(
        mf1_path,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"typeOfLevel": "surface"}},
    )
    input_data_mf1_depth = xr.open_dataset(
        mf1_path,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"typeOfLevel": "depthBelowLandLayer"}},
    )

    input_data_mf1_ax1 = xr.merge(
        [input_data_mf1_surf, input_data_mf1_depth, input_data_mf1_q, input_data_mf1_t],
        compat="identical",
    )

    # Create a new 'time' dimension using the valid_time values:
    # - Legacy 'time' -> new attribute: reference time
    # - Legacy 'valid_time' -> new dimension: time
    input_data_mf1_ax1 = input_data_mf1_ax1.drop_vars("time")
    input_data_mf1_ax1 = input_data_mf1_ax1.assign_coords(time=input_data_mf1_ax1["valid_time"])
    input_data_mf1_ax1 = input_data_mf1_ax1.drop_vars("valid_time")

    # Create and fill the output xarray dataset
    clean_attributes(input_data_mf1_ax1.attrs)

    dict_of_vars1 = {  # legacy_variable_name: new_variable_name
        "isobaricInhPa": "isobaric",
        "longitude": "longitude",
        "latitude": "latitude",
        "time": "time",
        "asn": "asn",
        "tcc": "tcc",
        "skt": "skt",
        "swvl1": "swvl1",
        "sd": "sd",
        "v10": "v10",
        "tco3": "tco3",
        "sp": "sp",
        "q": "q_isobaric",
        "t": "t_isobaric",
        "z": "z",
        "tcwv": "tcwv",
        "msl": "mslp",
        "u10": "u10",
        "sst": "sst",
        "d2m": "d2m",
        "siconc": "sic",
        "t2m": "t2m",
    }

    # Rename variables and dims
    input_data_mf1_ax1 = input_data_mf1_ax1.rename_vars(dict_of_vars1)
    input_data_mf1_ax1 = input_data_mf1_ax1.swap_dims(
        {k: dict_of_vars1[k] if k in list(dict_of_vars1.keys()) else k for k in list(input_data_mf1_ax1.dims)},
    )
    # Only keep necessary variables
    input_data_mf1_ax1 = input_data_mf1_ax1.drop_vars(
        [x for x in list(input_data_mf1_ax1.variables) if x not in list(dict_of_vars1.values())],
    )

    # inject "r" at 850hPa or at 950hPa depending on the version of the legacy ADF
    ref_pressure_value = input_data_mf1_r.load().isobaricInhPa.values
    ref_pressure_index = np.where(input_data_mf1_ax1.isobaric.data == ref_pressure_value)[0][0]
    r_isobaric = xr.DataArray(
        name="r_isobaric",
        data=dask.array.full(input_data_mf1_ax1.q_isobaric.shape, 999999.0, dtype="float32"),
        attrs={
            "long_name": "relative humidity",
            "units": "%",
        },
        dims=["isobaric", "values"],
    )
    r_isobaric.data[ref_pressure_index, :] = input_data_mf1_r.r.data
    input_data_mf1_ax1["r_isobaric"] = r_isobaric

    # Corrections on variable attributes
    correct_attrs(input_data_mf1_ax1, dict_of_vars1)

    return input_data_mf1_ax1


def open_mf2_product(mf2_path):
    # AX___MF2_AX
    input_data_mf2_ax = xr.open_dataset(mf2_path, engine="cfgrib")
    input_data_mf2_ax = input_data_mf2_ax.drop_vars("time")
    input_data_mf2_ax = input_data_mf2_ax.assign_coords(time=input_data_mf2_ax["valid_time"])
    input_data_mf2_ax = input_data_mf2_ax.drop_vars("valid_time")
    dict_of_vars3 = {
        "hybrid": "hybrid",
        "longitude": "longitude",
        "latitude": "latitude",
        "time": "time",
        "q": "q_hybrid",
        "t": "t_hybrid",
    }  # legacy_variable_name: new_variable_name
    # Rename variables and dims
    input_data_mf2_ax = input_data_mf2_ax.rename_vars(dict_of_vars3)
    input_data_mf2_ax = input_data_mf2_ax.swap_dims(
        {k: dict_of_vars3[k] if k in list(dict_of_vars3.keys()) else k for k in list(input_data_mf2_ax.dims)},
    )
    # Only keep necessary variables
    input_data_mf2_ax = input_data_mf2_ax.drop_vars(
        [x for x in list(input_data_mf2_ax.variables) if x not in list(dict_of_vars3.values())],
    )

    # Assign empty r_hybrid
    r_hybrid = xr.DataArray(
        name="r_hybrid",
        data=dask.array.full(input_data_mf2_ax.q_hybrid.shape, 999999.0, dtype="float32"),
        attrs={
            "long_name": "relative humidity",
            "units": "%",
        },
        dims=["hybrid", "values"],
    )
    input_data_mf2_ax["r_hybrid"] = r_hybrid

    # Corrections on variable attributes
    correct_attrs(input_data_mf2_ax, dict_of_vars3)

    return input_data_mf2_ax


def merge_mf1_mf2_files(ds_mf1, ds_mf2):
    merged_dataset = xr.merge([ds_mf1, ds_mf2])
    # Remove non stac attributes
    for attribute_name in ["Conventions", "institution", "history"]:
        del merged_dataset.attrs[attribute_name]

    # Set the right units for lon/lat
    merged_dataset.longitude.attrs["units"] = "degree_east"
    merged_dataset.latitude.attrs["units"] = "degree_north"

    return merged_dataset


def main(data_directory, tmp_dir=None, keep_tmp_dir=False):
    # We discard hybrid data, contained in MF2 files, but keep the code commented if needed, see
    # https://gitlab.eopf.copernicus.eu/cpm/adf-auxiliary-data-file/-/merge_requests/30#note_67577
    # files = get_all_grib_files(data_directory, file_type=["MF1", "MF2"])
    files = get_all_grib_files(data_directory, file_type=["MF1"])
    files_by_ref_date = get_all_mf_ref_times(files)
    # Filter only "full" products: 4 MF1 files
    full_products = {
        ref_date: products_paths for ref_date, products_paths in files_by_ref_date.items() if len(products_paths) == 4
    }
    print(f"Found {len(full_products)} full products with the following reference times: {list(full_products.keys())}")

    if not tmp_dir:
        tmp_dir = Path("./work_dir")
    tmp_dir.mkdir(exist_ok=True)

    for reference_time, product_paths in full_products.items():

        # Merge MF1 and MF2 files
        product_files = group_mx_files_by_dates(files_by_ref_date[reference_time], "MF")
        product_tmp_dir = tmp_dir / f"{reference_time}_tmp_dir"
        product_tmp_dir.mkdir(exist_ok=True)
        tmp_files = []
        for date, file_paths in product_files.items():
            # merged_ds = merge_mf1_mf2_files(
            #     reshape_dataset(open_mf1_product(file_paths["MF1"])),
            #     reshape_dataset(open_mf2_product(file_paths["MF2"])),
            # )
            merged_ds = reshape_dataset(open_mf1_product(file_paths["MF1"]))
            out_name = product_tmp_dir / f"{date}_merge_tmp.zarr"
            merged_ds.to_zarr(out_name, mode="w")
            tmp_files.append(out_name)

        # Open and stack temporary files
        datasets_to_stack = [xr.open_zarr(file) for file in tmp_files]
        output_ds = xr.concat(datasets_to_stack, dim="time")

        # Rechunk the data to use complete time and elevation dimensions (when possible)
        chunk_size_3d = {"time": -1, "longitude": "auto", "latitude": "auto"}
        chunk_size_hybrid = {"time": -1, "hybrid": -1, "longitude": "auto", "latitude": "auto"}
        chunk_size_isobaric = {"time": -1, "isobaric": -1, "longitude": "auto", "latitude": "auto"}
        for var in output_ds.data_vars:
            if output_ds[var].ndim == 3:
                output_ds[var] = output_ds[var].chunk(chunk_size_3d)
            elif output_ds[var].ndim == 4:
                if "hybrid" in output_ds[var].dims:
                    output_ds[var] = output_ds[var].chunk(chunk_size_hybrid)
                else:
                    output_ds[var] = output_ds[var].chunk(chunk_size_isobaric)

        # # Add model levels coefficients as variables
        # mf2_file = list(product_files.values())[0]["MF2"]  # use the first found mf2 file
        # output_ds = set_model_levels_coefficients(mf2_file, output_ds)

        # Set attributes
        time_start = pd.to_datetime(str(output_ds.time.data[0]))
        time_end = pd.to_datetime(str(output_ds.time.data[-1]))
        file_name = gen_dynamic_adf_name(base_name="S00__ADF_ECMWF", start_time=time_start, stop_time=time_end)

        output_ds.attrs["stac_version"] = "1.1.0"
        output_ds.attrs["stac_extensions"] = [
            "https://stac-extensions.github.io/forecast/v0.1.0/schema.json",
            "https://stac-extensions.github.io/product/v1.0.0/schema.json",
        ]
        output_ds.attrs["id"] = file_name.rsplit(".", 1)[0]
        output_ds.attrs["type"] = "Feature"
        output_ds.attrs["properties"] = {
            "description": "ECMWF forecast data",
            "created": datetime.today().strftime(REF_DATE_FORMAT),
            "forecast:reference_datetime": reference_time,
            "start_datetime": time_start.strftime(REF_DATE_FORMAT),
            "end_datetime": time_end.strftime(REF_DATE_FORMAT),
            "product:type": "ADF_ECMWF",
        }
        output_ds.time.encoding["units"] = f"hours since {time_start}"

        # Old coordinates from legacy files are stored in the encoding and appear as attributes in the
        # metadata, but they are not needed so they are removed
        for var_name in output_ds.variables:
            output_ds[var_name].encoding.pop("coordinates", None)

        # Write the output file
        out_dir = environ.get("ADF_OUTPUT", "../../scratch")
        out_path = Path(out_dir) / file_name
        output_ds.to_zarr(out_path, mode="w")
        print(f"Successfully wrote {out_path}")
        if not keep_tmp_dir:
            shutil.rmtree(product_tmp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("data_dir", type=Path, help="Path to the data directory")

    parser.add_argument(
        "--working_dir",
        "-wd",
        type=Path,
        default=Path.cwd() / "tmp_files",
        help="Path to the working directory (default: './tmp_files')",
    )

    parser.add_argument(
        "--keep_working_dir",
        "-k",
        type=bool,
        default=False,
        help="Whether or not to keep temporary files",
    )

    args = parser.parse_args()

    if not args.data_dir.is_dir():
        raise ValueError(f"data_dir is not a valid directory: {args.data_dir}")
    if not args.working_dir.is_dir():
        raise ValueError(f"working_dir is not a valid directory: {args.working_dir}")

    main(args.data_dir, args.working_dir)
