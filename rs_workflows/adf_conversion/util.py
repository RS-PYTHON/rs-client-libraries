#!/usr/bin/env python
# coding: utf8
#
# Vendored from the ESA EOPF Copernicus project (CPM ADF auxiliary-data-file):
# https://gitlab.eopf.copernicus.eu/cpm/adf-auxiliary-data-file/-/tree/main/scripts/
#
# Copyright 2022-2025 ESA
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
Util functions for ADF scripts
"""

import logging

import os.path as osp
import shutil
from datetime import datetime
from pathlib import Path
from tarfile import TarFile
from typing import TYPE_CHECKING, Any, Iterable, Literal, Optional

import numpy as np
import pandas as pd
import xarray as xr

REF_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
LEGACY_DATE_FORMAT = "%Y%m%dT%H%M%S"

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core import EOProduct

REF_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

UNIT_FROM_DEGREE = {
    "degrees": 1,
    "degree_east": 1,
    "degree_north": 1,
    "arcminutes": 60,
    "arcseconds": 3600,
}


def set_model_levels_coefficients(grib_path, ds):
    """
    Two factors (a and b) are used to calculate the model level pressure, knowing the surface
    pressure: pmodlev(Pa) = a + b*surface_pressure(Pa).
    From an input GRIB file, retrieve the array containing these coefficients, and split it into two
    separate arrays (a and b), skipping level 0.
    :param grib_path: Path to a GRIB file with the coefficients
    :param ds: Dataset containing the initial array
    :return:
    """
    grib_ds = xr.open_dataset(
        grib_path,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": "q"}},
        read_keys=["pv"],
    )

    # a coefficient
    a_coeff = grib_ds.q.attrs["GRIB_pv"][1:138]
    ds["pv_coeff_a"] = (["hybrid"], a_coeff)
    ds.pv_coeff_a.attrs["unit"] = "Pa"
    ds.pv_coeff_a.attrs["long_name"] = (
        "Factor a to calculate the model level pressure with " "pmodlev(Pa) = a + b*surface_pressure(Pa)"
    )

    # b coefficient
    b_coeff = grib_ds.q.attrs["GRIB_pv"][139:]
    ds["pv_coeff_b"] = (["hybrid"], b_coeff)
    ds.pv_coeff_a.attrs["unit"] = "1"
    ds.pv_coeff_b.attrs["long_name"] = (
        "Factor b to calculate the model level pressure with " "pmodlev(Pa) = a + b*surface_pressure(Pa)"
    )

    return ds


def regrid_dataset(ds):
    """
    TODO: doc
    """
    # Find unique latitudes
    lat_unique, lat_indices = np.unique(ds.latitude, return_inverse=True)

    # We don't use raw unique latitudes: only the ones found near the equator
    min_latitude = abs(ds.latitude).min()
    result = ds.latitude == min_latitude
    lon_unique = ds.longitude[result].values
    lon_indices = np.array([np.argmin(abs(lon_unique - lon)) for lon in ds.longitude.values])

    # Create an empty grid with these lon/lat
    grid_2d = np.full((len(lat_unique), len(lon_unique)), np.nan, dtype=np.float32)

    def make_3d_grid(var_name):
        return np.full((len(ds[var_name]), len(lat_unique), len(lon_unique)), np.nan, dtype=np.float32)

    # A dataset can have multiple "altitude" coordinates: isobaric, hybrid ... with different sizes
    # We create all the 3d grids corresponding to all the altitude coordinates
    altitude_dimensions = set(ds.coords) - {"longitude", "latitude", "time"}
    all_3d_grids = {var_name: make_3d_grid(var_name) for var_name in altitude_dimensions}

    for var_name in ds.data_vars:
        if len(ds[var_name].shape) == 1:  # 1d -> 2d array
            reshaped_data = np.copy(grid_2d)

            # Transpose the data: (longitude, latitude) in grib, but we want (latitude, longitude)
            reshaped_data[lat_indices, lon_indices] = ds[var_name]

            new_var = xr.DataArray(
                reshaped_data,
                dims=("latitude", "longitude"),
                coords={"latitude": lat_unique, "longitude": lon_unique},
                attrs=ds[var_name].attrs,
            )
            ds[var_name] = new_var

        elif len(ds[var_name].shape) == 2:  # 2d -> 3d array
            # Find the altitude levels coordinate (isobaric, hybrid...)
            altitude_levels_name = ds[var_name].dims[0]
            reshaped_data = np.copy(all_3d_grids[altitude_levels_name])

            # Transpose the data: (alt, longitude, latitude) in grib, but we want (alt, latitude, longitude)
            reshaped_data[:, lat_indices, lon_indices] = ds[var_name]

            new_var = xr.DataArray(
                reshaped_data,
                dims=(altitude_levels_name, "latitude", "longitude"),
                coords={
                    altitude_levels_name: ds[altitude_levels_name],
                    "latitude": lat_unique,
                    "longitude": lon_unique,
                },
                attrs=ds[var_name].attrs,
            )
            ds[var_name] = new_var

        else:
            raise np.exceptions.AxisError(
                f"Expecting only either 2d or 3d variables, but the "
                f"variable '{var_name}' has the following shape: "
                f"{ds[var_name].shape}",
            )

    return ds


def reshape_dataset(ds):
    ds = regrid_dataset(ds)

    # Convert longitudes from [0, 360] to [-180, 180]
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
    ds = ds.roll(longitude=len(ds.longitude) // 2, roll_coords=True)
    # Add a column at 180° with data from -180°
    ds = xr.concat([ds, ds.sel(longitude=-180).assign_coords(longitude=180)], dim="longitude")
    ds["longitude"] = ds["longitude"].astype(np.float32)
    return ds

def get_all_grib_files(
    products_dir: Path,
    file_type: list[str] | str | None = None,
    date: Optional[str] = "",
) -> list[Path]:
    """
    Retrieve all files from a directory containing subdirectories representing products, with grib
    files inside. For example:
    .
    ├── S3__AX___MF1_AX_20230508T120000_20230509T000000_20230507T180214___________________ECW_O_NR_001.SEN3
    │   ├── T1D05071200050815001  <----------- file to retrieve
    │   ├── T1D05071200050815001.02ccc.idx
    │   └── xfdumanifest.xml
    ├── S3__AX___MF1_AX_20230508T180000_20230509T060000_20230508T060754___________________ECW_O_NR_001.SEN3
    │   ├── T1D05080000050821001  <----------- file to retrieve
    │   ├── T1D05080000050821001.02ccc.idx
    │   └── xfdumanifest.xml
    ├── S3__AX___MF2_AX_20230508T120000_20230509T000000_20230507T180250___________________ECW_O_NR_001.SEN3
    │   ├── T2D05071200050815001  <----------- file to retrieve
    │   ├── T2D05071200050815001.923a8.idx
    │   └── xfdumanifest.xml
    ├── S3__AX___MF2_AX_20230508T180000_20230509T060000_20230508T060832___________________ECW_O_NR_001.SEN3
    │   ├── T2D05080000050821001  <----------- file to retrieve
    │   ├── T2D05080000050821001.923a8.idx
    │   └── xfdumanifest.xml
    ...

    :param products_dir: Directory containing the products
    :param file_type: List of sub-strings to search for in the products names (e.g. ["MF1", "MF2])
    :param date: Date to search for in the products names
    :return: A list with all grib files corresponding to the set filters (file type and date)
    """
    print(f"Call get_all_grib_files with products_dir='{products_dir}', file_type='{file_type}' and date='{date}'")


    if file_type is None:
        file_type = [""]
    elif isinstance(file_type, str):
        file_type = [file_type]
    all_files = []

    for product in products_dir.iterdir():
        file_type_condition = any((file_type in product.name for file_type in file_type))
        if product.is_dir() and file_type_condition and date in product.name:
            for product_item in product.iterdir():
                if product_item.suffix == "":
                    all_files.append(product_item)

    print(f"return '{all_files}'")
    return all_files


def group_mx_files_by_dates(product_files: Iterable[Path], prefix: Literal["MF", "MA"]) -> dict[str, dict[str, Path]]:
    """
    Usable for MF1/MF2 couples or MA1/MA2 couples. They are referenced as MX1 and MX2.
    Return a dictionary grouping MX1 and MX2 files by dates. It expects a list of 8 paths to files
    with the same reference time: 4 MX1 files and 4 MX2 files.
    :param product_files:
    :param prefix: Controls whether to use forecast or analysis files
    :return: A dictionary with 4 keys (time steps) and dictionaries as values, indicating paths to
             files.
    For example, with forecast files:
    {
        date_step_1: {"MF1": "path_to_MF1_with_date1", "MF2": "path_to_MF2_with_date1"},
        date_step_2: {"MF1": "path_to_MF1_with_date2", "MF2": "path_to_MF2_with_date2"},
        date_step_3: {"MF1": "path_to_MF1_with_date3", "MF2": "path_to_MF2_with_date3"},
        date_step_4: {"MF1": "path_to_MF1_with_date4", "MF2": "path_to_MF2_with_date4"},
    }
    """
    MX1 = f"{prefix}1"
    MX2 = f"{prefix}2"
    grouped_files = {}
    for file in product_files:
        product_name = file.parent.name
        date = product_name.split("_")[7]
        date = pd.to_datetime(str(date)).strftime(REF_DATE_FORMAT)

        if not grouped_files.get(date, None):
            grouped_files[date] = {}

        if MX1 in product_name:  # MF1 / MA1
            grouped_files[date][MX1] = file
        else:  # MF2 / MA2
            grouped_files[date][MX2] = file

    # Sort based on the reference date
    return dict(sorted(grouped_files.items(), key=lambda x: x[0]))


def get_all_mf_ref_times(all_files: list[Path]) -> dict[str, set[Path]]:
    """
    For a given list of MF1 and MF2 legacy files, return a dictionary mapping all discovered
    reference times to the list of corresponding files.
    :param all_files: List of all MF1 and MF2 files
    :return: A dictionary mapping reference times to files: {ref_time: {set of corresponding files}, ...}
    """
    output_dict = {}

    for input_file in all_files:
        if "MF1" in input_file.parent.name:
            ds = xr.open_dataset(input_file, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "t"}})
        else:
            ds = xr.open_dataset(input_file, engine="cfgrib")
        ref_time = pd.to_datetime(str(ds.time.data)).strftime(REF_DATE_FORMAT)

        if ref_time not in output_dict.keys():
            output_dict[ref_time] = {input_file}
        else:
            output_dict[ref_time].add(input_file)

    # Sort the dictionary by reference date
    return dict(sorted(output_dict.items()))


def get_all_ma_ref_times(all_files: list[Path]) -> dict[str, set[Path]]:
    """
    For a given list of MA1 and MA2 legacy files, return a dictionary mapping all discovered
    reference times to the list of corresponding files. Reference time is the time dimension.
    :param all_files: List of all MF1 and MF2 files
    :return: A dictionary mapping reference times to files: {ref_time: {set of corresponding files}, ...}
    """
    output_dict = {}

    for input_file in all_files:
        if "MA1" in input_file.parent.name:
            ds = xr.open_dataset(input_file, engine="cfgrib", backend_kwargs={"filter_by_keys": {"shortName": "t"}})
        else:
            ds = xr.open_dataset(input_file, engine="cfgrib")
        ref_time = pd.to_datetime(str(ds.time.data)).date().strftime("%Y-%m-%d")

        if ref_time not in output_dict.keys():
            output_dict[ref_time] = {input_file}
        else:
            output_dict[ref_time].add(input_file)

    return output_dict


def move_rename_extract(file_path, working_dir, new_name):
    shutil.copy(file_path, working_dir)
    shutil.move(osp.join(working_dir, osp.basename(file_path)), osp.join(working_dir, new_name))
    archive = TarFile(osp.join(working_dir, new_name))
    archive.extractall(path=working_dir)


def gen_static_adf_name(base_name):
    now = datetime.now()
    date_frmt = now.strftime("%Y%m%dT%H%M%S")
    return f"{base_name}_20000101T000000_21000101T000000_{date_frmt}.zarr"


def gen_dynamic_adf_name(base_name: str, start_time: datetime, stop_time: datetime):
    time_format = "%Y%m%dT%H%M%S"

    creation_time = datetime.now().strftime(time_format)
    start_time = start_time.strftime(time_format)
    stop_time = stop_time.strftime(time_format)

    return f"{base_name}_{start_time}_{stop_time}_{creation_time}.zarr"


def get_attrs(in_product_attrs):
    attrs = {}
    for elem in in_product_attrs.keys():
        if elem != "_io_config":
            attrs[elem] = in_product_attrs[elem]
    return attrs


def copy_var(in_product: "EOProduct", out_product: "EOProduct", in_path: str, out_path: str) -> None:
    out_product.add_variable(out_path, in_product[in_path])
    if out_product[out_path].data.dtype == "float64":
        tmp_data = out_product[out_path].data.astype(dtype="float32")
        tmp_attrs = out_product[out_path].attrs
        out_product.__delitem__(out_path)
        out_product.add_variable(out_path, data=tmp_data, attrs=get_attrs(tmp_attrs))


def copy_var_list(in_product: "EOProduct", out_product: "EOProduct", path_list) -> None:
    for in_path, out_path in path_list:
        copy_var(
            in_product=in_product,
            out_product=out_product,
            in_path=in_path,
            out_path=out_path,
        )
    out_product.attrs.update(in_product.attrs)


def copy_var_list_with_mapping(in_product: "EOProduct", out_product: "EOProduct", mapping) -> None:
    for path in mapping:
        var_mappping = mapping.get(path)
        if path == "attributes":
            for attr in var_mappping:
                out_product.attrs[attr] = var_mappping.get(attr)
        else:
            for old_var_name in var_mappping:
                new_var_path = f"{path}/{var_mappping.get(old_var_name)}"
                copy_var(
                    in_product=in_product,
                    out_product=out_product,
                    in_path=old_var_name,
                    out_path=new_var_path,
                )


def path_list_with_prefix(path_list: dict[str, list[str]]):
    """Return a simple flat to group path list for copy_var.
    Fro exemple
    {
        "": ["rho_w_510_mean_LUT"],
        "coordinates": ["months_rho_w_510", "lat_rho_w_510"]
    }
    Become the list of (in_path, out_path) :
    [
        ("rho_w_510_mean_LUT","rho_w_510_mean_LUT"),
        ("months_rho_w_510","coordinates/months_rho_w_510"),
        ("lat_rho_w_510","coordinates/lat_rho_w_510")
    ]

    """
    out_list = []
    for group, paths in path_list.items():
        group = group.removesuffix("/")
        if group:
            group += "/"
        for path in paths:
            out_list.append((path, str(group + path)))
    return out_list


def clean_units(attrs):
    """
    Clean the "units" field by replacing '~' by '1'

    :param attrs: attributes mapping
    """
    unit = attrs.get("units", "")
    if unit == "~":
        attrs["units"] = "1"
        attrs["GRIB_units"] = "1"


def clean_attributes(attrs):
    """
    Clean an attributes mapping, clean the "units", remove "GRIB_XXX" entries, starting lowercase
    on long_name
    """
    clean_units(attrs)

    # Leading lowercase on long_name
    if "long_name" in attrs:
        name_value = attrs["long_name"]
        attrs["long_name"] = name_value[0].lower() + name_value[1:]

    # remove GRIB attributes
    keys = list(attrs)
    for key in keys:
        if key.startswith("GRIB_"):
            del attrs[key]


def center_longitudes(dataset, center) -> xr.Dataset:
    """
    Shift longitudes in the dataset

    :param dataset: input dataset
    :param center: index of longitude=0° in output dataset
    :return: dataset with centered longitudes
    """
    output = dataset.roll(longitude=center, roll_coords=True)
    # convert longitude to [-180.0, 180.0]
    longi_attrs = output.longitude.attrs
    output = output.assign_coords(longitude=((output.longitude + 180.0) % 360.0 - 180.0))
    output.longitude.attrs = longi_attrs
    return output


def duplicate_first_longitude(dataset: xr.Dataset) -> xr.Dataset:
    """
    Duplicate the first longitude in all variables and concatenate it at the end with
    longitude=first_longitude+360°.

    :param dataset: input dataset
    :return: dataset with duplicated first column
    """
    ref_dtype = dataset.longitude.dtype
    unit = dataset.longitude.attrs.get("units", "degrees")
    first_lon = dataset.longitude[0]
    first_col = dataset.sel(longitude=first_lon)

    # Computing the new longitude may change its dtype, so we re apply the one used in input
    first_col["longitude"] = (first_lon + (360 * UNIT_FROM_DEGREE[unit])).astype(ref_dtype)
    return xr.concat([dataset, first_col], "longitude")


def duplicate_first_and_last_longitude(dataset: xr.Dataset) -> xr.Dataset:
    """
    Duplicate the first and last longitude in all variables and concatenate them to make sure the
    longitude range [-180, +180] is covered. Longitude is assumed to be in degrees

    :param dataset: input dataset
    :return: dataset with duplicated first and last column
    """
    ref_dtype = dataset.longitude.dtype
    unit = dataset.longitude.attrs.get("units", "degrees")
    first_lon = dataset.longitude[0]
    last_lon = dataset.longitude[-1]

    # Computing the new longitude may change its dtype, so we re apply the one used in input
    first_col = dataset.sel(longitude=first_lon)
    last_col = dataset.sel(longitude=last_lon)

    first_col["longitude"] = (first_lon + (360 * UNIT_FROM_DEGREE[unit])).astype(ref_dtype)
    last_col["longitude"] = (last_lon - (360 * UNIT_FROM_DEGREE[unit])).astype(ref_dtype)

    output = xr.concat([last_col, dataset, first_col], "longitude")
    output.longitude.attrs = dataset.longitude.attrs
    return output


def duplicate_longitude_when_needed(dataset: xr.Dataset) -> xr.Dataset:
    """
    Detect when the longitude axis doesn't cover the full range [-180.0, +180.0] and duplicate the
    longitude columns accordingly

    :param dataset: input dataset
    :return: dataset with duplicated column (if needed)
    """

    unit = dataset.longitude.attrs.get("units", "degrees")
    first_lon = dataset.longitude[0]
    last_lon = dataset.longitude[-1]
    if last_lon < 180 * UNIT_FROM_DEGREE[unit]:
        # need first longitude
        if first_lon > -180 * UNIT_FROM_DEGREE[unit]:
            # need last longitude
            return duplicate_first_and_last_longitude(dataset)
        else:
            return duplicate_first_longitude(dataset)

    return dataset


def apply_dataset_mapping(
    source: xr.Dataset,
    mapping: dict,
    coord_attrs: dict | None = None,
    variable_attrs: dict | None = None,
) -> xr.Dataset:
    """
    Remap variables and coordinates from a source dataset to an output dataset

    :param source: input dataset
    :param mapping: mapping instructions (with fields "maps", "coordinates", "attributes")
    :param coord_attrs: Coordinates attributes to patch
    :param variable_attrs: Variables attributes to patch
    :return: remapped dataset
    """
    if coord_attrs is None:
        coord_attrs = {}
    if variable_attrs is None:
        variable_attrs = {}

    coord_list = list(mapping["coordinates"])
    data_var = {
        var: (
            coord_list,
            source[mapping["maps"][var]].data,
            source[mapping["maps"][var]].attrs,
        )
        for var in mapping["maps"]
    }

    # patch variable attributes
    for name in data_var:
        if name in variable_attrs:
            data_var[name][2].update(variable_attrs[name])

    # build next coordinates with their attributes
    coords = {}
    next_coords_attrs = {}
    for name in coord_list:
        coord_source = mapping["coordinates"][name]
        if isinstance(coord_source, str):
            # load coordinate from source
            source_data = source[coord_source].data
            if source_data.dtype == "float64":
                source_data = source_data.astype("float32")
            coords[name] = (name, source_data)
            next_coords_attrs[name] = {}
            for attr_name, attr_val in source[coord_source].attrs.items():
                if not isinstance(attr_val, str):
                    next_coords_attrs[name][attr_name] = attr_val
                    continue
                next_val = attr_val
                # patch input atttributes: sometimes the name of the old variable appears in the
                # description so we replace it by the new name
                for new_var, old_var in mapping["maps"].items():
                    next_val = next_val.replace(old_var, new_var)
                next_coords_attrs[name][attr_name] = next_val

        elif isinstance(coord_source, list) or isinstance(coord_source, tuple):
            # expect a tuple (name, data[, attrs])
            coords[name] = coord_source[:2]
            if len(coord_source) == 3:
                next_coords_attrs[name] = coord_source[2]
            else:
                next_coords_attrs[name] = {}

        # clean coordinate attributes
        clean_attributes(next_coords_attrs[name])

        # apply patches
        if name in coord_attrs:
            next_coords_attrs[name].update(coord_attrs[name])

    # Create the output xr.Dataset
    output_data = xr.Dataset(data_var, coords=coords, attrs=mapping["attributes"])

    # Apply new attributes
    for name in next_coords_attrs:
        output_data.coords[name].attrs = next_coords_attrs[name]

    return output_data


class AuxBinaryReader:
    def __init__(
        self,
        in_file_name: str,
        header_str: int = 10,
        header_int: int = 6,
        header_float: int = 6,
        bytes_lenght: int = 4,
    ):
        """
        Count of field of each type in header.
        """
        self.bytes_lenght = bytes_lenght
        self._header_str_size = header_str
        self._header_int_size = header_int
        self._header_real_size = header_float
        self._string_attr_type = np.dtype("a132")
        self._int_attr_type = None
        self._real_attr_type = None
        self._real_data_type = None
        self._seek = 0  # last field number read
        self.file = None
        self.in_file_name = in_file_name
        self.attrs_list = []

    def __enter__(self) -> "AuxBinaryReader":
        self.file = open(self.in_file_name, mode="rb")
        self.attrs_list = self._read_all_attrs()
        return self

    def __exit__(self, *args, **qargs) -> None:
        self.file.close()

    def _generate_types(self, big_endian: bool = True) -> None:
        """Set numpy type used for reading either to big or little endian."""
        if big_endian:
            endian_str = ">"
        else:
            endian_str = "<"
        self._int_attr_type = np.dtype(f"{endian_str}i4")  # 4 bytes  int, not actually sure it's unsigned.
        self._real_attr_type = np.dtype(f"{endian_str}f8")  # 8 bytes double/floats
        self._real_data_type = np.dtype(f"{endian_str}f{self.bytes_lenght}")

    def _read_attr(self) -> Any:
        """Read the next attribute."""
        self._seek += 1
        if self._seek <= self._header_str_size:
            dtype = self._string_attr_type
        elif self._seek <= self._header_str_size + self._header_int_size:
            dtype = self._int_attr_type
        else:
            if self._seek > self._header_str_size + self._header_int_size + self._header_real_size:
                raise RuntimeError("Trying ot read attrs out of declared header.")
            dtype = self._real_attr_type
        attr = np.fromfile(self.file, count=1, dtype=dtype)
        return attr[0]

    def _read_all_attrs(self) -> list[Any]:
        """Read the list of attributes. They are unnamed, this file format not being self describing."""
        attr_list = [""]  # start at 1.
        for i in range(self._header_str_size):
            attr_list.append(self._read_attr().decode("utf-8"))
        encoding = attr_list[2]
        self._generate_types(not encoding.startswith("LITE"))
        for i in range(self._header_int_size + self._header_real_size):
            attr_list.append(self._read_attr())
        return attr_list

    def read_coord(self, first_field: int, last_field: int, step_field: int) -> np.ndarray:
        """
        Construct a numpy array from interval read from file. The last coordinate value is expected
        to be included in the range.
        """
        return np.arange(
            self.attrs_list[first_field],
            self.attrs_list[last_field] + 0.5 * self.attrs_list[step_field],
            self.attrs_list[step_field],
        )

    def read_data(self, shape_fields: Iterable[int]) -> np.ndarray:
        """Construct a numpy array from data read from file."""
        data = np.fromfile(self.file, count=-1, dtype=self._real_data_type)
        shape = [self.attrs_list[field] for field in shape_fields]
        return np.reshape(data, shape)


# To read 2bytes little endian integer array :
class IntegerArray(AuxBinaryReader):
    def read_data(self, shape_fields: Iterable[int]) -> np.ndarray:
        """Construct a numpy array from data read from file."""
        data = np.fromfile(self.file, count=-1, dtype=np.dtype("<i2"))
        shape = [self.attrs_list[field] for field in shape_fields]
        return np.reshape(data, shape)
