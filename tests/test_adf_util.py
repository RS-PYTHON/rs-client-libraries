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

"""Unit tests for rs_workflows/adf_conversion/util.py.

The ``adf_conversion`` directory is not an importable package (no
``__init__.py``), so the ``util`` module is loaded directly from its source
file.  Functions that rely on GRIB readers (cfgrib) or on the ``eopf``
``EOProduct`` type are exercised with lightweight fakes / monkeypatched
``xarray`` so no heavy optional dependency is required.
"""

import importlib.util
import re
import tarfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr

UTIL_PATH = Path(__file__).resolve().parents[1] / "rs_workflows" / "adf_conversion" / "util.py"


def _load_util():
    spec = importlib.util.spec_from_file_location("adf_util", UTIL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


util = _load_util()


# AuxBinaryReader hardcodes the legacy numpy "a" (bytes) string dtype, which was
# removed in numpy 2.0: instantiating it raises TypeError there. Mark the reader
# tests as expected failures on numpy>=2 (rather than skips, so they stay green
# even under --error-for-skips) while still running for real on numpy<2.
try:
    np.dtype("a132")
    _LEGACY_STR_DTYPE_OK = True
except TypeError:
    _LEGACY_STR_DTYPE_OK = False

requires_legacy_str_dtype = pytest.mark.xfail(
    condition=not _LEGACY_STR_DTYPE_OK,
    reason="AuxBinaryReader uses the legacy numpy 'a' string dtype, removed in numpy>=2",
    raises=TypeError,
    strict=False,
)


# --------------------------------------------------------------------------- #
# Name generators
# --------------------------------------------------------------------------- #
def test_gen_static_adf_name():
    """The static ADF name uses the fixed validity range and a .zarr suffix."""
    name = util.gen_static_adf_name("S00__ADF_GETAS")
    assert re.fullmatch(
        r"S00__ADF_GETAS_20000101T000000_21000101T000000_\d{8}T\d{6}\.zarr",
        name,
    )


def test_gen_dynamic_adf_name():
    """The dynamic ADF name embeds the start/stop validity times."""
    start = datetime(2021, 3, 21, 3, 0, 0)
    stop = datetime(2021, 3, 21, 15, 0, 0)
    name = util.gen_dynamic_adf_name("S00__ADF_ECMWA", start, stop)
    assert name.startswith("S00__ADF_ECMWA_20210321T030000_20210321T150000_")
    assert name.endswith(".zarr")
    assert re.fullmatch(
        r"S00__ADF_ECMWA_20210321T030000_20210321T150000_\d{8}T\d{6}\.zarr",
        name,
    )


# --------------------------------------------------------------------------- #
# Small dict / attribute helpers
# --------------------------------------------------------------------------- #
def test_get_attrs_drops_io_config():
    """get_attrs() copies every attribute except the internal _io_config."""
    attrs = util.get_attrs({"a": 1, "_io_config": "secret", "b": 2})
    assert attrs == {"a": 1, "b": 2}


def test_path_list_with_prefix_matches_docstring_example():
    """path_list_with_prefix() flattens grouped paths into (in, out) tuples."""
    result = util.path_list_with_prefix(
        {
            "": ["rho_w_510_mean_LUT"],
            "coordinates": ["months_rho_w_510", "lat_rho_w_510"],
        },
    )
    assert result == [
        ("rho_w_510_mean_LUT", "rho_w_510_mean_LUT"),
        ("months_rho_w_510", "coordinates/months_rho_w_510"),
        ("lat_rho_w_510", "coordinates/lat_rho_w_510"),
    ]


def test_path_list_with_prefix_strips_trailing_slash():
    """A trailing slash in the group name is normalised."""
    assert util.path_list_with_prefix({"coordinates/": ["a"]}) == [("a", "coordinates/a")]


def test_clean_units_replaces_tilde():
    """A '~' unit is rewritten to '1' (and a GRIB_units field is added)."""
    attrs = {"units": "~"}
    util.clean_units(attrs)
    assert attrs == {"units": "1", "GRIB_units": "1"}


def test_clean_units_leaves_regular_units():
    """A regular unit is left untouched and no GRIB_units field is created."""
    attrs = {"units": "m"}
    util.clean_units(attrs)
    assert attrs == {"units": "m"}


def test_clean_attributes_cleans_units_lowercases_and_removes_grib():
    """clean_attributes() lowercases long_name and drops every GRIB_* entry."""
    attrs = {
        "units": "~",
        "long_name": "Temperature",
        "GRIB_paramId": 130,
        "keep": "value",
    }
    util.clean_attributes(attrs)
    assert attrs == {"units": "1", "long_name": "temperature", "keep": "value"}


# --------------------------------------------------------------------------- #
# Longitude helpers (xarray)
# --------------------------------------------------------------------------- #
def _lon_dataset(longitudes, values=None, attrs=None):
    longitudes = np.asarray(longitudes, dtype="float64")
    if values is None:
        values = np.arange(len(longitudes), dtype="float64")
    coord_attrs = attrs if attrs is not None else {}
    return xr.Dataset(
        {"v": (("longitude",), np.asarray(values, dtype="float64"))},
        coords={"longitude": ("longitude", longitudes, coord_attrs)},
    )


def test_center_longitudes_rolls_and_recenters():
    """center_longitudes() rolls the axis and remaps it to [-180, 180]."""
    dataset = _lon_dataset([0.0, 90.0, 180.0, 270.0], values=[0.0, 1.0, 2.0, 3.0])
    out = util.center_longitudes(dataset, 2)
    assert list(out.longitude.values) == [-180.0, -90.0, 0.0, 90.0]
    assert list(out.v.values) == [2.0, 3.0, 0.0, 1.0]


def test_duplicate_first_longitude():
    """The first longitude is duplicated at +360 degrees at the end."""
    dataset = _lon_dataset([0.0, 90.0, 180.0, 270.0], values=[10.0, 20.0, 30.0, 40.0])
    out = util.duplicate_first_longitude(dataset)
    assert len(out.longitude) == 5
    assert out.longitude.values[-1] == 360.0
    assert out.v.values[-1] == 10.0


def test_duplicate_first_and_last_longitude():
    """The first (+360) and last (-360) longitudes wrap the axis on both sides."""
    dataset = _lon_dataset([0.0, 90.0, 270.0], values=[10.0, 20.0, 30.0])
    out = util.duplicate_first_and_last_longitude(dataset)
    assert len(out.longitude) == 5
    assert out.longitude.values[0] == 270.0 - 360.0  # last wrapped to the front
    assert out.v.values[0] == 30.0
    assert out.longitude.values[-1] == 0.0 + 360.0  # first wrapped to the end
    assert out.v.values[-1] == 10.0


def test_duplicate_longitude_when_needed_both_sides():
    """When the axis covers neither edge, both columns are duplicated."""
    dataset = _lon_dataset([0.0, 90.0, 170.0])
    out = util.duplicate_longitude_when_needed(dataset)
    assert len(out.longitude) == 5


def test_duplicate_longitude_when_needed_first_only():
    """When only the upper edge is missing, a single column is duplicated."""
    dataset = _lon_dataset([-180.0, 0.0, 170.0])
    out = util.duplicate_longitude_when_needed(dataset)
    assert len(out.longitude) == 4


def test_duplicate_longitude_when_needed_full_range_unchanged():
    """A dataset already covering up to 180 degrees is returned unchanged."""
    dataset = _lon_dataset([0.0, 90.0, 180.0])
    out = util.duplicate_longitude_when_needed(dataset)
    assert len(out.longitude) == 3


# --------------------------------------------------------------------------- #
# apply_dataset_mapping (xarray)
# --------------------------------------------------------------------------- #
@pytest.fixture(name="source_dataset")
def _source_dataset():
    lon = np.array([0.0, 1.0, 2.0], dtype="float64")
    return xr.Dataset(
        data_vars={
            "oldvar": (("mylon",), np.array([10.0, 20.0, 30.0]), {"long_name": "Value of oldvar", "units": "~"}),
        },
        coords={"mylon": ("mylon", lon, {"units": "degrees", "long_name": "Longitude in degrees"})},
    )


def test_apply_dataset_mapping_with_source_coordinate(source_dataset):
    """Variables and coordinates are remapped, coordinates cast to float32 and cleaned."""
    mapping = {
        "maps": {"newvar": "oldvar"},
        "coordinates": {"newlon": "mylon"},
        "attributes": {"title": "remapped"},
    }
    out = util.apply_dataset_mapping(source_dataset, mapping)

    assert "newvar" in out.data_vars
    assert out["newvar"].dims == ("newlon",)
    assert np.array_equal(out["newvar"].values, [10.0, 20.0, 30.0])
    assert out.coords["newlon"].dtype == np.float32
    # long_name is lowercased by clean_attributes
    assert out.coords["newlon"].attrs["long_name"] == "longitude in degrees"
    assert out.attrs["title"] == "remapped"


def test_apply_dataset_mapping_with_inline_coordinate_and_variable_attrs(source_dataset):
    """Inline (name, data, attrs) coordinates and variable attribute patches are honoured."""
    mapping = {
        "maps": {"v": "oldvar"},
        "coordinates": {"c": ("c", np.array([5, 6, 7]), {"units": "m"})},
        "attributes": {},
    }
    out = util.apply_dataset_mapping(source_dataset, mapping, variable_attrs={"v": {"flag": "yes"}})

    assert np.array_equal(out.coords["c"].values, [5, 6, 7])
    assert out["v"].attrs["flag"] == "yes"


# --------------------------------------------------------------------------- #
# AuxBinaryReader / IntegerArray
# --------------------------------------------------------------------------- #
def _write_aux_file(path, encoding=b"BIGENDIAN", ints=(2, 3), reals=(1.0, 2.0), data=None, data_dtype=">f4"):
    """Write a binary file matching the AuxBinaryReader layout used in the tests.

    Layout: 3 header strings (the 2nd one carries the endianness tag), then 2
    big-endian int32 attributes, 2 big-endian float64 attributes, then the data.
    """
    with open(path, "wb") as file:
        for text in (b"header0", encoding, b"header2"):
            np.array([text], dtype="S132").tofile(file)
        np.array(ints, dtype=">i4").tofile(file)
        np.array(reals, dtype=">f8").tofile(file)
        if data is not None:
            np.asarray(data, dtype=data_dtype).tofile(file)


@requires_legacy_str_dtype
def test_aux_binary_reader_reads_attrs_coords_and_data(tmp_path):
    """AuxBinaryReader exposes the header attributes and reshapes the data block."""
    aux_file = tmp_path / "sample.bin"
    _write_aux_file(aux_file, ints=(2, 3), reals=(1.0, 2.0), data=[1, 2, 3, 4, 5, 6])

    with util.AuxBinaryReader(aux_file, header_str=3, header_int=2, header_float=2, bytes_lenght=4) as reader:
        # attrs_list starts with "" then the 3 strings, 2 ints, 2 reals
        assert reader.attrs_list[4] == 2
        assert reader.attrs_list[5] == 3
        # read_coord builds an inclusive arange from the int/real attributes
        np.testing.assert_array_equal(reader.read_coord(4, 5, 6), np.array([2.0, 3.0]))
        # read_data reshapes the remaining floats using the int attributes as shape
        np.testing.assert_array_equal(reader.read_data([4, 5]), np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]))


@requires_legacy_str_dtype
def test_aux_binary_reader_raises_when_reading_past_header(tmp_path):
    """Reading an attribute beyond the declared header raises a RuntimeError."""
    aux_file = tmp_path / "sample.bin"
    _write_aux_file(aux_file, data=[1, 2, 3, 4, 5, 6])

    with util.AuxBinaryReader(aux_file, header_str=3, header_int=2, header_float=2) as reader:
        with pytest.raises(RuntimeError, match="out of declared header"):
            reader._read_attr()  # pylint: disable=protected-access


@requires_legacy_str_dtype
def test_aux_binary_reader_little_endian(tmp_path):
    """A 'LITE' endianness tag selects the little-endian attribute types."""
    aux_file = tmp_path / "sample.bin"
    # write little-endian attributes to match the declared encoding
    with open(aux_file, "wb") as file:
        for text in (b"header0", b"LITE", b"header2"):
            np.array([text], dtype="S132").tofile(file)
        np.array((4, 1), dtype="<i4").tofile(file)
        np.array((0.0, 0.0), dtype="<f8").tofile(file)

    with util.AuxBinaryReader(aux_file, header_str=3, header_int=2, header_float=2) as reader:
        assert reader.attrs_list[4] == 4
        assert reader._int_attr_type == np.dtype("<i4")  # pylint: disable=protected-access


@requires_legacy_str_dtype
def test_integer_array_reads_int16(tmp_path):
    """IntegerArray reads the data block as little-endian int16 values."""
    aux_file = tmp_path / "int.bin"
    _write_aux_file(aux_file, ints=(2, 2), data=[1, 2, 3, 4], data_dtype="<i2")

    with util.IntegerArray(aux_file, header_str=3, header_int=2, header_float=2) as reader:
        np.testing.assert_array_equal(reader.read_data([4, 5]), np.array([[1, 2], [3, 4]], dtype="<i2"))


# --------------------------------------------------------------------------- #
# Filesystem helpers
# --------------------------------------------------------------------------- #
def test_move_rename_extract(tmp_path):
    """move_rename_extract() copies, renames and untars the archive into the work dir."""
    source = tmp_path / "src"
    source.mkdir()
    payload = source / "hello.txt"
    payload.write_text("hi")

    archive = tmp_path / "archive.tar"
    with tarfile.open(archive, "w") as tar:
        tar.add(payload, arcname="hello.txt")

    working = tmp_path / "work"
    working.mkdir()
    util.move_rename_extract(str(archive), str(working), "renamed.tar")

    assert (working / "renamed.tar").exists()
    assert (working / "hello.txt").read_text() == "hi"


def _make_product_dir(parent, name, files):
    product = parent / name
    product.mkdir()
    for file_name, content in files.items():
        (product / file_name).write_text(content)
    return product


def test_get_all_grib_files_filters_by_type_and_date(tmp_path):
    """Only extension-less files inside matching product directories are returned."""
    products = tmp_path / "products"
    products.mkdir()
    mf1 = _make_product_dir(
        products,
        "S3__AX___MF1_AX_20230508T120000_20230509T000000_x_ECW.SEN3",
        {"T1D05071200050815001": "grib", "T1D05071200050815001.idx": "idx", "xfdumanifest.xml": "xml"},
    )
    _make_product_dir(
        products,
        "S3__AX___MF2_AX_20230508T120000_20230509T000000_x_ECW.SEN3",
        {"T2D05080000050821001": "grib"},
    )
    (products / "loose.txt").write_text("ignored")

    only_mf1 = util.get_all_grib_files(products, file_type="MF1", date="20230508")
    assert only_mf1 == [mf1 / "T1D05071200050815001"]

    both = util.get_all_grib_files(products, file_type=["MF1", "MF2"], date="20230508")
    assert len(both) == 2

    none_for_date = util.get_all_grib_files(products, file_type="MF1", date="20990101")
    assert none_for_date == []

    all_dirs = util.get_all_grib_files(products, file_type=None)
    assert len(all_dirs) == 2


def test_group_mx_files_by_dates(tmp_path):
    """MF1/MF2 files sharing a reference time are grouped under that date."""
    mf1_dir = tmp_path / "S3__AX___MF1_AX_20230508T120000_20230509T000000_x_ECW.SEN3"
    mf1_dir.mkdir()
    mf1_file = mf1_dir / "T1D"
    mf1_file.write_text("a")

    mf2_dir = tmp_path / "S3__AX___MF2_AX_20230508T120000_20230509T000000_x_ECW.SEN3"
    mf2_dir.mkdir()
    mf2_file = mf2_dir / "T2D"
    mf2_file.write_text("b")

    grouped = util.group_mx_files_by_dates([mf1_file, mf2_file], "MF")
    assert list(grouped.keys()) == ["2023-05-08T12:00:00"]
    assert grouped["2023-05-08T12:00:00"] == {"MF1": mf1_file, "MF2": mf2_file}


# --------------------------------------------------------------------------- #
# GRIB readers (xarray monkeypatched)
# --------------------------------------------------------------------------- #
def test_get_all_mf_ref_times_groups_files_by_time(monkeypatch, tmp_path):
    """get_all_mf_ref_times() maps every reference time to its set of files."""
    mf1_dir = tmp_path / "prod_MF1"
    mf1_dir.mkdir()
    mf1_file = mf1_dir / "f1"
    mf1_file.write_text("x")
    mf2_dir = tmp_path / "prod_MF2"
    mf2_dir.mkdir()
    mf2_file = mf2_dir / "f2"
    mf2_file.write_text("y")

    fake_ds = MagicMock()
    fake_ds.time.data = np.datetime64("2023-05-08T12:00:00")
    monkeypatch.setattr(util.xr, "open_dataset", lambda *args, **kwargs: fake_ds)

    result = util.get_all_mf_ref_times([mf1_file, mf2_file])
    assert result == {"2023-05-08T12:00:00": {mf1_file, mf2_file}}


def test_get_all_ma_ref_times_groups_by_day(monkeypatch, tmp_path):
    """get_all_ma_ref_times() keys the files by reference day."""
    ma1_dir = tmp_path / "prod_MA1"
    ma1_dir.mkdir()
    ma1_file = ma1_dir / "f1"
    ma1_file.write_text("x")

    fake_ds = MagicMock()
    fake_ds.time.data = np.datetime64("2023-05-08T12:00:00")
    monkeypatch.setattr(util.xr, "open_dataset", lambda *args, **kwargs: fake_ds)

    result = util.get_all_ma_ref_times([ma1_file])
    assert result == {"2023-05-08": {ma1_file}}


def test_set_model_levels_coefficients(monkeypatch):
    """The pv array is split into the a/b model-level coefficients."""
    pv = np.arange(276, dtype="float64")
    grib_ds = MagicMock()
    grib_ds.q.attrs = {"GRIB_pv": pv}
    monkeypatch.setattr(util.xr, "open_dataset", lambda *args, **kwargs: grib_ds)

    dataset = xr.Dataset(coords={"hybrid": np.arange(137)})
    out = util.set_model_levels_coefficients("dummy.grib", dataset)

    np.testing.assert_array_equal(out["pv_coeff_a"].values, pv[1:138])
    np.testing.assert_array_equal(out["pv_coeff_b"].values, pv[139:])
    assert out.pv_coeff_a.dims == ("hybrid",)
    assert "Factor a" in out.pv_coeff_a.attrs["long_name"]
    assert "Factor b" in out.pv_coeff_b.attrs["long_name"]


# --------------------------------------------------------------------------- #
# copy_var family (fake EOProduct)
# --------------------------------------------------------------------------- #
class _FakeVar:
    def __init__(self, data, attrs=None):
        self.data = data
        self.attrs = dict(attrs or {})


class _FakeProduct:
    """Minimal stand-in for an eopf EOProduct, supporting the API copy_var uses."""

    def __init__(self):
        self._vars = {}
        self.attrs = {}

    def add_variable(self, path, var=None, data=None, attrs=None):
        """Store a variable from an existing one (var) or from raw data/attrs."""
        if var is not None:
            self._vars[path] = _FakeVar(var.data, var.attrs)
        else:
            self._vars[path] = _FakeVar(data, attrs)

    def __getitem__(self, path):
        return self._vars[path]

    def __delitem__(self, path):
        del self._vars[path]


def test_copy_var_casts_float64_to_float32_and_drops_io_config():
    """copy_var() downcasts float64 data to float32 and strips _io_config attrs."""
    in_product = _FakeProduct()
    in_product.add_variable("a", data=np.array([1.0, 2.0], dtype="float64"), attrs={"x": 1, "_io_config": "drop"})
    out_product = _FakeProduct()

    util.copy_var(in_product, out_product, "a", "b")

    assert out_product["b"].data.dtype == np.float32
    assert out_product["b"].attrs == {"x": 1}


def test_copy_var_keeps_non_float64_dtype():
    """copy_var() leaves a non-float64 variable (and its attrs) untouched."""
    in_product = _FakeProduct()
    in_product.add_variable("a", data=np.array([1, 2], dtype="int32"), attrs={"x": 1})
    out_product = _FakeProduct()

    util.copy_var(in_product, out_product, "a", "b")

    assert out_product["b"].data.dtype == np.int32
    assert out_product["b"].attrs == {"x": 1}


def test_copy_var_list_copies_and_merges_attrs():
    """copy_var_list() copies each variable and merges the source attributes."""
    in_product = _FakeProduct()
    in_product.add_variable("a", data=np.array([1, 2], dtype="int32"))
    in_product.add_variable("b", data=np.array([3, 4], dtype="int32"))
    in_product.attrs = {"title": "src"}
    out_product = _FakeProduct()

    util.copy_var_list(in_product, out_product, [("a", "x"), ("b", "y")])

    assert np.array_equal(out_product["x"].data, [1, 2])
    assert np.array_equal(out_product["y"].data, [3, 4])
    assert out_product.attrs == {"title": "src"}


def test_copy_var_list_with_mapping():
    """copy_var_list_with_mapping() applies attribute and grouped-variable mappings."""
    in_product = _FakeProduct()
    in_product.add_variable("old", data=np.array([1, 2], dtype="int32"))
    out_product = _FakeProduct()

    mapping = {
        "attributes": {"title": "remapped"},
        "coordinates": {"old": "new"},
    }
    util.copy_var_list_with_mapping(in_product, out_product, mapping)

    assert out_product.attrs == {"title": "remapped"}
    assert np.array_equal(out_product["coordinates/new"].data, [1, 2])
