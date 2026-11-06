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

"""Unit tests for the Zarr v2/v3 helpers."""

import pytest
import zarr

from rs_workflows.on_demand_conversion import read_zarr_stac_item
from rs_workflows.utils import zarr_utils

ATTRIBUTES = {
    "stac_discovery": {
        "type": "Feature",
        "id": "S01SIWSLC_TEST",
        "geometry": None,
        "bbox": None,
        "properties": {"datetime": "2024-01-01T00:00:00Z", "product:type": "S01SIWSLC"},
    },
}


@pytest.fixture(name="zarr_store", params=[2, 3], ids=["zarr_v2", "zarr_v3"])
def _zarr_store(request, tmp_path):
    """Write a Zarr store with root attributes and a sub-group, in the requested format."""
    store = tmp_path / "S01SIWSLC_TEST.zarr"
    group = zarr.open_group(str(store), mode="w", zarr_format=request.param)
    group.attrs.update(ATTRIBUTES)
    group.create_group("measurements").attrs["nested"] = True
    return store, request.param


def test_get_zarr_storage_options(monkeypatch):
    """S3 stores use the workflow S3 credentials, local stores don't need any option."""
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")  # nosec B105: test-only fake secret
    monkeypatch.setenv("S3_ENDPOINT", "http://endpoint")
    monkeypatch.setenv("S3_REGION", "region")

    assert zarr_utils.get_zarr_storage_options("/local/P.zarr") is None
    assert zarr_utils.get_zarr_storage_options("s3://b/P.zarr") == {
        "key": "access",
        "secret": "secret",
        "client_kwargs": {"endpoint_url": "http://endpoint", "region_name": "region"},
    }


def test_open_zarr_group_passes_s3_options(monkeypatch, mocker):
    """S3 Zarr stores are opened read-only, with auto-detected format and S3 credentials."""
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")  # nosec B105: test-only fake secret
    monkeypatch.setenv("S3_ENDPOINT", "http://endpoint")
    monkeypatch.setenv("S3_REGION", "region")
    open_group = mocker.patch("rs_workflows.utils.zarr_utils.zarr.open_group")

    zarr_utils.open_zarr_group("s3://b/out/P.zarr/")

    open_group.assert_called_once_with(
        store="s3://b/out/P.zarr",
        mode="r",
        zarr_format=None,
        storage_options=zarr_utils.get_zarr_storage_options("s3://b/out/P.zarr"),
        use_consolidated=False,
    )


def test_read_zarr_attributes(zarr_store):
    """Root attributes are read the same way from Zarr v2 and v3 stores."""
    store, zarr_format = zarr_store

    assert zarr_utils.open_zarr_group(str(store)).metadata.zarr_format == zarr_format
    assert zarr_utils.read_zarr_attributes(str(store)) == ATTRIBUTES


def test_is_zarr_group(zarr_store, tmp_path):
    """Only Zarr groups are identified as Zarr stores, whatever their Zarr format."""
    store, _ = zarr_store
    zarr.create_array(str(tmp_path / "array.zarr"), shape=(2,), dtype="i4")
    (tmp_path / "not_a_group").mkdir()
    (tmp_path / "file.json").write_text("{}")

    assert zarr_utils.is_zarr_group(str(store))
    assert zarr_utils.is_zarr_group(str(store / "measurements"))
    assert not zarr_utils.is_zarr_group(str(tmp_path / "array.zarr"))
    assert not zarr_utils.is_zarr_group(str(tmp_path / "not_a_group"))
    assert not zarr_utils.is_zarr_group(str(tmp_path / "file.json"))
    assert not zarr_utils.is_zarr_group(str(tmp_path / "missing.zarr"))


def test_read_zarr_attributes_raises_on_missing_store(tmp_path):
    """An explicit error is raised if the Zarr store doesn't exist."""
    with pytest.raises(FileNotFoundError):
        zarr_utils.read_zarr_attributes(str(tmp_path / "missing.zarr"))


def test_read_zarr_stac_item(zarr_store):
    """The on-demand conversion builds the STAC item from Zarr v2 and v3 outputs."""
    store, _ = zarr_store
    item = read_zarr_stac_item(f"{store}/")
    assert item.id == "S01SIWSLC_TEST"
    assert item.properties["product:type"] == "S01SIWSLC"
    assert item.assets["S01SIWSLC_TEST"].href == str(store)
