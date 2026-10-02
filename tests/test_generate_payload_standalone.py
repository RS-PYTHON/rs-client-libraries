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

"""Tests for the standalone payload generator script."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pystac import Asset, Item, ItemCollection

from rs_workflows.flow_utils import AuxiliarySource
from scripts import generate_payload_standalone as standalone

RESOURCES = Path(__file__).parent / "resources"
S3_L0_TASKTABLE = RESOURCES / "TaskTable_S3_L0_generated_by_rs_python_v1.json"

STAC_DISCOVERY = {
    "id": "S3A_CADU",
    "geometry": None,
    "properties": {
        "start_datetime": "2025-01-01T10:00:00Z",
        "end_datetime": "2025-01-01T10:10:00Z",
        "platform": "sentinel-3a",
    },
}


@pytest.fixture(name="zarr_input")
def fixture_zarr_input(tmp_path) -> str:
    """Local zarr v3 product with its STAC metadata."""
    zarr_path = tmp_path / "S3A_CADU.zarr"
    zarr_path.mkdir()
    (zarr_path / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group", "attributes": {"stac_discovery": STAC_DISCOVERY}}),
    )
    return str(zarr_path)


def _aux_item(item_id: str, href: str) -> Item:
    item = Item(id=item_id, geometry=None, bbox=None, datetime=datetime(2025, 1, 1), properties={})
    item.add_asset("file", Asset(href=href))
    return item


@pytest.fixture(name="rs_client")
def fixture_rs_client(mocker) -> MagicMock:
    """Mock the rs-server clients used to search and stage the ADFS."""
    aux_item = _aux_item("aux1", "s3://catalog-bucket/aux/aux1.EOF")
    rs_client = MagicMock()
    rs_client.get_auxip_client.return_value.search.return_value = ItemCollection([aux_item])
    staging_client = rs_client.get_staging_client.return_value
    staging_client.run_staging.return_value = {"host": {}}
    staging_client.wait_for_jobs.return_value = {"host": {"status": "successful"}}
    rs_client.get_catalog_client.return_value.get_items.side_effect = lambda **_: [aux_item]
    mocker.patch.object(standalone, "RsClient", return_value=rs_client)
    return rs_client


@pytest.fixture(name="s3_env")
def fixture_s3_env(monkeypatch):
    """Secrets referenced by the default storage configuration."""
    for prefix in ("S3", "S3_DEMS1"):
        for name in ("ACCESSKEY", "SECRETKEY", "ENDPOINT", "REGION"):
            monkeypatch.setenv(f"{prefix}_{name}", f"{prefix}-{name}".lower())


@pytest.mark.asyncio
async def test_read_zarr_attributes_v2_and_missing(tmp_path):
    """Zarr v2 attributes are read from .zattrs, and a missing zarr returns empty attributes."""
    zarr_path = tmp_path / "v2.zarr"
    zarr_path.mkdir()
    (zarr_path / ".zattrs").write_text(json.dumps({"stac_discovery": STAC_DISCOVERY}))
    assert (await standalone.read_zarr_attributes(str(zarr_path)))["stac_discovery"] == STAC_DISCOVERY
    assert await standalone.read_zarr_attributes(str(tmp_path / "missing.zarr")) == {}


def test_build_input_item():
    """The input item is built from the stac_discovery metadata, with a zarr asset."""
    item = standalone.build_input_item("s3://bucket/product.zarr/", {"stac_discovery": STAC_DISCOVERY})
    assert item.id == "S3A_CADU"
    assert item.datetime == datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    assert item.assets["product"].href == "s3://bucket/product.zarr"
    assert item.assets["product"].media_type == standalone.ZARR_MEDIA_TYPE

    # Without metadata, the id is deduced from the path
    assert standalone.build_input_item("s3://bucket/other.zarr", {}).id == "other"


def test_derive_external_variables():
    """External variables are deduced from the input product properties."""
    item = standalone.build_input_item("s3://bucket/product.zarr", {"stac_discovery": STAC_DISCOVERY})
    variables = standalone.derive_external_variables([item])
    assert variables["start_datetime"] == datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    assert variables["end_datetime"] == datetime(2025, 1, 1, 10, 10, tzinfo=timezone.utc)
    assert variables["satellite"] == "sentinel-3a"
    assert variables["instrument_mode"] is None


def test_guess_input_name():
    """The input name is the single candidate, or the one whose regex matches."""
    task_table = {
        "io": [
            {"name": "A", "store_params": {"regex": r".*_A_.*\.zarr"}},
            {"name": "B", "store_params": {"regex": r".*_B_.*\.zarr"}},
        ],
    }
    assert standalone.guess_input_name("s3://b/x.zarr", ["A"], task_table) == "A"
    assert standalone.guess_input_name("s3://b/S3_B_1.zarr", ["A", "B"], task_table) == "B"
    with pytest.raises(ValueError, match="NAME=PATH"):
        standalone.guess_input_name("s3://b/S3_C_1.zarr", ["A", "B"], task_table)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("NAME=s3://bucket/a=b.zarr", ("NAME", "s3://bucket/a=b.zarr")),
        ("s3://bucket/a=b.zarr", (None, "s3://bucket/a=b.zarr")),
        ("/local/path.zarr", (None, "/local/path.zarr")),
    ],
)
def test_parse_key_value(value, expected):
    """Paths containing '=' are not split."""
    assert standalone.parse_key_value(value) == expected


def test_parse_mappings():
    """Parse the AUX and output mapping arguments."""
    aux = standalone.parse_aux_mapping("*=s3-aux:catalog")
    assert (aux.product_type, aux.collection_name, aux.source) == ("*", "s3-aux", AuxiliarySource.CATALOG)
    output = standalone.parse_output_mapping("S03ISPS=S03ISP:s3-l0")
    assert (output.name, output.product_type, output.collection_name) == ("S03ISPS", "S03ISP", "s3-l0")


def test_call_with_retries(mocker):
    """The function is retried on exception."""
    mocker.patch.object(standalone.time, "sleep")
    func = MagicMock(side_effect=[RuntimeError("boom"), "ok"])
    assert standalone.call_with_retries(func, 1, 0, "test") == "ok"
    with pytest.raises(RuntimeError):
        standalone.call_with_retries(MagicMock(side_effect=RuntimeError("boom")), 1, 0, "test")


def test_main_generates_payload(tmp_path, zarr_input, rs_client, s3_env):  # pylint: disable=unused-argument
    """End-to-end: stage the ADFS and write the payload, without Prefect."""
    output = tmp_path / "payload.yaml"
    exit_code = standalone.main(
        [
            "--tasktable",
            str(S3_L0_TASKTABLE),
            "--pipeline",
            "s3_l0_full",
            "--input",
            zarr_input,
            "--aux-mapping",
            "*=s3-aux",
            "--output-mapping",
            "S03ISPS=S03ISP:s3-l0",
            "--owner-id",
            "owner",
            "--output-bucket",
            "out-bucket",
            "--output",
            str(output),
        ],
    )
    assert exit_code == 0

    # The ADFS were searched with the input product datetimes and staged into the mapped collection
    search_kwargs = rs_client.get_auxip_client.return_value.search.call_args.kwargs
    assert "2025-01-01T10:00:00.000Z" in json.dumps(search_kwargs["stac_filter"])
    assert rs_client.get_staging_client.return_value.run_staging.call_args.args[1] == "s3-aux"

    payload = yaml.safe_load(output.read_text())
    io = payload["I/O"]
    assert io["input_products"][0]["id"] == "S3ACADUS"
    assert io["input_products"][0]["path"] == zarr_input
    assert {adf["id"]: adf["path"] for adf in io["adfs"]} == {
        "osf": "s3://catalog-bucket/aux/aux1.EOF",
        "fro": "s3://catalog-bucket/aux/aux1.EOF",
    }
    output_paths = {product["id"]: product["path"] for product in io["output_products"]}
    assert output_paths["S03ISPS"].startswith("s3://out-bucket/owner/s3-l0/")
    # Secrets are masked by default
    assert io["input_products"][0]["store_params"]["storage_options"]["key"] == "********"


def test_main_dry_run(tmp_path, zarr_input, rs_client, s3_env):  # pylint: disable=unused-argument
    """In dry run mode, the ADFS are only searched."""
    output = tmp_path / "payload.yaml"
    args = ["-t", str(S3_L0_TASKTABLE), "-p", "s3_l0_full", "-i", f"S3ACADUS={zarr_input}"]
    args += ["--aux-mapping", "*=s3-aux", "--owner-id", "owner", "--output-bucket", "b", "--dry-run", "-o", str(output)]
    assert standalone.main(args) == 0
    rs_client.get_staging_client.return_value.run_staging.assert_not_called()
    assert output.exists()


def test_main_requires_aux_mapping(zarr_input, rs_client, s3_env):  # pylint: disable=unused-argument
    """An error code is returned when the AUX mapping is missing."""
    args = ["-t", str(S3_L0_TASKTABLE), "-p", "s3_l0_full", "-i", zarr_input, "--owner-id", "owner"]
    assert standalone.main(args) == 1
