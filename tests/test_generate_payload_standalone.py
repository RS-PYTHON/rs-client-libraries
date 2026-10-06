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
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

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


@pytest.mark.parametrize("dry_run", [False, True])
def test_main_create_collections(mocker, tmp_path, zarr_input, rs_client, s3_env, dry_run):  # pylint: disable=W0613
    """The collections of the AUX and output mappings are created at startup, except in dry run mode."""
    create_collection = mocker.patch.object(
        standalone,
        "check_and_create_collection",
        SimpleNamespace(fn=AsyncMock()),
    ).fn
    args = ["-t", str(S3_L0_TASKTABLE), "-p", "s3_l0_full", "-i", f"S3ACADUS={zarr_input}", "--owner-id", "owner"]
    args += ["--aux-mapping", "*=s3-aux", "--aux-mapping", "OL_1_EO__AX=s3-adf:auxip"]
    args += ["--output-mapping", "S03ISPS=S03ISP:s3-l0", "--output-bucket", "b", "--create-collections"]
    args += ["-o", str(tmp_path / "payload.yaml")] + (["--dry-run"] if dry_run else [])
    assert standalone.main(args) == 0
    created = [call.args[1] for call in create_collection.await_args_list]
    assert created == ([] if dry_run else ["s3-adf", "s3-aux", "s3-l0"])


def test_main_requires_aux_mapping(zarr_input, rs_client, s3_env):  # pylint: disable=unused-argument
    """An error code is returned when the AUX mapping is missing."""
    args = ["-t", str(S3_L0_TASKTABLE), "-p", "s3_l0_full", "-i", zarr_input, "--owner-id", "owner"]
    assert standalone.main(args) == 1


def _adf_stager(rs_client, aux_mappings: list[str], **kwargs) -> standalone.AdfsStager:
    """AdfsStager with the S3 L0 task table queries and the given AUX mappings."""
    dpr_input = SimpleNamespace(
        satellite="sentinel-3a",
        auxiliary_product_to_collection_identifier=[standalone.parse_aux_mapping(m) for m in aux_mappings],
    )
    task_table = json.loads(S3_L0_TASKTABLE.read_text())
    return standalone.AdfsStager(rs_client, "owner", dpr_input, task_table, 0, 0, **kwargs)  # type: ignore[arg-type]


def _input_adfs(product_type: str) -> dict:
    """ADFS input with a single alternative requesting the given product type."""
    parameters = {
        "product_type": product_type,
        "start_datetime": "2025-01-01T10:00:00Z",
        "end_datetime": "2025-01-01T10:10:00Z",
        "dTa": 0,
        "dTb": 0,
    }
    alternative = {"order": 1, "timeout_seconds": 0, "query": {"name": "LatestValCover", "parameters": parameters}}
    return {"name": "eop", "type": "filename", "alternatives": [alternative]}


def _search_product_type(search_call) -> str:
    """Return the product:type value of a STAC search call."""
    return next(arg["args"][1] for arg in search_call.kwargs["stac_filter"]["args"] if arg["op"] == "=")


def test_select_legacy_aux_collection_and_source():
    """An explicit mapping is used as is, otherwise the legacy files are staged from the AUXIP."""
    stager = _adf_stager(MagicMock(), ["OL_1_EO__AX=legacy:cdse", "*=s3-adf:catalog"])
    assert stager.select_legacy_aux_collection_and_source("OL_1_EO__AX") == ("legacy", AuxiliarySource.CDSE)
    assert stager.select_legacy_aux_collection_and_source("OL_1_CAL_AX") == ("s3-adf", AuxiliarySource.AUXIP)


@pytest.fixture(name="adf_conversion")
def fixture_adf_conversion(mocker, tmp_path) -> SimpleNamespace:
    """Mock the download, conversion, upload and collection creation of the ADF conversion."""
    zarr_path = tmp_path / "S3A_ADF_OLEOP.zarr"
    zarr_path.mkdir()
    (zarr_path / ".zattrs").write_text(
        json.dumps(
            {
                "id": "S3A_ADF_OLEOP",
                "properties": {"start_datetime": "2025-01-01T00:00:00Z", "end_datetime": "2025-01-02T00:00:00Z"},
            },
        ),
    )

    async def upload(_product_path, stac_item, s3_dir):
        stac_item.assets["data"].href = f"{s3_dir}/{stac_item.id}.zarr/"

    mocks = SimpleNamespace(
        download=AsyncMock(),
        run_script=MagicMock(return_value=[zarr_path]),
        upload=AsyncMock(side_effect=upload),
        create_collection=AsyncMock(),
    )
    mocker.patch.object(standalone, "download_and_extract_assets_task", SimpleNamespace(fn=mocks.download))
    mocker.patch.object(standalone, "run_adf_script", SimpleNamespace(fn=mocks.run_script))
    mocker.patch.object(standalone, "upload_adf_product", mocks.upload)
    mocker.patch.object(standalone, "check_and_create_collection", SimpleNamespace(fn=mocks.create_collection))
    return mocks


@pytest.mark.asyncio
async def test_process_input_adfs_converts_legacy_adf(adf_conversion):
    """A missing ADF is generated from the legacy auxiliary files, published and searched in the catalog."""
    legacy_item = _aux_item("S3A_OL_1_EO__AX", "s3://catalog-bucket/aux/S3A_OL_1_EO__AX.zip")
    generated_item = _aux_item("S3A_ADF_OLEOP", "s3://out-bucket/owner/s3-adf/S3A_ADF_OLEOP.zarr/")
    rs_client = MagicMock()
    catalog_client = rs_client.get_catalog_client.return_value
    # The ADF is not in the catalog before the conversion
    catalog_client.search.side_effect = [None, ItemCollection([generated_item]), ItemCollection([generated_item])]
    rs_client.get_auxip_client.return_value.search.return_value = ItemCollection([legacy_item])
    staging_client = rs_client.get_staging_client.return_value
    staging_client.run_staging.return_value = {"host": {}}
    staging_client.wait_for_jobs.return_value = {"host": {"status": "successful"}}
    catalog_client.get_items.side_effect = lambda **_: [legacy_item]

    stager = _adf_stager(rs_client, ["*=s3-adf:catalog"], bucket_configuration=[["*", "*", "*", "*", "out-bucket"]])
    name, adf_type, (status, items) = await stager.process_input_adfs(_input_adfs("ADF_OLEOP"))

    assert (name, adf_type, status) == ("eop", "filename", True)
    assert [item.id for item in items] == ["S3A_ADF_OLEOP"]

    # The legacy file was searched on the AUXIP with the task table query, and staged
    auxip_search = rs_client.get_auxip_client.return_value.search.call_args
    assert _search_product_type(auxip_search) == "OL_1_EO__AX"
    assert staging_client.run_staging.call_args.args[1] == "s3-adf"

    # The legacy file was converted with stb_convert_products, uploaded and published
    adf_conversion.download.assert_awaited_once()
    assert adf_conversion.run_script.call_args.args[0] == standalone.ADF_CONVERSIONS["ADF_OLEOP"].script_path
    assert adf_conversion.upload.await_args.args[2] == "s3://out-bucket/owner/s3-adf"
    collection, published = catalog_client.add_item.call_args.args
    assert collection == "s3-adf"
    assert published.properties["product:type"] == "ADF_OLEOP"
    assert published.assets["data"].href == "s3://out-bucket/owner/s3-adf/S3A_ADF_OLEOP.zarr/"

    # The generated ADF is searched in its collection with the task table query
    catalog_search = catalog_client.search.call_args
    assert catalog_search.kwargs["collections"] == ["s3-adf"]
    assert _search_product_type(catalog_search) == "ADF_OLEOP"

    # The ADF is generated only once per run
    await stager.process_input_adfs(_input_adfs("ADF_OLEOP"))
    adf_conversion.run_script.assert_called_once()


@pytest.mark.asyncio
async def test_process_input_adfs_mission_dependent_legacy_types(adf_conversion):
    """The legacy files of the orbit ADF are the ones of the satellite mission, searched on the AUXIP."""
    rs_client = MagicMock()
    rs_client.get_catalog_client.return_value.search.return_value = None
    rs_client.get_auxip_client.return_value.search.return_value = None
    stager = _adf_stager(rs_client, ["*=s3-adf:catalog"])
    with pytest.raises(RuntimeError, match="did not return any result"):
        await stager.process_input_adfs(_input_adfs("ADF_FPOAX"))
    auxip_searches = rs_client.get_auxip_client.return_value.search.call_args_list
    assert [_search_product_type(search) for search in auxip_searches] == ["AX___FPO_AX"]
    adf_conversion.run_script.assert_not_called()


def test_find_platform():
    """The platform is read from the CQL2 filter rendered from the task table query."""
    platform = {"op": "=", "args": [{"property": "platform"}, "sentinel-3b"]}
    product_type = {"op": "=", "args": [{"property": "product:type"}, "ADF_FPOAX"]}
    assert standalone.AdfsStager.find_platform({"filter": {"op": "and", "args": [product_type, platform]}}) == (
        "sentinel-3b"
    )
    assert standalone.AdfsStager.find_platform({"filter": product_type}) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "product_type, kwargs",
    [
        ("AX___OSF_AX", {}),  # not an ADF generated by conversion
        ("ADF_OLEOP", {"adf_conversion": False}),
        ("ADF_OLEOP", {"dry_run": True}),
    ],
)
async def test_process_input_adfs_without_conversion(adf_conversion, product_type, kwargs):
    """No conversion is done for legacy types, when disabled, or in dry run mode."""
    rs_client = MagicMock()
    rs_client.get_catalog_client.return_value.search.return_value = None
    stager = _adf_stager(rs_client, ["*=s3-adf:catalog"], **kwargs)
    with pytest.raises(RuntimeError, match="did not return any result"):
        await stager.process_input_adfs(_input_adfs(product_type))
    adf_conversion.run_script.assert_not_called()
    rs_client.get_staging_client.return_value.run_staging.assert_not_called()


@pytest.mark.asyncio
async def test_process_input_adfs_no_legacy_file(adf_conversion):
    """The ADF is not generated when no legacy auxiliary file is found."""
    rs_client = MagicMock()
    rs_client.get_catalog_client.return_value.search.return_value = None
    rs_client.get_auxip_client.return_value.search.return_value = None
    stager = _adf_stager(rs_client, ["*=s3-adf:catalog"])
    with pytest.raises(RuntimeError, match="did not return any result"):
        await stager.process_input_adfs(_input_adfs("ADF_OLEOP"))
    adf_conversion.run_script.assert_not_called()
