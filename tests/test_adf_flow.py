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

"""Unit tests for the adf_conversion flow."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from pystac import Asset, Item, ItemCollection

from rs_workflows import adf_flow
from rs_workflows.flow_utils import (
    AdfProcessIn,
    AdfType,
    AuxiliaryProductMapping,
    FlowEnvArgs,
)
from rs_workflows.utils import utils as workflow_utils

pytestmark = pytest.mark.filterwarnings(
    "ignore:Logger 'prefect\\.task_runs' attempted to send logs to the API without a flow run id\\.:UserWarning",
)


@pytest.fixture
def sample_adf_process_in():
    """Create a sample AdfProcessIn object."""
    return AdfProcessIn(
        env=FlowEnvArgs(owner_id="test-user"),
        adf_type=AdfType.S00__ADF_ECMWA,
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type="AX___MA1_AX", collection_name="AUX_EXACT"),
            AuxiliaryProductMapping(product_type="ADF_ECMWA", collection_name="ADF_PUBLISH"),
            AuxiliaryProductMapping(product_type="*", collection_name="AUX"),
        ],
        start_datetime=datetime(2021, 3, 21, 3, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2021, 3, 21, 15, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_adf_ecmwf_process_in():
    """Create a sample AdfProcessIn object for S00__ADF_ECMWF."""
    return AdfProcessIn(
        env=FlowEnvArgs(owner_id="test-user"),
        adf_type=AdfType.S00__ADF_ECMWF,
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type="AX___MF1_AX", collection_name="AUX_MF1"),
            AuxiliaryProductMapping(product_type="AX___MF2_AX", collection_name="AUX_MF2"),
            AuxiliaryProductMapping(product_type="ADF_ECMWF", collection_name="ADF_ECMWF_PUBLISH"),
            AuxiliaryProductMapping(product_type="*", collection_name="AUX"),
        ],
        start_datetime=datetime(2021, 1, 21, 0, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2021, 1, 21, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_adf_water_process_in():
    """Create a sample AdfProcessIn object for S00__ADF_WATER."""
    return AdfProcessIn(
        env=FlowEnvArgs(owner_id="test-user"),
        adf_type=AdfType.S00__ADF_WATER,
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type="AX___LWM_AX", collection_name="AUX_LWM"),
            AuxiliaryProductMapping(product_type="AX___CLM_AX", collection_name="AUX_CLM"),
            AuxiliaryProductMapping(product_type="AX___TRM_AX", collection_name="AUX_TRM"),
            AuxiliaryProductMapping(product_type="AX___OOM_AX", collection_name="AUX_OOM"),
            AuxiliaryProductMapping(product_type="SR_2_MLM_AX", collection_name="AUX_MLM"),
            AuxiliaryProductMapping(product_type="SR_2_SURFAX", collection_name="AUX_SURF"),
            AuxiliaryProductMapping(product_type="ADF_WATER", collection_name="ADF_WATER_PUBLISH"),
            AuxiliaryProductMapping(product_type="*", collection_name="AUX"),
        ],
        start_datetime=datetime(2021, 8, 1, 0, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2021, 8, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def create_stac_item_mock(monkeypatch):
    """Run STAC item creation synchronously in flow tests without Prefect orchestration."""
    mock = MagicMock(side_effect=adf_flow.create_stac_item_from_zarr.fn)
    monkeypatch.setattr(adf_flow, "create_stac_item_from_zarr", mock)
    return mock


def test_create_stac_item_from_zarr(mocker, tmp_path):
    """Test STAC item creation from ZARR metadata."""
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=MagicMock())
    zarr_dir = tmp_path / "test.zarr"
    zarr_dir.mkdir()
    zattrs_file = zarr_dir / ".zattrs"
    zattrs_content = {
        "id": "S00__ADF_ECMWA_20210321T030000_20210321T150000",
        "properties": {
            "product:type": "ADF_ECMWA",
            "created": "2026-04-27T13:30:05",
            "start_datetime": "2021-03-21T03:00:00Z",
            "end_datetime": "2021-03-21T15:00:00Z",
        },
    }
    zattrs_file.write_text(json.dumps(zattrs_content))

    item = adf_flow.create_stac_item_from_zarr.fn(zarr_dir, "ADF_ECMWA")

    assert item.id == "S00__ADF_ECMWA_20210321T030000_20210321T150000"
    assert item.properties["product:type"] == "ADF_ECMWA"  # Workaround applied
    assert item.properties["created"] == "2026-04-27T13:30:05Z"
    assert "data" in item.assets
    assert item.assets["data"].href == str(zarr_dir)


def test_create_stac_item_from_json_uses_stac_discovery(mocker, tmp_path):
    """Test STAC item creation from JSON metadata with stac_discovery."""
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=MagicMock())
    json_file = tmp_path / "fallback-id.json"
    json_file.write_text(
        json.dumps(
            {
                "stac_discovery": {
                    "id": "discovery-id",
                    "geometry": None,
                    "bbox": None,
                    "stac_extensions": ["https://stac-extensions.github.io/product/v1.0.0/schema.json"],
                    "properties": {
                        "product:type": "ADF_WATER",
                        "created": "2026-04-27T13:30:05",
                    },
                },
            },
        ),
    )

    item = adf_flow.create_stac_item_from_zarr.fn(json_file, "ADF_WATER")

    assert item.id == "discovery-id"
    assert item.properties["product:type"] == "ADF_WATER"
    assert item.properties["created"] == "2026-04-27T13:30:05Z"
    assert "data" in item.assets
    assert item.assets["data"].href == str(json_file)
    assert item.assets["data"].media_type == "application/json"


def test_create_stac_item_from_json_falls_back_to_filename(mocker, tmp_path):
    """Test JSON STAC item creation falls back to the file stem without stac_discovery.id."""
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=MagicMock())
    json_file = tmp_path / "fallback-id.json"
    json_file.write_text(
        json.dumps(
            {
                "stac_discovery": {
                    "properties": {
                        "product:type": "ADF_WATER",
                        "start_datetime": "2021-08-01T00:00:00Z",
                        "end_datetime": "2021-08-01T12:00:00Z",
                    },
                },
            },
        ),
    )

    item = adf_flow.create_stac_item_from_zarr.fn(json_file, "ADF_WATER")

    assert item.id == "fallback-id"
    assert item.properties["product:type"] == "ADF_WATER"


@pytest.mark.parametrize(
    "item_id, expected_start, expected_end",
    [
        (
            "S03A_ADF_OLCAL_20211027T000000_20991231T235959_20260612T114433",
            "2021-10-27T00:00:00Z",
            "2099-12-31T23:59:59Z",
        ),
        (
            "S00__ADF_ECMWA_20210321T030000_20210321T150000_20260101T000000",
            "2021-03-21T03:00:00Z",
            "2021-03-21T15:00:00Z",
        ),
    ],
)
def test_extract_datetimes_from_item_id_valid(item_id, expected_start, expected_end):
    """Test extraction of start/end datetimes from a well-formed item_id."""
    result = adf_flow.extract_datetimes_from_item_id(item_id)
    assert result is not None
    start_iso, end_iso = result
    assert start_iso == expected_start
    assert end_iso == expected_end


@pytest.mark.parametrize(
    "item_id",
    [
        "no-dates-here",
        # only two timestamps, missing creation
        "S00__ADF_ECMWA_20210321T030000_20210321T150000",
        "S00__ADF_ECMWA",
        "",
    ],
)
def test_extract_datetimes_from_item_id_no_match(item_id):
    """Test that non-matching item_ids return None."""
    assert adf_flow.extract_datetimes_from_item_id(item_id) is None


def test_create_stac_item_uses_stac_props_over_item_id(mocker, tmp_path):
    """Test that create_stac_item_from_zarr prefers stac_props datetimes over item_id ones."""
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=MagicMock())
    zarr_dir = tmp_path / "test.zarr"
    zarr_dir.mkdir()
    zattrs_content = {
        "id": "S03A_ADF_OLCAL_20211027T000000_20991231T235959_20260612T114433",
        "properties": {
            "product:type": "ADF_OLCAL",
            "created": "2026-06-12T11:44:33Z",
            # These stac_props should take priority over the item_id datetimes
            "start_datetime": "2000-01-01T00:00:00Z",
            "end_datetime": "2000-12-31T23:59:59Z",
        },
    }
    (zarr_dir / ".zattrs").write_text(json.dumps(zattrs_content))

    item = adf_flow.create_stac_item_from_zarr.fn(zarr_dir, "ADF_OLCAL")

    assert item.id == "S03A_ADF_OLCAL_20211027T000000_20991231T235959_20260612T114433"
    # Datetimes should come from stac_props, not from item_id
    assert item.common_metadata.start_datetime is not None
    assert item.common_metadata.end_datetime is not None
    assert item.common_metadata.start_datetime.year == 2000
    assert item.common_metadata.start_datetime.month == 1
    assert item.common_metadata.start_datetime.day == 1
    assert item.common_metadata.end_datetime.year == 2000
    assert item.common_metadata.end_datetime.month == 12
    assert item.common_metadata.end_datetime.day == 31


def test_create_stac_item_uses_item_id_when_stac_props_missing(mocker, tmp_path):
    """Test that item_id datetimes are used when stac_props don't have start/end datetime."""
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=MagicMock())
    zarr_dir = tmp_path / "test.zarr"
    zarr_dir.mkdir()
    zattrs_content = {
        "id": "S03A_ADF_OLCAL_20211027T000000_20991231T235959_20260612T114433",
        "properties": {
            "product:type": "ADF_OLCAL",
            "created": "2026-06-12T11:44:33Z",
            # No start_datetime/end_datetime => should fall back to item_id
        },
    }
    (zarr_dir / ".zattrs").write_text(json.dumps(zattrs_content))

    item = adf_flow.create_stac_item_from_zarr.fn(zarr_dir, "ADF_OLCAL")

    assert item.id == "S03A_ADF_OLCAL_20211027T000000_20991231T235959_20260612T114433"
    # Datetimes should come from item_id since stac_props don't have them
    assert item.common_metadata.start_datetime is not None
    assert item.common_metadata.end_datetime is not None
    assert item.common_metadata.start_datetime.year == 2021
    assert item.common_metadata.start_datetime.month == 10
    assert item.common_metadata.start_datetime.day == 27
    assert item.common_metadata.end_datetime.year == 2099
    assert item.common_metadata.end_datetime.month == 12
    assert item.common_metadata.end_datetime.day == 31


def test_create_stac_item_falls_back_to_metadata_when_item_id_has_no_dates(mocker, tmp_path):
    """Test that metadata properties are used when item_id doesn't match the datetime pattern."""
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=MagicMock())
    zarr_dir = tmp_path / "test.zarr"
    zarr_dir.mkdir()
    zattrs_content = {
        "id": "simple-id-no-dates",
        "properties": {
            "product:type": "ADF_ECMWA",
            "start_datetime": "2021-03-21T03:00:00Z",
            "end_datetime": "2021-03-21T15:00:00Z",
        },
    }
    (zarr_dir / ".zattrs").write_text(json.dumps(zattrs_content))

    item = adf_flow.create_stac_item_from_zarr.fn(zarr_dir, "ADF_ECMWA")

    assert item.id == "simple-id-no-dates"
    # Datetimes should come from metadata properties since item_id doesn't match
    assert item.common_metadata.start_datetime is not None
    assert item.common_metadata.end_datetime is not None
    assert item.common_metadata.start_datetime.year == 2021
    assert item.common_metadata.start_datetime.month == 3
    assert item.common_metadata.start_datetime.day == 21
    assert item.common_metadata.start_datetime.hour == 3
    assert item.common_metadata.end_datetime.hour == 15


def test_extract_datetimes_from_item_id_logs_warning_on_no_match():
    """Test that a warning is logged when the item_id pattern doesn't match (make sonarqube happy)"""
    mock_logger = MagicMock()
    result = adf_flow.extract_datetimes_from_item_id("no-dates-here", logger=mock_logger)
    assert result is None
    mock_logger.warning.assert_called_once()
    assert "no-dates-here" in mock_logger.warning.call_args[0][0]


def test_run_adf_script_returns_generated_zarr(monkeypatch, mocker, tmp_path):
    """Test the conversion script wrapper returns the generated ZARR path."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)
    input_dir = tmp_path / "INPUT"
    work_dir = tmp_path / "WORK"
    output_dir = tmp_path / "OUTPUT"
    input_dir.mkdir()
    work_dir.mkdir()
    output_dir.mkdir()
    generated_zarr = output_dir / "generated.zarr"
    generated_zarr.mkdir()

    completed_process = MagicMock(stdout="line 1\nline 2", stderr="warn 1")
    run_mock = MagicMock(return_value=completed_process)
    monkeypatch.setattr(adf_flow.subprocess, "run", run_mock)

    result = adf_flow.run_adf_script.fn(adf_flow.ADF_ECMWA_SCRIPT_PATH, input_dir, work_dir, output_dir)

    assert result == [generated_zarr]
    run_mock.assert_called_once()
    command = run_mock.call_args.args[0]
    assert command == [
        adf_flow.sys.executable,
        str(adf_flow.ADF_ECMWA_SCRIPT_PATH),
        str(input_dir),
        "--working_dir",
        str(work_dir),
    ]
    assert run_mock.call_args.kwargs["env"]["ADF_OUTPUT"] == str(output_dir)
    mock_logger.info.assert_any_call("Conversion log: line 1")
    mock_logger.info.assert_any_call("Conversion log: line 2")
    mock_logger.info.assert_any_call("Conversion log: warn 1")


def test_run_adf_script_logs_and_raises_on_subprocess_error(monkeypatch, mocker, tmp_path):
    """Test subprocess failures are logged before being re-raised."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)
    input_dir = tmp_path / "INPUT"
    work_dir = tmp_path / "WORK"
    output_dir = tmp_path / "OUTPUT"
    input_dir.mkdir()
    work_dir.mkdir()
    output_dir.mkdir()

    error = adf_flow.subprocess.CalledProcessError(
        returncode=7,
        cmd=["ignored"],
        output="",
        stderr="boom",
    )
    error.stdout = "before failure"
    run_mock = MagicMock(side_effect=error)
    monkeypatch.setattr(adf_flow.subprocess, "run", run_mock)

    with pytest.raises(adf_flow.subprocess.CalledProcessError):
        adf_flow.run_adf_script.fn(adf_flow.ADF_ECMWA_SCRIPT_PATH, input_dir, work_dir, output_dir)

    mock_logger.info.assert_any_call("Conversion log: before failure")
    mock_logger.info.assert_any_call("Conversion log: boom")
    mock_logger.error.assert_called_once_with("ADF conversion failed with exit code 7")


def test_run_adf_script_raises_when_no_product_is_generated(monkeypatch, mocker, tmp_path):
    """Test the wrapper raises when the conversion script produces no ZARR or JSON output."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)
    input_dir = tmp_path / "INPUT"
    work_dir = tmp_path / "WORK"
    output_dir = tmp_path / "OUTPUT"
    input_dir.mkdir()
    work_dir.mkdir()
    output_dir.mkdir()

    monkeypatch.setattr(adf_flow.subprocess, "run", MagicMock(return_value=MagicMock(stdout="", stderr="")))

    with pytest.raises(RuntimeError, match="No ZARR or JSON product generated"):
        adf_flow.run_adf_script.fn(adf_flow.ADF_ECMWA_SCRIPT_PATH, input_dir, work_dir, output_dir)


@pytest.mark.asyncio
async def test_download_and_extract_assets_task_extracts_zip(monkeypatch, mocker, tmp_path):
    """Test ZIP assets are downloaded and extracted to the destination directory."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.utils.utils.get_run_logger", return_value=mock_logger)
    item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))

    async def fake_download(_href, destination):
        destination.write_bytes(b"zip payload")

    def fake_extract_zip(_archive_path, extract_to):
        nested_dir = extract_to / "nested"
        nested_dir.mkdir()
        (nested_dir / "content.txt").write_text("payload")

    recursive_extract_mock = MagicMock()
    extract_zip_mock = MagicMock(side_effect=fake_extract_zip)
    monkeypatch.setattr(workflow_utils, "s3_download_file", fake_download)
    monkeypatch.setattr(workflow_utils, "extract_zip", extract_zip_mock)
    monkeypatch.setattr(workflow_utils, "recursive_extract", recursive_extract_mock)

    await adf_flow.download_and_extract_assets_task.fn([item], tmp_path)

    assert (tmp_path / "nested" / "content.txt").read_text() == "payload"
    extract_zip_mock.assert_called_once()
    recursive_extract_mock.assert_called_once_with(tmp_path)


@pytest.mark.asyncio
async def test_download_and_extract_assets_task_copies_plain_file_and_skips_non_s3(
    monkeypatch,
    mocker,
    tmp_path,
):
    """Test non-archive S3 assets are copied and non-S3 assets are skipped."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.utils.utils.get_run_logger", return_value=mock_logger)
    item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    item.add_asset("plain", Asset(href="s3://bucket/aux-item.txt"))
    item.add_asset("skip", Asset(href="https://example.com/aux-item.txt"))

    async def fake_download(_href, destination):
        destination.write_text("plain payload")

    download_mock = AsyncMock(side_effect=fake_download)
    recursive_extract_mock = MagicMock()
    monkeypatch.setattr(workflow_utils, "s3_download_file", download_mock)
    monkeypatch.setattr(workflow_utils, "recursive_extract", recursive_extract_mock)

    await adf_flow.download_and_extract_assets_task.fn([item], tmp_path)

    assert (tmp_path / "aux-item.txt").read_text() == "plain payload"
    download_mock.assert_awaited_once()
    recursive_extract_mock.assert_not_called()
    mock_logger.warning.assert_called_once()
    assert "Skipping non-S3 asset" in mock_logger.warning.call_args[0][0]


@pytest.mark.asyncio
async def test_download_and_extract_assets_task_downloads_only_selected_asset(
    monkeypatch,
    mocker,
    tmp_path,
):
    """Test only named assets are downloaded when the optional asset filter is provided."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.utils.utils.get_run_logger", return_value=mock_logger)
    item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    item.add_asset("product", Asset(href="s3://bucket/product.txt"))
    item.add_asset("metadata", Asset(href="s3://bucket/metadata.txt"))

    async def fake_download(href, destination):
        destination.write_text(f"payload from {href}")

    download_mock = AsyncMock(side_effect=fake_download)
    monkeypatch.setattr(workflow_utils, "s3_download_file", download_mock)

    await adf_flow.download_and_extract_assets_task.fn([item], tmp_path, asset="product")

    assert (tmp_path / "product.txt").read_text() == "payload from s3://bucket/product.txt"
    assert not (tmp_path / "metadata.txt").exists()
    download_mock.assert_awaited_once_with("s3://bucket/product.txt", mocker.ANY)


@pytest.mark.asyncio
async def test_download_and_extract_assets_task_accepts_feature_collection_dict(
    monkeypatch,
    mocker,
    tmp_path,
):
    """Test inline STAC FeatureCollection dictionaries are normalized before download."""
    mocker.patch("rs_workflows.utils.utils.get_run_logger", return_value=MagicMock())

    async def fake_download(href, destination):
        destination.write_text(f"payload from {href}")

    download_mock = AsyncMock(side_effect=fake_download)
    monkeypatch.setattr(workflow_utils, "s3_download_file", download_mock)

    await adf_flow.download_and_extract_assets_task.fn(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "id": "item-1",
                    "geometry": None,
                    "bbox": None,
                    "properties": {"datetime": "2026-05-04T12:38:15Z"},
                    "links": [],
                    "assets": {"product": {"href": "s3://bucket/product.txt"}},
                },
            ],
        },
        tmp_path,
    )

    assert (tmp_path / "product.txt").read_text() == "payload from s3://bucket/product.txt"
    download_mock.assert_awaited_once_with("s3://bucket/product.txt", mocker.ANY)


@pytest.mark.asyncio
async def test_adf_conversion_flow_logic(
    monkeypatch,
    mocker,
    sample_adf_process_in,
    create_stac_item_mock,
    tmp_path,
    _mock_os_env,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test the full adf_conversion flow logic with mocks."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)
    # 1. Mock auxip_staging_task
    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "aux_staging_task", staging_mock)

    # 2. Mock download_and_extract_assets_task
    extract_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", extract_mock)

    # 3. Mock S3 operations
    upload_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload_mock)
    rmtree_mock = MagicMock()
    monkeypatch.setattr(adf_flow.shutil, "rmtree", rmtree_mock)

    # 4. Mock run_adf_script
    zarr_path = tmp_path / "mock.zarr"
    zarr_path.mkdir()
    zattrs_content = {
        "id": "mock-adf",
        "properties": {
            "product:type": "ADF_ECMWA",
            "start_datetime": "2021-03-21T03:00:00Z",
            "end_datetime": "2021-03-21T15:00:00Z",
        },
    }
    (zarr_path / ".zattrs").write_text(json.dumps(zattrs_content))
    run_script_mock = MagicMock(return_value=[zarr_path])
    monkeypatch.setattr(adf_flow, "run_adf_script", run_script_mock)
    generated_item = Item(
        id="mock-adf",
        geometry=None,
        bbox=None,
        datetime=datetime.now(timezone.utc),
        properties={"product:type": "S00__ADF_ECMWA"},
    )
    generated_item.add_asset("data", Asset(href=str(zarr_path)))
    create_item_mock = MagicMock(return_value=generated_item)
    monkeypatch.setattr(adf_flow, "create_stac_item_from_zarr", create_item_mock)

    # 5. Mock external configurations
    config_mock = MagicMock(return_value=[["*", "*", "*", "*", "test-bucket"]])
    monkeypatch.setattr(adf_flow, "fetch_csv_from_endpoint", config_mock)
    monkeypatch.setattr(adf_flow, "find_s3_output_bucket", MagicMock(return_value="test-bucket"))

    # 6. Mock publish
    publish_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "publish", publish_mock)

    # 7. Mock FlowEnv
    flow_env_mock = mocker.MagicMock()
    flow_env_mock.start_span.return_value = MagicMock()
    flow_env_mock.start_span.return_value.__enter__.return_value = MagicMock()
    flow_env_mock.owner_id = "test-user"
    flow_env_mock.serialize.return_value = FlowEnvArgs(owner_id="test-user")
    monkeypatch.setattr(adf_flow, "FlowEnv", lambda env: flow_env_mock)

    # Execute flow
    await adf_flow.adf_conversion.fn(sample_adf_process_in)

    # Verifications
    assert staging_mock.call_count == 1
    temporal_filter = staging_mock.call_args.kwargs["cql2_filter"]["filter"]["args"][0]
    assert temporal_filter == {
        "op": "t_intersects",
        "args": [
            {"interval": [{"property": "start_datetime"}, {"property": "end_datetime"}]},
            {"interval": ["2021-03-21T03:00:00+00:00", "2021-03-21T15:00:00+00:00"]},
        ],
    }
    assert extract_mock.called
    run_script_mock.assert_called_once()
    assert run_script_mock.call_args.args[0] == adf_flow.ADF_ECMWA_SCRIPT_PATH
    assert upload_mock.called
    assert publish_mock.called

    # Check that workaround was applied before publishing
    published_metadata = publish_mock.call_args[0][2]
    publish_mapping = publish_mock.call_args[0][1]
    assert published_metadata[0].product_type == "ADF_ECMWA"
    assert published_metadata[0].stac_item.properties["product:type"] == "ADF_ECMWA"
    assert published_metadata[0].stac_item.assets["data"].href.startswith("s3://test-bucket/")
    assert publish_mapping[0].collection_name == "ADF_PUBLISH"
    rmtree_calls = [
        (call.args[0].name, call.kwargs) for call in rmtree_mock.call_args_list if hasattr(call.args[0], "name")
    ]
    assert ("INPUT", {"ignore_errors": True}) in rmtree_calls
    assert ("OUTPUT", {"ignore_errors": True}) in rmtree_calls


@pytest.mark.asyncio
async def test_adf_conversion_flow_logic_for_ecmwf(
    monkeypatch,
    mocker,
    sample_adf_ecmwf_process_in,
    create_stac_item_mock,
    tmp_path,
    _mock_os_env,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test that S00__ADF_ECMWF stages MF inputs and uses the ECMWF conversion script."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)

    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "aux_staging_task", staging_mock)

    extract_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", extract_mock)

    upload_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload_mock)
    monkeypatch.setattr(adf_flow.shutil, "rmtree", MagicMock())

    zarr_path = tmp_path / "mock-ecmwf.zarr"
    zarr_path.mkdir()
    (zarr_path / ".zattrs").write_text(
        json.dumps(
            {
                "id": "mock-ecmwf-adf",
                "properties": {
                    "product:type": "ADF_ECMWF",
                    "start_datetime": "2021-01-21T00:00:00Z",
                    "end_datetime": "2021-01-21T12:00:00Z",
                },
            },
        ),
    )
    run_script_mock = MagicMock(return_value=[zarr_path])
    monkeypatch.setattr(adf_flow, "run_adf_script", run_script_mock)
    generated_item = Item(
        id="mock-ecmwf-adf",
        geometry=None,
        bbox=None,
        datetime=datetime.now(timezone.utc),
        properties={"product:type": "S00__ADF_ECMWF"},
    )
    generated_item.add_asset("data", Asset(href=str(zarr_path)))
    monkeypatch.setattr(adf_flow, "create_stac_item_from_zarr", MagicMock(return_value=generated_item))

    monkeypatch.setattr(
        adf_flow,
        "fetch_csv_from_endpoint",
        MagicMock(return_value=[["*", "*", "*", "*", "test-bucket"]]),
    )
    monkeypatch.setattr(adf_flow, "find_s3_output_bucket", MagicMock(return_value="test-bucket"))

    publish_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "publish", publish_mock)

    flow_env_mock = mocker.MagicMock()
    flow_env_mock.start_span.return_value = MagicMock()
    flow_env_mock.start_span.return_value.__enter__.return_value = MagicMock()
    flow_env_mock.owner_id = "test-user"
    flow_env_mock.serialize.return_value = FlowEnvArgs(owner_id="test-user")
    monkeypatch.setattr(adf_flow, "FlowEnv", lambda env: flow_env_mock)

    await adf_flow.adf_conversion.fn(sample_adf_ecmwf_process_in)

    assert staging_mock.call_count == 1
    staged_product_types = [
        call.kwargs["cql2_filter"]["filter"]["args"][1]["args"][1] for call in staging_mock.call_args_list
    ]
    assert staged_product_types == ["AX___MF1_AX"]
    assert [call.kwargs["catalog_collection_identifier"] for call in staging_mock.call_args_list] == [
        "AUX_MF1",
    ]
    extract_mock.assert_awaited_once()
    run_script_mock.assert_called_once()
    assert run_script_mock.call_args.args[0] == adf_flow.ADF_ECMWF_SCRIPT_PATH
    upload_mock.assert_awaited_once()

    published_metadata = publish_mock.call_args[0][2]
    publish_mapping = publish_mock.call_args[0][1]
    assert published_metadata[0].product_type == "ADF_ECMWF"
    assert published_metadata[0].stac_item.properties["product:type"] == "ADF_ECMWF"
    assert publish_mapping[0].collection_name == "ADF_ECMWF_PUBLISH"


@pytest.mark.asyncio
async def test_adf_conversion_flow_logic_for_water(
    monkeypatch,
    mocker,
    sample_adf_water_process_in,
    create_stac_item_mock,
    tmp_path,
    _mock_os_env,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test that S00__ADF_WATER stages WATER inputs and uses the WATER conversion script."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)

    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "aux_staging_task", staging_mock)

    extract_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", extract_mock)

    upload_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload_mock)
    monkeypatch.setattr(adf_flow.shutil, "rmtree", MagicMock())

    zarr_path = tmp_path / "mock-water.zarr"
    zarr_path.mkdir()
    (zarr_path / ".zattrs").write_text(
        json.dumps(
            {
                "id": "mock-water-adf",
                "properties": {
                    "product:type": "ADF_WATER",
                    "start_datetime": "2021-08-01T00:00:00Z",
                    "end_datetime": "2021-08-01T12:00:00Z",
                },
            },
        ),
    )
    run_script_mock = MagicMock(return_value=[zarr_path])
    monkeypatch.setattr(adf_flow, "run_adf_script", run_script_mock)

    monkeypatch.setattr(
        adf_flow,
        "fetch_csv_from_endpoint",
        MagicMock(return_value=[["*", "*", "*", "*", "test-bucket"]]),
    )
    monkeypatch.setattr(adf_flow, "find_s3_output_bucket", MagicMock(return_value="test-bucket"))

    publish_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "publish", publish_mock)

    flow_env_mock = mocker.MagicMock()
    flow_env_mock.start_span.return_value = MagicMock()
    flow_env_mock.start_span.return_value.__enter__.return_value = MagicMock()
    flow_env_mock.owner_id = "test-user"
    flow_env_mock.serialize.return_value = FlowEnvArgs(owner_id="test-user")
    monkeypatch.setattr(adf_flow, "FlowEnv", lambda env: flow_env_mock)

    await adf_flow.adf_conversion.fn(sample_adf_water_process_in)

    assert staging_mock.call_count == 6
    staged_product_types = [
        call.kwargs["cql2_filter"]["filter"]["args"][1]["args"][1] for call in staging_mock.call_args_list
    ]
    assert staged_product_types == [
        "AX___LWM_AX",
        "AX___CLM_AX",
        "AX___TRM_AX",
        "AX___OOM_AX",
        "SR_2_MLM_AX",
        "SR_2_SURFAX",
    ]
    assert [call.kwargs["catalog_collection_identifier"] for call in staging_mock.call_args_list] == [
        "AUX_LWM",
        "AUX_CLM",
        "AUX_TRM",
        "AUX_OOM",
        "AUX_MLM",
        "AUX_SURF",
    ]
    extract_mock.assert_awaited_once()
    assert len(extract_mock.call_args.args[0]) == 6
    run_script_mock.assert_called_once()
    assert run_script_mock.call_args.args[0].name == "S00__ADF_WATER.py"
    create_stac_item_mock.assert_called_once_with(zarr_path, "ADF_WATER")
    upload_mock.assert_awaited_once()

    published_metadata = publish_mock.call_args[0][2]
    publish_mapping = publish_mock.call_args[0][1]
    assert published_metadata[0].product_type == "ADF_WATER"
    assert published_metadata[0].stac_item.properties["product:type"] == "ADF_WATER"
    assert publish_mapping[0].collection_name == "ADF_WATER_PUBLISH"


@pytest.mark.asyncio
async def test_adf_conversion_raises_when_publish_collection_not_found(
    monkeypatch,
    mocker,
    sample_adf_process_in,
    create_stac_item_mock,
    tmp_path,
    _mock_os_env,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test the flow raises when no publish collection mapping is available."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)
    sample_adf_process_in.auxiliary_product_to_collection_identifier = [
        AuxiliaryProductMapping(product_type="AX___MA1_AX", collection_name="AUX_INPUT"),
    ]

    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "aux_staging_task", staging_mock)

    extract_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", extract_mock)

    upload_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload_mock)

    zarr_path = tmp_path / "mock.zarr"
    zarr_path.mkdir()
    (zarr_path / ".zattrs").write_text(
        json.dumps(
            {
                "id": "mock-adf",
                "properties": {
                    "product:type": "ADF_ECMWA",
                    "start_datetime": "2021-03-21T03:00:00Z",
                    "end_datetime": "2021-03-21T15:00:00Z",
                },
            },
        ),
    )
    monkeypatch.setattr(adf_flow, "run_adf_script", MagicMock(return_value=[zarr_path]))

    monkeypatch.setattr(
        adf_flow,
        "fetch_csv_from_endpoint",
        MagicMock(return_value=[["*", "*", "*", "*", "test-bucket"]]),
    )
    find_bucket_mock = MagicMock(return_value="test-bucket")
    monkeypatch.setattr(adf_flow, "find_s3_output_bucket", find_bucket_mock)

    publish_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "publish", publish_mock)

    flow_env_mock = mocker.MagicMock()
    flow_env_mock.start_span.return_value = MagicMock()
    flow_env_mock.start_span.return_value.__enter__.return_value = MagicMock()
    flow_env_mock.owner_id = "test-user"
    flow_env_mock.serialize.return_value = FlowEnvArgs(owner_id="test-user")
    monkeypatch.setattr(adf_flow, "FlowEnv", lambda env: flow_env_mock)

    with pytest.raises(RuntimeError, match="No target collection found for generated product type"):
        await adf_flow.adf_conversion.fn(sample_adf_process_in)

    assert not find_bucket_mock.called
    assert not upload_mock.called
    assert not publish_mock.called


@pytest.fixture
def sample_adf_getas_process_in():
    """Create a sample AdfProcessIn object for S00__ADF_GETAS."""
    return AdfProcessIn(
        env=FlowEnvArgs(owner_id="test-user"),
        adf_type=AdfType.S00__ADF_GETAS,
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type="AX___DEM_AX", collection_name="AUX_DEM"),
            AuxiliaryProductMapping(product_type="ADF_GETAS", collection_name="ADF_GETAS_PUBLISH"),
            AuxiliaryProductMapping(product_type="*", collection_name="AUX"),
        ],
        start_datetime=datetime(2021, 6, 15, 0, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2021, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_adf_conversion_flow_logic_for_getas(
    monkeypatch,
    mocker,
    sample_adf_getas_process_in,
    create_stac_item_mock,
    tmp_path,
    _mock_os_env,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test that S00__ADF_GETAS stages AX___DEM_AX inputs and uses the GETAS conversion script."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)

    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "aux_staging_task", staging_mock)

    extract_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", extract_mock)

    upload_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload_mock)
    monkeypatch.setattr(adf_flow.shutil, "rmtree", MagicMock())

    zarr_path = tmp_path / "mock-getas.zarr"
    zarr_path.mkdir()
    (zarr_path / ".zattrs").write_text(
        json.dumps(
            {
                "id": "mock-getas-adf",
                "properties": {
                    "product:type": "ADF_GETAS",
                    "start_datetime": "2021-06-15T00:00:00Z",
                    "end_datetime": "2021-06-15T12:00:00Z",
                },
            },
        ),
    )
    run_script_mock = MagicMock(return_value=[zarr_path])
    monkeypatch.setattr(adf_flow, "run_adf_script", run_script_mock)

    monkeypatch.setattr(
        adf_flow,
        "fetch_csv_from_endpoint",
        MagicMock(return_value=[["*", "*", "*", "*", "test-bucket"]]),
    )
    monkeypatch.setattr(adf_flow, "find_s3_output_bucket", MagicMock(return_value="test-bucket"))

    publish_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "publish", publish_mock)

    flow_env_mock = mocker.MagicMock()
    flow_env_mock.start_span.return_value = MagicMock()
    flow_env_mock.start_span.return_value.__enter__.return_value = MagicMock()
    flow_env_mock.owner_id = "test-user"
    flow_env_mock.serialize.return_value = FlowEnvArgs(owner_id="test-user")
    monkeypatch.setattr(adf_flow, "FlowEnv", lambda env: flow_env_mock)

    await adf_flow.adf_conversion.fn(sample_adf_getas_process_in)

    # Verify AX___DEM_AX was staged
    assert staging_mock.call_count == 1
    staged_product_types = [
        call.kwargs["cql2_filter"]["filter"]["args"][1]["args"][1] for call in staging_mock.call_args_list
    ]
    assert staged_product_types == ["AX___DEM_AX"]
    assert [call.kwargs["catalog_collection_identifier"] for call in staging_mock.call_args_list] == [
        "AUX_DEM",
    ]
    extract_mock.assert_awaited_once()

    # Verify GETAS script was used
    run_script_mock.assert_called_once()
    assert run_script_mock.call_args.args[0] == adf_flow.ADF_GETAS_SCRIPT_PATH
    upload_mock.assert_awaited_once()

    # Verify published metadata
    published_metadata = publish_mock.call_args[0][2]
    publish_mapping = publish_mock.call_args[0][1]
    assert published_metadata[0].product_type == "ADF_GETAS"
    assert published_metadata[0].stac_item.properties["product:type"] == "ADF_GETAS"
    assert publish_mapping[0].collection_name == "ADF_GETAS_PUBLISH"


def test_run_adf_script_stb_convert_products(monkeypatch, mocker, tmp_path):
    """Test that run_adf_script dispatches to stb_convert_products when given the sentinel value."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)
    input_dir = tmp_path / "INPUT"
    work_dir = tmp_path / "WORK"
    output_dir = tmp_path / "OUTPUT"
    input_dir.mkdir()
    work_dir.mkdir()
    output_dir.mkdir()
    # simulate multiple generated outputs (as stb_convert_products may produce)
    (output_dir / "product_a.zarr").mkdir()
    (output_dir / "product_b.zarr").mkdir()
    (output_dir / "product_c.json").write_text("{}")

    completed_process = MagicMock(stdout="done", stderr="")
    run_mock = MagicMock(return_value=completed_process)
    monkeypatch.setattr(adf_flow.subprocess, "run", run_mock)

    result = adf_flow.run_adf_script.fn(adf_flow.STB_CONVERT_PRODUCTS, input_dir, work_dir, output_dir)

    assert result == [
        output_dir / "product_a.zarr",
        output_dir / "product_b.zarr",
        output_dir / "product_c.json",
    ]
    run_mock.assert_called_once()
    command = run_mock.call_args.args[0]
    assert command == ["stb_convert_products", "-i", str(input_dir), "-o", str(output_dir)]
    # stb_convert_products mode should not set custom env (env=None)
    assert run_mock.call_args.kwargs["env"] is None


@pytest.mark.parametrize(
    "adf_type, expected_aux_type, expected_generated_type",
    [
        (AdfType.S03_ADF_OLCAL, "OL_1_CAL_AX", "ADF_OLCAL"),
        (AdfType.S03_ADF_OLEOP, "OL_1_EO__AX", "ADF_OLEOP"),
        (AdfType.S03_ADF_OLINS, "OL_1_INS_AX", "ADF_OLINS"),
        (AdfType.S03_ADF_OLLUT, "OL_1_CLUTAX", "ADF_OLLUT"),
        (AdfType.S03_ADF_OLPRG, "OL_1_PRG_AX", "ADF_OLPRG"),
        (AdfType.S03_ADF_OLRAC, "OL_1_RAC_AX", "ADF_OLRAC"),
        (AdfType.S03_ADF_OLSPC, "OL_1_SPC_AX", "ADF_OLSPC"),
    ],
)
@pytest.mark.asyncio
async def test_adf_conversion_flow_logic_for_s03_ol(
    monkeypatch,
    mocker,
    tmp_path,
    _mock_os_env,
    create_stac_item_mock,
    adf_type,
    expected_aux_type,
    expected_generated_type,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test that S03_ADF_OL* types stage the correct auxiliary file and use stb_convert_products."""
    mock_logger = MagicMock()
    mocker.patch("rs_workflows.adf_flow.get_run_logger", return_value=mock_logger)

    adf_input = AdfProcessIn(
        env=FlowEnvArgs(owner_id="test-user"),
        adf_type=adf_type,
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type=expected_aux_type, collection_name="AUX_OL_INPUT"),
            AuxiliaryProductMapping(product_type=expected_generated_type, collection_name="ADF_OL_PUBLISH"),
            AuxiliaryProductMapping(product_type="*", collection_name="AUX"),
        ],
        start_datetime=datetime(2021, 10, 27, 0, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2021, 10, 27, 12, 0, 0, tzinfo=timezone.utc),
    )

    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "aux_staging_task", staging_mock)

    extract_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", extract_mock)

    upload_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload_mock)
    monkeypatch.setattr(adf_flow.shutil, "rmtree", MagicMock())

    zarr_path = tmp_path / "mock-ol.zarr"
    zarr_path.mkdir()
    (zarr_path / ".zattrs").write_text(
        json.dumps(
            {
                "id": f"mock-{adf_type}-adf",
                "properties": {
                    "product:type": expected_generated_type,
                    "start_datetime": "2021-10-27T00:00:00Z",
                    "end_datetime": "2021-10-27T12:00:00Z",
                },
            },
        ),
    )
    run_script_mock = MagicMock(return_value=[zarr_path])
    monkeypatch.setattr(adf_flow, "run_adf_script", run_script_mock)

    monkeypatch.setattr(
        adf_flow,
        "fetch_csv_from_endpoint",
        MagicMock(return_value=[["*", "*", "*", "*", "test-bucket"]]),
    )
    monkeypatch.setattr(adf_flow, "find_s3_output_bucket", MagicMock(return_value="test-bucket"))

    publish_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "publish", publish_mock)

    flow_env_mock = mocker.MagicMock()
    flow_env_mock.start_span.return_value = MagicMock()
    flow_env_mock.start_span.return_value.__enter__.return_value = MagicMock()
    flow_env_mock.owner_id = "test-user"
    flow_env_mock.serialize.return_value = FlowEnvArgs(owner_id="test-user")
    monkeypatch.setattr(adf_flow, "FlowEnv", lambda env: flow_env_mock)

    await adf_flow.adf_conversion.fn(adf_input)

    # Verify the correct auxiliary type was staged
    assert staging_mock.call_count == 1
    staged_product_types = [
        call.kwargs["cql2_filter"]["filter"]["args"][1]["args"][1] for call in staging_mock.call_args_list
    ]
    assert staged_product_types == [expected_aux_type]
    assert [call.kwargs["catalog_collection_identifier"] for call in staging_mock.call_args_list] == [
        "AUX_OL_INPUT",
    ]
    extract_mock.assert_awaited_once()

    # Verify stb_convert_products was used (script_path == STB_CONVERT_PRODUCTS)
    run_script_mock.assert_called_once()
    assert run_script_mock.call_args.args[0] == adf_flow.STB_CONVERT_PRODUCTS
    upload_mock.assert_awaited_once()

    # Verify published metadata
    published_metadata = publish_mock.call_args[0][2]
    publish_mapping_arg = publish_mock.call_args[0][1]
    assert published_metadata[0].product_type == expected_generated_type
    assert published_metadata[0].stac_item.properties["product:type"] == expected_generated_type
    assert publish_mapping_arg[0].collection_name == "ADF_OL_PUBLISH"
