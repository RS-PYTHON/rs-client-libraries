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

"""Unit tests for the import_adf_from_obs flow."""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import boto3
import pytest
from pystac import Item

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.on_demand.adf.import_adf_from_obs import (
    _build_s3_key,
    _compute_target_collection,
    _filter_files_by_pattern,
    _get_output_bucket,
    _handle_production_mode,
    _handle_rehearsal_mode,
    convert_date,
    create_new_stac_item,
    download_adf_files,
    extract_files,
    import_adf_from_obs,
    import_items,
)


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client."""
    return MagicMock(spec=boto3.client("s3"))


@pytest.fixture
def mock_flow_env():
    """Create a mock FlowEnv."""
    flow_env = MagicMock(spec=FlowEnv)
    flow_env.owner_id = "test-user"
    flow_env.rs_client = MagicMock()
    return flow_env


@pytest.fixture
def sample_configuration():
    """Create a sample configuration for testing."""
    return {
        "input": {
            "bucket": "my-bucket-input",
            "path": "my-path-input/Ancillary_Data",
            "files": ["file1.tar.gz"],
            "extract_pattern": "S3__*.tgz|S3A_*.tgz",
        },
        "output": {"additional_path": "", "collection": "collection-output", "override": False},
    }


@pytest.fixture
def sample_extracted_files() -> list[str]:
    """Create sample extracted files for testing."""
    files: list[str] = [
        "S3A_OL_1_CLUTAX_20160425T095210_20991231T235959_20160525T120000___________________MPC_O_AL_003.SEN3.tgz",
        "S3__AX___LWM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3.tgz",
    ]
    return files


@pytest.fixture
def sample_extracted_files_tobeDELTED(tmp_path):
    """Create sample extracted files for testing."""
    files = [
        tmp_path
        / "S3A_OL_1_CLUTAX_20160425T095210_20991231T235959_20160525T120000___________________MPC_O_AL_003.SEN3.tgz",
        tmp_path
        / "S3__AX___LWM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3.tgz",
    ]
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()
    return [str(file) for file in files]


# --- Tests for helper functions ---


def test_filter_files_by_pattern():
    """Test filtering files by pattern."""
    files = [
        "S3B_OL_1_PRG_AX_20180618T000000_20991231T23595.tar.gz",
        "S3A_OL_1_CLUTAX_20160425T095210_20991231T235959_201605.tar.gz",
        "S3__AX___OOM_AX_20000101T000000_20991231T235959_20151.tar.gz",
        "other_file.txt",
    ]
    pattern = "S3__*.tar.gz|S3A_*.tar.gz"
    filtered = _filter_files_by_pattern(files, pattern)
    assert len(filtered) == 2
    assert "S3A_OL_1_CLUTAX_20160425T095210_20991231T235959_201605.tar.gz" in filtered
    assert "S3__AX___OOM_AX_20000101T000000_20991231T235959_20151.tar.gz" in filtered
    assert "other_file.txt" not in filtered


def test_compute_target_collection():
    """Test computing target collection from filename."""
    filename = "S3__AX___LWM_AX_20000101T000000_20991231T235959_20151214T120000___________________MPC_O_AL_001.SEN3.tgz"
    assert _compute_target_collection(filename, None) == "ax___lwm_ax"
    assert _compute_target_collection(filename, "custom-collection") == "custom-collection"


def test_build_s3_key():
    """Test building S3 key."""
    owner_id = "test-user"
    target_collection = "adf-test"
    additional_path = "subdir"
    filename = "S3__ADF_ECMWA_20210321T030000.zarr"
    s3_key = _build_s3_key(owner_id, target_collection, additional_path, filename)
    additional_path = ""
    s3_key2 = _build_s3_key(owner_id, target_collection, additional_path, filename)

    assert s3_key == "test-user/adf-test/subdir/S3__ADF_ECMWA_20210321T030000.zarr"
    assert s3_key2 == "test-user/adf-test/S3__ADF_ECMWA_20210321T030000.zarr"


def test_convert_date():
    """Test date conversion."""
    input_date = "20210321T030000"
    expected_output = "2021-03-21T03:00:00.000Z"
    assert convert_date(input_date) == expected_output


# --- Tests for main tasks ---


@pytest.mark.asyncio
async def test_download_adf_files(mock_s3_client, tmp_path):
    """Test downloading ADF files from S3."""
    bucket = "test-bucket"
    path = "test/path"
    files = ["file1.tar.gz", "file2.tar.gz"]
    input_dir = str(tmp_path / "input")
    os.makedirs(input_dir, exist_ok=True)

    # Mock S3 client
    mock_s3_client.download_file = MagicMock()

    result = await download_adf_files(mock_s3_client, bucket, path, files, input_dir)

    assert len(result) == 2
    assert all(f"{input_dir}/" in file for file in result)
    mock_s3_client.download_file.assert_any_call(bucket, f"{path}/file1.tar.gz", f"{input_dir}/file1.tar.gz")


@pytest.mark.asyncio
async def test_extract_files(tmp_path):
    """Test extracting files from tar.gz."""
    # Create a mock tar.gz file
    tar_file = tmp_path / "test.tar.gz"
    tar_file.touch()
    extract_dir = str(tmp_path / "extract")

    with patch("rs_workflows.on_demand.adf.import_adf_from_obs.extract_tar") as mock_extract_tar:
        mock_extract_tar.return_value = (2, ["file1.zarr", "file2.zarr"])
        result = await extract_files([str(tar_file)], extract_dir)

    assert len(result) == 2
    assert all(extract_dir in file for file in result)
    mock_extract_tar.assert_called_once()


@pytest.mark.asyncio
async def test_create_new_stac_item():
    """Test creating a new STAC item."""
    item_name = "S3__ADF_ECMWA_20210321T030000_20210321T150000_20260101T000000.zarr"
    href = "s3://test-bucket/S3__ADF_ECMWA_20210321T030000.zarr"

    with patch("rs_workflows.on_demand.adf.import_adf_from_obs.create_stac_item") as mock_create_stac_item:
        mock_item = Item(
            id="S3__ADF_ECMWA_20210321T030000_20210321T150000_20260101T000000",
            geometry=None,
            bbox=None,
            datetime=datetime.now(timezone.utc),
            properties={},
        )
        mock_create_stac_item.return_value = mock_item
        result = await create_new_stac_item(item_name, href)

    assert result.id == "S3__ADF_ECMWA_20210321T030000_20210321T150000_20260101T000000"
    mock_create_stac_item.assert_called_once()


@pytest.mark.asyncio
async def test_import_items_rehearsal_mode(mock_flow_env, mock_s3_client, tmp_path, sample_extracted_files):
    """Test import_items in rehearsal mode."""
    with patch.dict(
        os.environ,
        {
            "RSPY_HOST_OSAM": "http://test-host",
        },
    ):

        with patch(
            "rs_workflows.on_demand.adf.import_adf_from_obs.create_new_stac_item",
            new=AsyncMock(
                return_value=Item(
                    id="S3__ADF_ECMWA_20210321T030000_20210321T150000",
                    geometry=None,
                    bbox=None,
                    datetime=datetime.now(timezone.utc),
                    properties={},
                ),
            ),
        ) as mock_create_item:

            with patch("rs_workflows.on_demand.adf.import_adf_from_obs.fetch_csv_from_endpoint") as mock_fetch_csv:
                mock_fetch_csv.return_value = [["*", "*", "*", "*", "test-bucket"]]

                with patch("rs_workflows.on_demand.adf.import_adf_from_obs.get_run_logger") as mock_get_logger:
                    mock_logger = MagicMock()
                    mock_get_logger.return_value = mock_logger

                    await import_items(
                        flow_env=mock_flow_env,
                        s3_client=mock_s3_client,
                        extracted_files=sample_extracted_files,
                        output_path="",
                        override_collection=None,
                        override=False,
                        extract_pattern="S3__*.tgz",
                        rehearsal_mode=True,  # the test !!!
                    )

    mock_create_item.assert_called_once()
    mock_logger.info.assert_called()


@pytest.mark.asyncio
async def test_import_items_production_mode(mock_flow_env, mock_s3_client, tmp_path, sample_extracted_files):
    """Test import_items in production mode."""
    output_path = ""
    override_collection = None
    override = False
    extract_pattern = "S3__*.zarr"
    rehearsal_mode = False

    with patch("rs_workflows.on_demand.adf.import_adf_from_obs.create_new_stac_item") as mock_create_item:
        mock_item = Item(
            id="S3__ADF_ECMWA_20210321T030000_20210321T150000",
            geometry=None,
            bbox=None,
            datetime=datetime.now(timezone.utc),
            properties={},
        )
        mock_create_item.return_value = mock_item

        with patch("rs_workflows.on_demand.adf.import_adf_from_obs.fetch_csv_from_endpoint") as mock_fetch_csv:
            mock_fetch_csv.return_value = [["*", "*", "*", "*", "test-bucket"]]

            with patch(
                "rs_workflows.on_demand.adf.import_adf_from_obs.check_and_create_collection",
            ) as mock_check_collection:
                mock_check_collection.return_value = None

                with patch("rs_workflows.on_demand.adf.import_adf_from_obs.get_single_catalog_item") as mock_get_item:
                    mock_get_item.return_value = None

                    with patch("rs_workflows.on_demand.adf.import_adf_from_obs.published_stac_item") as mock_publish:
                        mock_publish.return_value = None

                        with patch("rs_workflows.on_demand.adf.import_adf_from_obs.get_run_logger") as mock_logger:
                            await import_items(
                                mock_flow_env,
                                mock_s3_client,
                                sample_extracted_files,
                                output_path,
                                override_collection,
                                override,
                                extract_pattern,
                                rehearsal_mode,
                            )

    mock_create_item.assert_called_once()
    mock_check_collection.assert_called_once()
    mock_publish.assert_called_once()
    mock_s3_client.upload_file.assert_called()


@pytest.mark.asyncio
async def test_import_adf_from_obs_flow(mock_flow_env, tmp_path, sample_configuration):
    """Test the full import_adf_from_obs flow."""
    with patch.dict(
        os.environ,
        {
            "S3_PUBLICATION_ENDPOINT": "http://test-endpoint",
            "S3_PUBLICATION_ACCESSKEY": "test-access-key",
            "S3_PUBLICATION_SECRETKEY": "test-secret-key",
            "S3_PUBLICATION_REGION": "test-region",
            "S3_ENDPOINT": "http://test-endpoint",
            "S3_ACCESSKEY": "test-access-key",
            "S3_SECRETKEY": "test-secret-key",
            "S3_REGION": "test-region",
            "RSPY_HOST_OSAM": "http://test-host",
        },
    ):
        with patch("rs_workflows.on_demand.adf.import_adf_from_obs.download_adf_files") as mock_download:
            mock_download.return_value = [str(tmp_path / "S3__ADF_ECMWA_20210321T030000.tar.gz")]

            with patch("rs_workflows.on_demand.adf.import_adf_from_obs.extract_files") as mock_extract:
                mock_extract.return_value = [str(tmp_path / "S3__ADF_ECMWA_20210321T030000.zarr")]

                with patch("rs_workflows.on_demand.adf.import_adf_from_obs.import_items") as mock_import:
                    mock_import.return_value = None

                    with patch("rs_workflows.on_demand.adf.import_adf_from_obs.FlowEnv") as mock_flow_env_class:
                        mock_flow_env_class.return_value = mock_flow_env

                        with patch("rs_workflows.on_demand.adf.import_adf_from_obs.get_run_logger") as mock_logger:
                            await import_adf_from_obs(
                                sample_configuration,
                                owner="test-user",
                                obs_id="PUBLICATION",
                                rehearsal_mode=True,
                            )

    mock_download.assert_called_once()
    mock_extract.assert_called_once()
    mock_import.assert_called_once()
