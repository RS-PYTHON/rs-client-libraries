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

"""Unit tests for shared workflow utilities."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pystac import Asset, Item

from rs_workflows.utils import utils as workflow_utils


@pytest.fixture
def mock_workflow_utils_logger(monkeypatch, mocker):
    """Replace Prefect run logger with a plain mock."""
    logger = mocker.Mock()
    monkeypatch.setattr(workflow_utils, "get_run_logger", lambda: logger)
    return logger


@pytest.mark.asyncio
async def test_upload_folder_flat(tmp_path, monkeypatch, mock_workflow_utils_logger):
    """Test flat upload keeps only file names in destination keys."""
    folder = tmp_path / "payload"
    nested = folder / "nested"
    nested.mkdir(parents=True)
    (folder / "a.txt").write_text("a")
    (nested / "b.txt").write_text("b")

    upload_mock = AsyncMock()
    monkeypatch.setattr(workflow_utils, "s3_upload_file", upload_mock)

    await workflow_utils.upload_folder_flat(folder, "s3://bucket/prefix/")

    uploaded_targets = [call.args[1] for call in upload_mock.await_args_list]
    assert uploaded_targets == ["s3://bucket/prefix/a.txt", "s3://bucket/prefix/b.txt"]


@pytest.mark.asyncio
async def test_process_asset_zip(monkeypatch, mock_workflow_utils_logger):
    """Test ZIP assets use ZIP extraction path and return normalized prefix."""
    download_mock = AsyncMock()
    upload_mock = AsyncMock()
    delete_mock = MagicMock()
    extract_tar_mock = MagicMock()

    def fake_extract_zip(_zip_path, extract_to):
        file_path = Path(extract_to) / "file"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text("content")

    extract_zip_mock = MagicMock(side_effect=fake_extract_zip)

    monkeypatch.setattr(workflow_utils, "s3_download_file", download_mock)
    monkeypatch.setattr(workflow_utils, "s3_delete", delete_mock)
    monkeypatch.setattr(workflow_utils, "extract_zip", extract_zip_mock)
    monkeypatch.setattr(workflow_utils, "extract_tar", extract_tar_mock)
    monkeypatch.setattr(workflow_utils, "recursive_extract", MagicMock(return_value=0))
    monkeypatch.setattr(workflow_utils, "normalize_extract_dir", MagicMock(side_effect=lambda path: path))
    monkeypatch.setattr(workflow_utils, "upload_folder_flat", upload_mock)
    monkeypatch.setattr(workflow_utils, "get_upload_prefix", MagicMock(return_value="s3://bucket/path/"))

    result = await workflow_utils.process_asset("s3://bucket/path/data.zip", "data.zip")

    assert result == "s3://bucket/path/file"
    download_target = Path(download_mock.await_args_list[0].args[1])
    assert download_target.name == "archive.zip"
    extract_zip_mock.assert_called_once()
    extract_tar_mock.assert_not_called()
    delete_mock.assert_called_once_with("s3://bucket/path/data.zip")
    upload_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_asset_tar(monkeypatch, mock_workflow_utils_logger):
    """Test TAR-like assets return a concrete extracted file href."""
    download_mock = AsyncMock()
    upload_mock = AsyncMock()
    delete_mock = MagicMock()
    extract_zip_mock = MagicMock()

    def fake_extract_tar(_file_path, extract_to):
        (Path(extract_to) / "file1").write_text("one")
        (Path(extract_to) / "file2").write_text("two")

    extract_tar_mock = MagicMock(side_effect=fake_extract_tar)

    monkeypatch.setattr(workflow_utils, "s3_download_file", download_mock)
    monkeypatch.setattr(workflow_utils, "s3_delete", delete_mock)
    monkeypatch.setattr(workflow_utils, "extract_zip", extract_zip_mock)
    monkeypatch.setattr(workflow_utils, "extract_tar", extract_tar_mock)
    monkeypatch.setattr(workflow_utils, "recursive_extract", MagicMock(return_value=1))
    monkeypatch.setattr(workflow_utils, "normalize_extract_dir", MagicMock(side_effect=lambda path: path))
    monkeypatch.setattr(workflow_utils, "upload_folder_flat", upload_mock)
    monkeypatch.setattr(workflow_utils, "get_upload_prefix", MagicMock(return_value="s3://bucket/path/data/"))

    result = await workflow_utils.process_asset("s3://bucket/path/data.tar/file.tar", "file.tar")

    assert result == "s3://bucket/path/data/file1"
    download_target = Path(download_mock.await_args_list[0].args[1])
    assert download_target.name == "file.tar"
    extract_tar_mock.assert_called_once()
    extract_zip_mock.assert_not_called()
    delete_mock.assert_called_once_with("s3://bucket/path/data.tar/file.tar")
    upload_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_asset_returns_concrete_extracted_file_when_multiple_files_exist(
    monkeypatch,
    mock_workflow_utils_logger,
):
    """Test normalized assets return a concrete extracted file href when multiple files exist."""
    download_mock = AsyncMock()
    upload_mock = AsyncMock()
    delete_mock = MagicMock()

    def fake_extract_zip(_zip_path, extract_to):
        (Path(extract_to) / "manifest.safe").write_text("meta")
        (Path(extract_to) / "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.EOF").write_text(
            "payload-content",
        )

    extract_zip_mock = MagicMock(side_effect=fake_extract_zip)

    monkeypatch.setattr(workflow_utils, "s3_download_file", download_mock)
    monkeypatch.setattr(workflow_utils, "s3_delete", delete_mock)
    monkeypatch.setattr(workflow_utils, "extract_zip", extract_zip_mock)
    monkeypatch.setattr(workflow_utils, "recursive_extract", MagicMock(return_value=0))
    monkeypatch.setattr(workflow_utils, "normalize_extract_dir", MagicMock(side_effect=lambda path: path))
    monkeypatch.setattr(workflow_utils, "upload_folder_flat", upload_mock)
    monkeypatch.setattr(
        workflow_utils,
        "get_upload_prefix",
        MagicMock(
            return_value=(
                "s3://bucket/path/" "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732/"
            ),
        ),
    )

    result = await workflow_utils.process_asset(
        "s3://bucket/path/S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.zip",
        "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.zip",
    )

    assert result == (
        "s3://bucket/path/S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732/"
        "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.EOF"
    )
    delete_mock.assert_called_once()
    upload_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_asset_returns_concrete_extracted_file_for_sen3_payload(
    monkeypatch,
    mock_workflow_utils_logger,
):
    """Test normalized `.SEN3` products return a concrete extracted file href."""
    download_mock = AsyncMock()
    upload_mock = AsyncMock()
    delete_mock = MagicMock()

    product_name = "S3A_AX___OSF_AX_20160216T192404_99991231T235959_20250724T075944___________________EUM_O_AL_001.SEN3"

    def fake_extract_zip(_zip_path, extract_to):
        product_dir = Path(extract_to) / product_name
        product_dir.mkdir(parents=True, exist_ok=True)
        (product_dir / "xfdumanifest.xml").write_text("meta")
        (product_dir / f"{product_name}.nc").write_text("payload-content")

    extract_zip_mock = MagicMock(side_effect=fake_extract_zip)

    monkeypatch.setattr(workflow_utils, "s3_download_file", download_mock)
    monkeypatch.setattr(workflow_utils, "s3_delete", delete_mock)
    monkeypatch.setattr(workflow_utils, "extract_zip", extract_zip_mock)
    monkeypatch.setattr(workflow_utils, "recursive_extract", MagicMock(return_value=0))
    monkeypatch.setattr(workflow_utils, "normalize_extract_dir", MagicMock(side_effect=lambda path: next(path.iterdir())))
    monkeypatch.setattr(workflow_utils, "upload_folder_flat", upload_mock)
    monkeypatch.setattr(
        workflow_utils,
        "get_upload_prefix",
        MagicMock(return_value=f"s3://bucket/path/{product_name}/"),
    )

    result = await workflow_utils.process_asset(
        f"s3://bucket/path/{product_name}.zip",
        f"{product_name}.zip",
    )

    assert result == f"s3://bucket/path/{product_name}/{product_name}.nc"
    delete_mock.assert_called_once()
    upload_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_asset_rejects_unsupported_extension(mock_workflow_utils_logger):
    """Test unsupported archive extensions are rejected."""
    with pytest.raises(ValueError, match="Unsupported archive type"):
        await workflow_utils.process_asset("s3://bucket/path/data.raw", "data.raw")


@pytest.mark.asyncio
async def test_asset_unzip_decompress_updates_supported_assets(monkeypatch, mock_workflow_utils_logger):
    """Test asset hrefs and names are updated for supported archive suffixes."""
    item = Item(id="item-1", geometry=None, bbox=None, datetime=datetime.now(UTC), properties={})
    item.add_asset("data.zip", Asset(href="s3://bucket/path/data.zip"))
    item.add_asset("bundle.tar.gz", Asset(href="s3://bucket/path/bundle.tar.gz"))
    item.add_asset("raw.bin", Asset(href="s3://bucket/path/raw.bin"))

    process_asset_mock = AsyncMock(side_effect=["s3://bucket/path/data/", "s3://bucket/path/bundle/"])
    monkeypatch.setattr(workflow_utils, "process_asset", process_asset_mock)

    updated = await workflow_utils.asset_unzip_decompress.fn(item)

    assert sorted(updated.assets.keys()) == ["bundle", "data", "raw.bin"]
    assert updated.assets["data"].href == "s3://bucket/path/data/"
    assert updated.assets["bundle"].href == "s3://bucket/path/bundle/"
    assert updated.assets["raw.bin"].href == "s3://bucket/path/raw.bin"
