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


@pytest.fixture
def mock_logger(monkeypatch, mocker):
    """Replace Prefect run logger with a plain mock."""
    logger = mocker.Mock()
    monkeypatch.setattr(adf_flow, "get_run_logger", lambda: logger)
    return logger


@pytest.fixture
def sample_adf_process_in():
    """Create a sample AdfProcessIn object."""
    return AdfProcessIn(
        env=FlowEnvArgs(owner_id="test-user"),
        adf_type=AdfType.S00__ADF_ECMWA,
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type="*", collection_name="AUX"),
            AuxiliaryProductMapping(product_type="S00__ADF_ECMWA", collection_name="ADF_EXACT"),
        ],
        start_datetime=datetime(2021, 3, 21, 3, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2021, 3, 21, 15, 0, 0, tzinfo=timezone.utc),
    )


def test_create_stac_item_from_zarr(tmp_path):
    """Test STAC item creation from ZARR metadata."""
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

    item = adf_flow.create_stac_item_from_zarr(zarr_dir)

    assert item.id == "S00__ADF_ECMWA_20210321T030000_20210321T150000"
    assert item.properties["product:type"] == "S00__ADF_ECMWA"  # Workaround applied
    assert item.properties["created"] == "2026-04-27T13:30:05Z"
    assert "data" in item.assets
    assert item.assets["data"].href == str(zarr_dir)


@pytest.mark.asyncio
async def test_adf_conversion_flow_logic(
    monkeypatch,
    mocker,
    sample_adf_process_in,
    tmp_path,
    _mock_os_env,
    mock_logger,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test the full adf_conversion flow logic with mocks."""
    # 1. Mock auxip_staging_task
    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "auxip_staging_task", staging_mock)

    # 2. Mock download_and_extract_assets_task
    extract_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "download_and_extract_assets_task", extract_mock)

    # 3. Mock S3 operations
    download_mock = AsyncMock()
    upload_mock = AsyncMock()
    monkeypatch.setattr(adf_flow, "s3_download_file", download_mock)
    monkeypatch.setattr(adf_flow, "s3_upload_dir", upload_mock)
    rmtree_mock = MagicMock()
    monkeypatch.setattr(adf_flow.shutil, "rmtree", rmtree_mock)

    # 4. Mock run_adf_ecmwa_script
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
    run_script_mock = MagicMock(return_value=zarr_path)
    monkeypatch.setattr(adf_flow, "run_adf_ecmwa_script", run_script_mock)

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
    assert staging_mock.call_count == 1  # Once for each MA type
    assert extract_mock.called
    assert not download_mock.called  # Download is now inside the extract task
    assert run_script_mock.called
    assert upload_mock.called
    assert publish_mock.called

    # Check that workaround was applied before publishing
    published_metadata = publish_mock.call_args[0][2]
    publish_mapping = publish_mock.call_args[0][1]
    assert published_metadata[0].stac_item.properties["product:type"] == "S00__ADF_ECMWA"
    assert published_metadata[0].stac_item.assets["data"].href.startswith("s3://test-bucket/")
    assert publish_mapping[0].collection_name == "ADF_EXACT"
    rmtree_calls = [
        (call.args[0].name, call.kwargs)
        for call in rmtree_mock.call_args_list
        if hasattr(call.args[0], "name")
    ]
    assert ("INPUT", {"ignore_errors": True}) in rmtree_calls
    assert ("OUTPUT", {"ignore_errors": True}) in rmtree_calls


@pytest.mark.asyncio
async def test_adf_conversion_raises_when_publish_collection_not_found(
    monkeypatch,
    mocker,
    sample_adf_process_in,
    tmp_path,
    _mock_os_env,
    mock_logger,
):  # pylint: disable=redefined-outer-name,unused-argument
    """Test the flow raises when no publish collection mapping is available."""
    sample_adf_process_in.auxiliary_product_to_collection_identifier = [
        AuxiliaryProductMapping(product_type="AX___MA1_AX", collection_name="AUX_INPUT"),
    ]

    source_item = Item(id="aux-item", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    source_item.add_asset("data", Asset(href="s3://bucket/aux-item.zip"))
    staging_mock = AsyncMock(return_value=(True, ItemCollection([source_item])))
    monkeypatch.setattr(adf_flow, "auxip_staging_task", staging_mock)

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
    monkeypatch.setattr(adf_flow, "run_adf_ecmwa_script", MagicMock(return_value=zarr_path))

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

    with pytest.raises(RuntimeError, match="No publish collection found"):
        await adf_flow.adf_conversion.fn(sample_adf_process_in)

    assert not find_bucket_mock.called
    assert not upload_mock.called
    assert not publish_mock.called
