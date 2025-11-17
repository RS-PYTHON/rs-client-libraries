# Copyright 2025 CS Group
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

"""Test the payload_generator module"""
import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pystac import Asset, Item

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_workflows.payload_generator import (
    build_workflow_step,
    find_s3_output_bucket,
    generate_payload,
    get_first_asset_dir,
    get_io,
    load_store_params_from_config,
    read_bucket_config_file,
    resolve_stac_input_path,
    wildcard_match,
)
from rs_workflows.payload_template import (
    GeneralConfiguration,
    InputProduct,
    IOConfig,
    OutputProduct,
    PayloadSchema,
    StorageOptions,
    StoreParams,
    WorkflowStep,
)

# build_workflow_step


def test_build_workflow_step_valid(sample_unit):
    """
    Test that a valid unit dictionary is correctly transformed into a WorkflowStep.
    Verifies all fields: name, module, inputs, adfs, outputs, and regex handling.
    """
    step = build_workflow_step(sample_unit)

    assert isinstance(step, WorkflowStep)
    assert step.name == "unit1"
    assert step.module == "module1"
    assert {"S1CADUS": "S1CADUS"} in (step.inputs or [])
    assert {"S3CADUS": "external_proc"} in (step.inputs or [])
    assert {"adf1": "ADF1"} in (step.adfs or [])
    assert {"*.tif": "output1"} in (step.outputs or [])


def test_build_workflow_step_missing_key_raises():
    """
    Test that missing required keys in the unit (e.g., 'name') raise a clear ValueError.
    """
    with pytest.raises(ValueError, match="Key 'name' not found"):
        build_workflow_step({"module": "no_name"})


# ----------------------------------------------------------------------

# get_io  (now receives flow_env)


def test_get_io_builds_input_and_output(
    sample_unit,
    mock_dpr_process_in,
    mock_store_params,
    flow_env,  # contains catalog_client inside
    mocker,
):
    """
    Test that get_io correctly builds InputProduct and OutputProduct objects.
    Mocks STAC item resolution and S3 bucket lookup to isolate logic.
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value="s3://mocked/cadip_session",
    )
    # the read_bucket_config_file function also needs to be mocked; otherwise,
    # it will fail to locate the file and the test will not pass.
    # this function is already tested separately.
    mocker.patch(
        "rs_workflows.payload_generator.read_bucket_config_file",
        return_value="[]",
    )
    mocker.patch(
        "rs_workflows.payload_generator.find_s3_output_bucket",
        return_value="mocked-output-bucket",
    )

    inputs, outputs = get_io(
        sample_unit,
        mock_dpr_process_in,
        mock_store_params,
        flow_env,
    )

    assert len(inputs) == 2
    assert isinstance(inputs[0], InputProduct)
    assert inputs[0].id == "S1CADUS"
    assert inputs[0].path == "s3://mocked/cadip_session"
    assert inputs[1].id == "S3CADUS"
    assert inputs[1].path == "s3://mocked/cadip_session"

    assert len(outputs) == 2
    assert isinstance(outputs[0], OutputProduct)
    assert outputs[0].id == "output1"
    assert outputs[0].path == "s3://mocked-output-bucket/test-owner/S1A_IW_GRDH_1S"
    assert outputs[1].id == "output2"
    assert outputs[1].path == "s3://mocked-output-bucket/test-owner/OUTPUT_COLLECTION_GRDH"


def test_get_io_missing_field_raises(mock_dpr_process_in, mock_store_params, flow_env, mocker):
    """
    Test that malformed input_products (missing 'name' or 'origin') raise KeyError.
    """
    # the read_bucket_config_file function also needs to be mocked; otherwise,
    # it will fail to locate the file and the test will not pass.
    # this function is already tested separately.
    mocker.patch(
        "rs_workflows.payload_generator.read_bucket_config_file",
        return_value="[]",
    )
    bad_unit = {
        "input_products": [{"store_type": "S3"}],  # missing name/origin
        "output_products": [],
    }
    with pytest.raises(KeyError):
        get_io(bad_unit, mock_dpr_process_in, mock_store_params, flow_env)


# ----------------------------------------------------------------------

# load_store_params_from_config


def test_load_store_params_from_config_valid(mock_storage_config_json):
    """
    Test loading a valid storage_configuration.json file.
    Verifies that S3 StorageOptions are correctly parsed.
    """
    result = load_store_params_from_config(mock_storage_config_json)

    assert isinstance(result, StoreParams)
    assert result.storage_options is not None
    assert len(result.storage_options) == 1
    # Note: we had to add the pylint disable because it seems the pylint is not
    # smart enough to detect the appropiate types for pydantic 2
    s3_opt = result.storage_options[0]  # pylint: disable=unsubscriptable-object
    assert isinstance(s3_opt, StorageOptions)
    assert s3_opt.name == "s3"
    # Tell mypy: client_kwargs is not None
    assert s3_opt.client_kwargs is not None
    assert s3_opt.client_kwargs["endpoint_url"] == "https://s3.tests.moc"


def test_load_store_params_from_config_missing_file():
    """
    Test that a missing configuration file raises FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        load_store_params_from_config("/non/existing/file.json")


def test_load_store_params_from_config_invalid_json(mock_storage_config_invalid_json):
    """
    Test that an invalid configuration file is raising JSONDecodeError.
    """
    with pytest.raises(json.JSONDecodeError):
        load_store_params_from_config(mock_storage_config_invalid_json)


# ----------------------------------------------------------------------

# generate_payload (task wrapper)


def test_generate_payload_success(
    mocker,
    sample_unit,
    mock_dpr_process_in,
    flow_env,
):
    """
    Test successful end-to-end payload generation for a normal processor.
    Mocks store params and get_io; verifies structure and logging.
    """
    mock_store_params = StoreParams(storage_options=[])
    mocker.patch(
        "rs_workflows.payload_generator.load_store_params_from_config",
        return_value=mock_store_params,
    )
    mocker.patch(
        "rs_workflows.payload_generator.get_io",
        return_value=([], []),
    )

    mock_logger = MagicMock()
    mocker.patch("rs_workflows.payload_generator.get_run_logger", return_value=mock_logger)

    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[sample_unit],
        adfs=[("ADF1", "s3://bucket/adf1")],
        dpr_process_in=mock_dpr_process_in,
    )

    assert isinstance(payload, PayloadSchema)
    assert isinstance(payload.io, IOConfig)
    assert isinstance(payload.general_configuration, GeneralConfiguration)
    assert len(payload.workflow or []) == 1
    assert payload.io.adfs[0].id == "ADF1"
    mock_logger.info.assert_any_call("Geting workflow and I/O sections")
    mock_logger.info.assert_any_call("Building the payload")


def test_generate_payload_missing_key_raises(mocker, mock_dpr_process_in, flow_env):
    """
    Test that a unit missing 'name' raises ValueError during payload generation.
    """
    mock_store_params = StoreParams(storage_options=[])
    mocker.patch(
        "rs_workflows.payload_generator.load_store_params_from_config",
        return_value=mock_store_params,
    )
    bad_unit = {"module": "no_name", "input_products": [], "output_products": []}
    with pytest.raises(ValueError, match="Key 'name' not found"):
        generate_payload.fn(
            flow_env=flow_env,
            unit_list=[bad_unit],
            adfs=[],
            dpr_process_in=mock_dpr_process_in,
        )


# ----------------------------------------------------------------------

# Mock-up processor path


def test_generate_payload_mockup_processor(flow_env, mock_dpr_process_in):
    """
    Test the special MOCKUP processor path returns a valid mock payload.
    """
    mock_dpr_process_in.processor_name = DprProcessor.MOCKUP
    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[],
        adfs=[],
        dpr_process_in=mock_dpr_process_in,
    )
    assert isinstance(payload, PayloadSchema)
    assert any(step.name == "mockup_processor" for step in (payload.workflow or []))
    # make mypy happy with the follwing 3 asserts ....
    assert payload.io is not None
    assert payload.io.input_products is not None
    assert len(payload.io.input_products) > 0
    assert payload.io.input_products[0].id == "S3ACADUS"
    assert "S03MWRL0_" in [op.id for op in payload.io.output_products]


# ----------------------------------------------------------------------

# Helper functions added in payload_generator.py


def test_wildcard_match():
    """
    Test wildcard_match supports * for any substring.
    """
    assert wildcard_match("user_test", "*") is True
    assert wildcard_match("collection_user_test", "collection_*") is True
    assert wildcard_match("product_type_1", "product_type_1") is True
    assert wildcard_match("product_type_1", "product_type_2") is False
    assert wildcard_match("product_type_1", "product_type") is False
    assert wildcard_match("abcXYZdef", "abc*def") is True
    assert wildcard_match("abcdef", "abc*def") is True
    assert wildcard_match("xyz", "*xyz") is True
    assert wildcard_match("xyz", "xyz*") is True
    assert wildcard_match("xyz", "x*z") is True
    assert wildcard_match("axyz", "x*z") is False


def test_read_bucket_config_file_valid(mock_bucket_config_with_fallback):
    """
    Test reading a valid bucket routing CSV with correct 5-column format.
    """
    rows = read_bucket_config_file(mock_bucket_config_with_fallback)
    assert len(rows) == 2
    assert rows[0] == ["*", "*", "*", "90", "s3://default-bucket"]
    assert rows[1] == ["test-owner", "my-coll", "S1*", "30", "s3://owner-specific-bucket"]


def test_read_bucket_config_file_missing_file(mock_bucket_config_missing_file):
    """
    Test reading a file that doesn't exist
    """
    with pytest.raises(RuntimeError, match=r".* was not found while resolving S3 bucket mappings"):
        read_bucket_config_file(mock_bucket_config_missing_file)


def test_read_bucket_config_file_malformed_short(mock_malformed_csv_short):
    """
    Test that malformed CSV rows raise RuntimeError with row number (shorter row).
    """
    with pytest.raises(RuntimeError, match=r"Row 1 .* exactly 5 entries"):
        read_bucket_config_file(mock_malformed_csv_short)


def test_read_bucket_config_file_malformed_long(mock_malformed_csv_long):
    """
    Test that malformed CSV rows raise RuntimeError with row number (longer row).
    """
    with pytest.raises(RuntimeError, match=r"Row 1 .* exactly 5 entries"):
        read_bucket_config_file(mock_malformed_csv_long)


def test_find_s3_output_bucket(mock_bucket_config_with_fallback):
    """
    Test bucket resolution logic with valid CSV data.
    """
    config_rows = read_bucket_config_file(mock_bucket_config_with_fallback)

    assert find_s3_output_bucket(config_rows, "test-owner", "my-coll", "S1A") == "s3://owner-specific-bucket"

    assert find_s3_output_bucket(config_rows, "other", "other", "L1") == "s3://default-bucket"


def test_find_s3_output_bucket_no_fallback(mock_bucket_config_no_fallback):
    """
    Test find_s3_output_bucket exception in case of no owner is to be found
    in the bucket resolution logic.
    """
    config_rows = read_bucket_config_file(mock_bucket_config_no_fallback)

    with pytest.raises(RuntimeError, match="Unable to determine the output bucket"):
        find_s3_output_bucket(config_rows, "nobody", "none", "NONE")


def test_get_first_asset_dir():
    """
    Test extraction of directory path from first asset href (S3 and local).
    """

    item = Item(
        id="test",
        geometry=None,
        bbox=None,
        datetime=None,
        properties={},
        stac_extensions=[],
        start_datetime=datetime(2023, 1, 1),
        end_datetime=datetime(2023, 1, 2),
    )
    item.add_asset(
        "name_of_asset",
        Asset(href="s3://catalog-bucket/items/asset.raw"),
    )
    assert get_first_asset_dir(item) == "s3://catalog-bucket/items"

    item.assets.clear()
    item.add_asset(
        "local",
        Asset(href="/local/abs/path/file.raw", media_type="application/octet-stream"),
    )
    assert get_first_asset_dir(item) == "/local/abs/path"


def test_resolve_stac_input_path(mocker, catalog_client):
    """
    Test resolve_stac_input_path returns the directory of the first asset
    from a STAC item (via get_first_asset_dir).
    """
    # Mock a real-looking STAC Item with one asset
    mock_asset = MagicMock(href="s3://catalog-bucket/items/item123/asset.raw")
    mock_item = MagicMock()
    mock_item.assets = {"data": mock_asset}

    # get_item returns our mock item
    mocker.patch.object(catalog_client, "get_item", return_value=mock_item)

    # get_first_asset_dir returns the directory (this is what the function uses)
    mocker.patch("rs_workflows.payload_generator.get_first_asset_dir", return_value="s3://catalog-bucket/items/item123")

    result = resolve_stac_input_path(catalog_client, "my-collection", "item123")

    assert result == "s3://catalog-bucket/items/item123"
