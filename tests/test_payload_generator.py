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
    fetch_csv_from_endpoint,
    find_s3_output_bucket,
    generate_payload,
    get_first_asset_dir,
    get_io,
    load_store_params_from_config,
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
    _mock_os_env,
):
    """
    Test that get_io correctly builds InputProduct and OutputProduct objects.
    Mocks STAC item resolution and S3 bucket lookup to isolate logic.
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value="s3://mocked/cadip_session",
    )
    # the fetch_csv_from_endpoint function also needs to be mocked; otherwise,
    # it will fail to fetch the file and the test will not pass.
    # this function is already tested separately.
    mocker.patch(
        "rs_workflows.payload_generator.fetch_csv_from_endpoint",
        return_value=[],
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
    # the fetch_csv_from_endpoint function also needs to be mocked; otherwise,
    # it will fail to fetch the file and the test will not pass.
    # this function is already tested separately.
    mocker.patch(
        "rs_workflows.payload_generator.fetch_csv_from_endpoint",
        return_value=[],
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
    # Note: we had to add the pylint disable because it seems the pylint is not
    # smart enough to detect the appropiate types for pydantic 2    
    assert isinstance(result.storage_options, StorageOptions)
    assert result.storage_options.name == "s3"
    # Tell mypy: client_kwargs is not None
    assert result.storage_options.client_kwargs is not None
    assert result.storage_options.client_kwargs["endpoint_url"] == "https://s3.tests.moc"


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
    mock_store_params = StoreParams(storage_options=None)
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
    mock_store_params = StoreParams(storage_options=None)
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


def test_fetch_csv_success(_mock_get_success):
    """
    Tests that fetch_csv_from_endpoint successfully parses a valid CSV response.

    This test uses a mocked successful http get request that returns a well-formed
    csv payload encoded as JSON. The function is expected to:

    - correctly parse the CSV rows,
    - return a list of lists,
    - preserve field order,
    - contain the expected number of rows.
    """
    result = fetch_csv_from_endpoint("https://dummy-osam")
    assert len(result) == 4
    assert result[0] == ["*", "*", "*", "30", "rspython-ops-catalog-all-production"]


def test_fetch_csv_network_error(_mock_get_network_error):
    """
    Tests that network-related failures are converted into RuntimeError.
    """
    with pytest.raises(RuntimeError):
        fetch_csv_from_endpoint("https://dummy-osam")


def test_fetch_csv_invalid_json(_mock_get_invalid_json):
    """
    Tests the behavior when the get response JSON cannot be decoded.
    """
    with pytest.raises(RuntimeError):
        fetch_csv_from_endpoint("https://dummy-osam")


def test_fetch_csv_row_not_list(_mock_get_row_not_list):
    """
    Tests handling of rows that are not lists inside the returned JSON payload.
    """
    with pytest.raises(RuntimeError):
        fetch_csv_from_endpoint("https://dummy-osam")


def test_fetch_csv_non_string(_mock_get_non_string):
    """
    Tests validation of non-string fields inside CSV rows.
    """
    with pytest.raises(RuntimeError):
        fetch_csv_from_endpoint("https://dummy-osam")


def test_fetch_csv_row_wrong_length_too_short(_mock_get_row_wrong_length_too_short):
    """
    Tests handling of CSV rows that contain fewer than the required 5 fields.
    """
    with pytest.raises(RuntimeError):
        fetch_csv_from_endpoint("https://dummy-osam")


def test_fetch_csv_row_wrong_length_too_long(_mock_get_row_wrong_length_too_long):
    """
    Tests handling of CSV rows that contain more than the required 5 fields.
    """
    with pytest.raises(RuntimeError):
        fetch_csv_from_endpoint("https://dummy-osam")


def test_find_s3_output_bucket(_mock_bucket_config_with_fallback):
    """
    Test bucket resolution logic with valid CSV data.
    """
    config_rows = fetch_csv_from_endpoint("https://dummy-osam")

    assert find_s3_output_bucket(config_rows, "test-owner", "my-coll", "L1") == "s3://owner-specific-bucket"

    assert find_s3_output_bucket(config_rows, "other", "other", "L1") == "s3://default-bucket"


def test_find_s3_output_bucket_no_fallback(_mock_bucket_config_no_fallback):
    """
    Test find_s3_output_bucket exception in case of no owner is to be found
    in the bucket resolution logic.
    """
    config_rows = fetch_csv_from_endpoint("https://dummy-osam")

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
