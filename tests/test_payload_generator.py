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

"""Test the payload_generator module"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr
from pystac import Asset, Item

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_workflows.flow_utils import (
    FlowGeneratedProduct,
    FlowInputProduct,
)
from rs_workflows.payload_generator import (  # load_store_params_from_config,
    DATA_EDH_DOMAIN,
    build_adfs,
    build_input_products,
    build_output_products,
    build_workflow_step,
    fetch_csv_from_endpoint,
    find_s3_output_bucket,
    generate_payload,
    get_first_asset_dir,
    get_io,
    resolve_stac_input_path,
    wildcard_match,
)
from rs_workflows.payload_template import (
    GeneralConfiguration,
    InputProduct,
    IOConfig,
    OutputProduct,
    PayloadSchema,
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
    assert step.name == "unit1.1"
    assert step.module == "module1"
    assert step.inputs == {"S1CADUS": "S1CADUS", "S3CADUS": "external_proc"}
    assert step.adfs == {"ADF1": "ADF1"}
    assert step.outputs == {"*.tif": "output1", "output2": "output2"}
    assert step.processing_unit == "unit1"
    assert step.parameters == {"testparam": "testvalue"}


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
        return_value=(None, "s3://mocked/cadip_session"),
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
    mocker.patch(
        "rs_workflows.payload_generator.uuid4",
        return_value="00000000-0000-0000-0000-000000000000",
    )

    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.get_storage_for_specific_product.return_value = "s3"
    mock_storage_config.get_storage_kind.return_value = "obs"

    inputs, outputs = get_io(
        sample_unit,
        mock_dpr_process_in,
        flow_env,
        mock_storage_config,
        [],
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
    assert outputs[0].path == "s3://mocked-output-bucket/test-owner/S1A_IW_GRDH_1S/00000000-0000-0000-0000-000000000000"
    assert outputs[1].id == "output2"
    assert (
        outputs[1].path
        == "s3://mocked-output-bucket/test-owner/OUTPUT_COLLECTION_GRDH/00000000-0000-0000-0000-000000000000"
    )


def test_get_io_missing_field_raises(mock_dpr_process_in, mock_store_params, flow_env, mocker):
    """
    Test that malformed input_products (missing 'name' or 'origin') raise RuntimeError.
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
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params

    with pytest.raises(RuntimeError):
        get_io(bad_unit, mock_dpr_process_in, flow_env, mock_storage_config, [])


# ----------------------------------------------------------------------

# generate_payload (task wrapper)


@pytest.mark.parametrize(
    "processor_name, expected_logging, expected_config",
    [
        (
            DprProcessor.S1L0,
            "/opt/dask-l0/logging_config.yaml",
            ["/opt/dask-l0/s1_default_configuration.yaml", "/opt/dask-l0/cadu_configuration.yaml"],
        ),
        (
            DprProcessor.S3L0,
            "/opt/dask-l0/logging_config.yaml",
            ["/opt/dask-l0/s3_default_configuration.yaml", "/opt/dask-l0/cadu_configuration.yaml"],
        ),
    ],
)
def test_generate_payload_success(
    mocker,
    sample_unit,
    mock_dpr_process_in,
    flow_env,
    processor_name,
    expected_logging,
    expected_config,
    _mock_os_env,
):
    """
    Test successful end-to-end payload generation for a normal processor.
    Mocks store params and get_io; verifies structure and logging.
    """
    mock_store_params = StoreParams(storage_options=None)
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    mocker.patch(
        "rs_workflows.payload_generator.StorageConfig",
        return_value=mock_storage_config,
    )
    mocker.patch(
        "rs_workflows.payload_generator.get_io",
        return_value=([], []),
    )
    mocker.patch(
        "rs_workflows.payload_generator.fetch_csv_from_endpoint",
        return_value=[],
    )

    mock_logger = MagicMock()
    mocker.patch("rs_workflows.payload_generator.get_run_logger", return_value=mock_logger)

    mock_secret = MagicMock()
    mock_secret.get.return_value = {"S3_ACCESSKEY": "dummy", "S3_SECRETKEY": "dummy"}
    mocker.patch("rs_workflows.payload_generator.Secret.load", return_value=mock_secret)

    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[sample_unit],
        adfs=[("ADF1", "filename", "s3://bucket/adf1")],
        dpr_process_in=mock_dpr_process_in,
    )

    assert isinstance(payload, PayloadSchema)
    assert isinstance(payload.io, IOConfig)
    assert isinstance(payload.general_configuration, GeneralConfiguration)
    assert len(payload.workflow or []) == 1
    assert payload.io.adfs[0].id == "ADF1"
    assert payload.logging is None
    assert payload.config is None
    mock_logger.info.assert_any_call("Building workflow and I/O sections")
    mock_logger.info.assert_any_call("Building the payload")

    # test the s1 l0 specific logging and config paths
    mock_dpr_process_in.processor_name = processor_name

    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[sample_unit],
        adfs=[("ADF1", "filename", "s3://bucket/adf1")],
        dpr_process_in=mock_dpr_process_in,
    )
    assert payload.logging == expected_logging
    assert payload.config == expected_config


def test_generate_payload_sets_datatree_and_default_filename_only_for_olci(
    mocker,
    sample_unit,
    mock_dpr_process_in,
    flow_env,
    _mock_os_env,
):
    """
    OLCI requires these triggering options, but they must not be emitted for other processors.
    """
    mocker.patch(
        "rs_workflows.payload_generator.load_storage_configuration",
        return_value=MagicMock(default_adfs_storage="s3"),
    )
    mocker.patch(
        "rs_workflows.payload_generator.get_io",
        return_value=(
            [InputProduct(id="input", path="s3://mocked/input", store_type="s3")],
            [OutputProduct(id="output", path="s3://mocked/output", store_type="s3")],
        ),
    )
    mocker.patch("rs_workflows.payload_generator.fetch_csv_from_endpoint", return_value=[])
    mocker.patch("rs_workflows.payload_generator.get_run_logger", return_value=MagicMock())
    mock_secret = MagicMock()
    mock_secret.get.return_value = {"S3_ACCESSKEY": "dummy", "S3_SECRETKEY": "dummy"}
    mocker.patch("rs_workflows.payload_generator.Secret.load", return_value=mock_secret)

    mock_dpr_process_in.processor_name = "TEST_PROCESSOR"
    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[sample_unit],
        adfs=[],
        dpr_process_in=mock_dpr_process_in,
    )
    general_configuration = payload.general_configuration.dump()
    assert "triggering__use_datatree" not in general_configuration
    assert "triggering__use_default_filename" not in general_configuration

    mock_dpr_process_in.processor_name = DprProcessor.S3L1OLCI
    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[sample_unit],
        adfs=[],
        dpr_process_in=mock_dpr_process_in,
    )
    general_configuration = payload.general_configuration.dump()
    assert general_configuration["triggering__use_datatree"] is True
    assert general_configuration["triggering__use_default_filename"] is True


def test_generate_payload_missing_key_raises(mocker, mock_dpr_process_in, flow_env, _mock_os_env):
    """
    Test that a unit missing 'name' raises ValueError during payload generation.
    """
    mock_store_params = StoreParams(storage_options=None)
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params

    mocker.patch(
        "rs_workflows.payload_generator.StorageConfig",
        return_value=mock_storage_config,
    )
    mocker.patch(
        "rs_workflows.payload_generator.fetch_csv_from_endpoint",
        return_value=[],
    )

    mock_secret = MagicMock()
    mock_secret.get.return_value = {"S3_ACCESSKEY": "dummy", "S3_SECRETKEY": "dummy"}
    mocker.patch("rs_workflows.payload_generator.Secret.load", return_value=mock_secret)

    bad_unit = {"module": "no_name", "input_products": [], "output_products": []}
    with pytest.raises(ValueError, match="Key 'name' not found"):
        generate_payload.fn(
            flow_env=flow_env,
            unit_list=[bad_unit],
            adfs=[],
            dpr_process_in=mock_dpr_process_in,
        )


def test_generate_payload_deduplicates_io(mocker, sample_unit, mock_dpr_process_in, flow_env, _mock_os_env):
    """
    When two units share the same input/output product id, generate_payload must
    include each id only once in the payload (regression test for the duplicate
    input_products bug).
    """
    shared_input = InputProduct(id="slcs", path="s3://bucket/slcs", store_type="s3")
    shared_output = OutputProduct(id="out1", path="s3://bucket/out1", store_type="s3")

    mocker.patch(
        "rs_workflows.payload_generator.StorageConfig",
        return_value=MagicMock(default_adfs_storage="s3"),
    )
    mocker.patch(
        "rs_workflows.payload_generator.get_io",
        return_value=([shared_input], [shared_output]),
    )
    mocker.patch("rs_workflows.payload_generator.fetch_csv_from_endpoint", return_value=[])
    mocker.patch("rs_workflows.payload_generator.get_run_logger", return_value=MagicMock())
    mock_secret = MagicMock()
    mock_secret.get.return_value = {"S3_ACCESSKEY": "dummy", "S3_SECRETKEY": "dummy"}
    mocker.patch("rs_workflows.payload_generator.Secret.load", return_value=mock_secret)

    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[sample_unit, sample_unit],
        adfs=[],
        dpr_process_in=mock_dpr_process_in,
    )

    assert payload.io is not None
    assert len(payload.io.input_products) == 1
    assert payload.io.input_products[0].id == "slcs"
    assert len(payload.io.output_products) == 1
    assert payload.io.output_products[0].id == "out1"


# ----------------------------------------------------------------------

# Mock-up processor path


def test_generate_payload_mockup_processor(mocker, flow_env, mock_dpr_process_in, _mock_os_env):
    """
    Test the MOCKUP processor path uses the generic payload generation.
    """
    mockup_unit = {
        "name": "single_unit",
        "module": "l0.s1.mockup_processor",
        "input_products": [{"name": "S1CADUS", "origin": "pipeline_input", "store_type": "cadu"}],
        "input_adfs": [],
        "output_products": [{"name": "S03OLCL0_", "store_type": "zarr"}],
    }
    mocker.patch(
        "rs_workflows.payload_generator.StorageConfig",
        return_value=MagicMock(default_adfs_storage="s3"),
    )
    mocker.patch(
        "rs_workflows.payload_generator.get_io",
        return_value=(
            [InputProduct(id="S1CADUS", path="s3://mocked/input", store_type="cadu")],
            [OutputProduct(id="S03OLCL0_", path="s3://mocked/output", store_type="zarr")],
        ),
    )
    mocker.patch("rs_workflows.payload_generator.fetch_csv_from_endpoint", return_value=[])
    mock_secret = MagicMock()
    mock_secret.get.return_value = {"S3_ACCESSKEY": "dummy", "S3_SECRETKEY": "dummy"}
    mocker.patch("rs_workflows.payload_generator.Secret.load", return_value=mock_secret)

    mock_dpr_process_in.processor_name = "MOCKUP"
    payload = generate_payload.fn(
        flow_env=flow_env,
        unit_list=[mockup_unit],
        adfs=[],
        dpr_process_in=mock_dpr_process_in,
    )
    assert isinstance(payload, PayloadSchema)
    assert any(step.module == "l0.s1.mockup_processor" for step in (payload.workflow or []))
    # make mypy happy with the follwing 3 asserts ....
    assert payload.io is not None
    assert payload.io.input_products is not None
    assert len(payload.io.input_products) > 0
    assert payload.io.input_products[0].id == "S1CADUS"
    assert "S03OLCL0_" in [op.id for op in payload.io.output_products]


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


def test_find_s3_output_bucket_priority_1():
    """Test case for Priority 1: Exact match for owner_id and output_collection"""
    config_rows = [
        ["user1", "coll1", "*", "30", "bucket1"],
        ["user1", "*", "*", "30", "bucket2"],
        ["*", "*", "*", "30", "fallback"],
    ]
    assert find_s3_output_bucket(config_rows, "user1", "coll1", "type1") == "bucket1"


def test_find_s3_output_bucket_owner_fallback():
    """Test case for Priority 2: Match for owner_id only (coll_pat is *)"""
    config_rows = [
        ["user1", "other_coll", "*", "30", "bucket1"],
        ["user1", "*", "*", "30", "bucket2"],
        ["*", "*", "*", "30", "fallback"],
    ]
    assert find_s3_output_bucket(config_rows, "user1", "coll1", "type1") == "bucket2"


def test_find_s3_output_bucket_global_fallback():
    """Test case for Priority 3: Global fallback (*, *, *)"""
    config_rows = [
        ["other_user", "*", "*", "30", "bucket1"],
        ["*", "*", "*", "30", "fallback"],
    ]
    assert find_s3_output_bucket(config_rows, "user1", "coll1", "type1") == "fallback"


def test_find_s3_output_bucket_no_match():
    """Test case where no matching bucket is found"""
    config_rows = [
        ["other_user", "*", "*", "30", "bucket1"],
    ]
    with pytest.raises(RuntimeError, match="Unable to determine the output bucket"):
        find_s3_output_bucket(config_rows, "user1", "coll1", "type1")


def test_find_s3_output_bucket_priority_order():
    """Test case for complex priority order scenario"""
    config_rows = [
        ["*", "*", "*", "30", "fallback"],
        ["user1", "*", "*", "30", "bucket_owner"],
        ["user1", "coll1", "*", "30", "bucket_exact"],
    ]
    # Should get bucket_exact
    assert find_s3_output_bucket(config_rows, "user1", "coll1", "type1") == "bucket_exact"
    # Should get bucket_owner for other coll
    assert find_s3_output_bucket(config_rows, "user1", "other_coll", "type1") == "bucket_owner"
    # Should get fallback for other user
    assert find_s3_output_bucket(config_rows, "other_user", "other_coll", "other_type") == "fallback"


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

    result_item, result_path = resolve_stac_input_path(catalog_client, "my-collection", "item123")

    assert result_item == mock_item
    assert result_path == "s3://catalog-bucket/items/item123"


# ----------------------------------------------------------------------

# build_input_products


def test_build_input_products_success(sample_unit, mock_store_params, mocker):
    """
    Test successful build of input products with specific storage configuration.
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, "s3://path/to/item"),
    )

    mock_dpr = MagicMock()
    mock_dpr.input_products = [FlowInputProduct(name="S1CADUS", item_id="item_id", collection_name="coll_id")]
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params

    inputs = build_input_products(sample_unit, mock_dpr, mock_storage, MagicMock())

    assert len(inputs) == 1
    assert inputs[0].id == "S1CADUS"
    assert inputs[0].store_type == "S3"
    assert inputs[0].path == "s3://path/to/item"
    assert inputs[0].store_params == mock_store_params


def test_build_input_products_success_multiple_inputs_regex(sample_unit, mock_store_params, mocker):
    """
    Test successful build of input products when multiple inputs share the same name (regex case).
    """
    # Mock STAC resolution to return different paths
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        side_effect=[
            ("item1", "s3://path/to/item1"),
            ("item2", "s3://path/to/item2"),
        ],
    )

    # Create multiple input products with SAME name
    mock_dpr = MagicMock()
    mock_dpr.input_products = [
        FlowInputProduct(name="S1CADUS", item_id="item1", collection_name="coll"),
        FlowInputProduct(name="S1CADUS", item_id="item2", collection_name="coll"),
    ]

    # Mock storage config
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params

    # Ensure mock_store_params behaves like StoreParams
    assert isinstance(mock_store_params, StoreParams)

    inputs = build_input_products(sample_unit, mock_dpr, mock_storage, MagicMock())

    # Assertions
    assert len(inputs) == 1  # regex => single InputProduct

    input_product = inputs[0]
    assert input_product.id == "S1CADUS"
    assert input_product.store_type == "S3"
    assert input_product.path == "s3://path/to/"
    assert input_product.type == "regex"

    # Checks generated regex
    assert isinstance(input_product.store_params, StoreParams)
    assert input_product.store_params.regex == r"(item1|item2)"
    assert input_product.store_params.multiplicity == "2"


def test_build_input_products_missing_mapping(sample_unit):
    """
    Test that RuntimeError is raised when input product is not found in unit definition.
    """
    mock_dpr = MagicMock()
    # "UNKNOWN" is not in sample_unit's input_products
    mock_dpr.input_products = [FlowInputProduct(name="UNKNOWN", item_id="item_id", collection_name="coll_id")]

    with pytest.raises(RuntimeError, match="Couldn't find any input"):
        build_input_products(sample_unit, mock_dpr, MagicMock(), MagicMock())


def test_build_input_products_missing_storage(sample_unit, mocker):
    """
    Test that RuntimeError is raised when storage configuration cannot be resolved.
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, "s3://path/to/item"),
    )

    mock_dpr = MagicMock()
    mock_dpr.input_products = [FlowInputProduct(name="S1CADUS", item_id="item_id", collection_name="coll_id")]
    # clear flags to avoid fallbacks triggering if checked before specific storage
    mock_dpr.unit = False
    mock_dpr.pipeline = False

    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = None
    # Ensure fallbacks also return None if called (though we disabled flags)
    mock_storage.get_storage_for_unit_section.return_value = None
    mock_storage.get_storage_for_pipeline_section.return_value = None

    with pytest.raises(RuntimeError, match="Couldn't find any storage configuration"):
        build_input_products(sample_unit, mock_dpr, mock_storage, MagicMock())


def test_build_input_products_fallback_storage_unit(sample_unit, mock_store_params, mocker):
    """
    Test fallback to unit section storage if specific product storage is missing.
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, "s3://path/to/item"),
    )

    mock_dpr = MagicMock()
    mock_dpr.input_products = [FlowInputProduct(name="S1CADUS", item_id="item_id", collection_name="coll_id")]
    mock_dpr.unit = True  # Enable unit fallback
    mock_dpr.pipeline = False

    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = None
    mock_storage.get_storage_for_unit_section.return_value = "fallback_s3"
    mock_storage.get_store_params.return_value = mock_store_params

    inputs = build_input_products(sample_unit, mock_dpr, mock_storage, MagicMock())

    assert len(inputs) == 1
    mock_storage.get_storage_for_unit_section.assert_called_with("input_products")
    mock_storage.get_store_params.assert_called_with("fallback_s3")


def test_build_input_products_fallback_storage_pipeline(sample_unit, mock_store_params, mocker):
    """
    Test fallback to pipeline section storage if specific product storage and unit fallback are missing.
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, "s3://path/to/item"),
    )

    mock_dpr = MagicMock()
    mock_dpr.input_products = [FlowInputProduct(name="S1CADUS", item_id="item_id", collection_name="coll_id")]
    mock_dpr.unit = False
    mock_dpr.pipeline = True  # Enable pipeline fallback

    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = None
    mock_storage.get_storage_for_pipeline_section.side_effect = lambda section: {
        "pipeline_input_1": None,
        "other": "pipeline_s3",
    }[section]
    mock_storage.get_store_params.return_value = mock_store_params

    inputs = build_input_products(sample_unit, mock_dpr, mock_storage, MagicMock())

    assert len(inputs) == 1
    mock_storage.get_storage_for_pipeline_section.assert_any_call("pipeline_input_1")
    mock_storage.get_storage_for_pipeline_section.assert_any_call("other")
    # store_name is forced to "s3" regardless of the pipeline resolution (temporary workaround)
    mock_storage.get_store_params.assert_called_with("s3")


# ----------------------------------------------------------------------

# build_adfs


def _make_dpr_process_in(edh_api_key=None):
    """Helper to create a minimal DprProcessIn mock for build_adfs tests."""
    mock = MagicMock()
    mock.edh_api_key = edh_api_key
    return mock


def test_build_adfs_single_folder(mock_store_params):
    """Test ADFS of type 'folder' with a single file"""
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    adfs = [
        ("adf1", "folder", "/data/myfolder/file1.txt"),
    ]

    result = build_adfs(mock_storage_config, adfs, _make_dpr_process_in())

    assert len(result) == 1
    assert result[0].id == "adf1"
    assert result[0].path == "/data/myfolder"


def test_build_adfs_single_filename(mock_store_params):
    """Test ADFS of type 'filename' with a single file"""
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    adfs = [
        ("adf1", "filename", "/data/file1.txt"),
    ]

    result = build_adfs(mock_storage_config, adfs, _make_dpr_process_in())

    assert len(result) == 1
    assert result[0].id == "adf1"
    assert result[0].path == "/data/file1.txt"


def test_build_adfs_multiple_entries(mock_store_params):
    """Test ADFS with multiple files"""
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    adfs = [
        ("adf1", "filename", "/data/folder/file1.txt"),
        ("adf1", "filename", "/data/folder/file2.txt"),
    ]

    result = build_adfs(mock_storage_config, adfs, _make_dpr_process_in())

    assert len(result) == 1
    adf = result[0]

    assert adf.id == "adf1"
    assert adf.path == "/data/folder/"
    assert isinstance(adf.store_params, StoreParams)
    assert adf.store_params.multiplicity == "2"
    assert adf.store_params.regex == r"(file1\.txt|file2\.txt)"


def test_build_adfs_edh_url_replaced_with_api_key(mock_store_params):
    """EDH URL is rewritten to inject the API key and becomes a SecretStr."""
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    edh_url = f"https://{DATA_EDH_DOMAIN}/copernicus-dem-30m/tile.tif"
    adfs = [("DEM", "filename", edh_url)]

    result = build_adfs(mock_storage_config, adfs, _make_dpr_process_in(edh_api_key="my-secret-token"))

    assert len(result) == 1
    path = result[0].path
    assert isinstance(path, SecretStr), "EDH path with injected API key must be a SecretStr"
    full_url = path.get_secret_value()
    assert "my-secret-token" in full_url
    assert DATA_EDH_DOMAIN not in full_url
    assert full_url == "https://edh:my-secret-token@api.earthdatahub.destine.eu/copernicus-dem-30m/tile.tif"


def test_build_adfs_edh_url_secret_hides_token(mock_store_params):
    """The SecretStr representation must not expose the API key in plain text."""
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    edh_url = f"https://{DATA_EDH_DOMAIN}/copernicus-dem-30m/tile.tif"
    adfs = [("DEM", "filename", edh_url)]

    result = build_adfs(mock_storage_config, adfs, _make_dpr_process_in(edh_api_key="my-secret-token"))

    path = result[0].path
    assert isinstance(path, SecretStr)
    assert "my-secret-token" not in str(path)
    assert "my-secret-token" not in repr(path)


def test_build_adfs_no_edh_key_url_unchanged(mock_store_params):
    """EDH URL is left unchanged when no API key is provided."""
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    edh_url = f"https://{DATA_EDH_DOMAIN}/copernicus-dem-30m/tile.tif"
    adfs = [("DEM", "filename", edh_url)]

    result = build_adfs(mock_storage_config, adfs, _make_dpr_process_in(edh_api_key=None))

    path = result[0].path
    assert not isinstance(path, SecretStr)
    assert path == edh_url


def test_build_adfs_non_edh_url_not_replaced(mock_store_params):
    """A non-EDH https URL is not replaced even when an API key is provided."""
    mock_storage_config = MagicMock()
    mock_storage_config.get_store_params.return_value = mock_store_params
    mock_storage_config.default_adfs_storage = "s3"

    other_url = "https://some-other-service.example.com/data/file.tif"
    adfs = [("ADF1", "filename", other_url)]

    result = build_adfs(mock_storage_config, adfs, _make_dpr_process_in(edh_api_key="my-secret-token"))

    path = result[0].path
    assert not isinstance(path, SecretStr)
    assert path == other_url


# ----------------------------------------------------------------------

# build_output_products


def test_build_output_products_specific_storage(
    sample_unit,
    mock_dpr_process_in,
    mock_store_params,
    mocker,
):
    """
    Test output products with specific storage configuration in unit.
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params
    mock_storage.get_storage_kind.return_value = "obs"

    # Provide mappings for BOTH output1 and output2
    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="output1_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="output2_type", collection_name="OUT_COLL"),
    ]

    mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket", return_value="out-bucket")
    mocker.patch(
        "rs_workflows.payload_generator.uuid4",
        return_value="00000000-0000-0000-0000-000000000000",
    )

    outputs = build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])

    assert len(outputs) == 2
    assert outputs[0].id == "output1"
    assert outputs[0].store_type == "S3"
    assert outputs[0].path == "s3://out-bucket/test-owner/OUT_COLL/00000000-0000-0000-0000-000000000000"

    assert outputs[1].id == "output2"
    assert outputs[1].store_type == "S3"
    assert outputs[1].path == "s3://out-bucket/test-owner/OUT_COLL/00000000-0000-0000-0000-000000000000"

    mock_storage.get_storage_for_specific_product.assert_called_with("output2")  # last call


def test_build_output_products_fallback_unit(
    sample_unit,
    mock_dpr_process_in,
    mock_store_params,
    mocker,
):
    """
    Test output products with fallback on unit.
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = None
    mock_storage.get_storage_for_unit_section.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params
    mock_storage.get_storage_kind.return_value = "obs"

    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="output1_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="output2_type", collection_name="OUT_COLL"),
    ]
    mock_dpr_process_in.unit = True
    mock_dpr_process_in.pipeline = False

    mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket", return_value="s3://out/bucket")

    outputs = build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])

    assert len(outputs) == 2
    assert outputs[0].store_type == "S3"
    assert outputs[1].store_type == "S3"


def test_build_output_products_fallback_pipeline(
    sample_unit,
    mock_dpr_process_in,
    mock_store_params,
    mocker,
):
    """
    Test output products with fallback on pipeline.
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = None
    mock_storage.get_storage_for_pipeline_section.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params
    mock_storage.get_storage_kind.return_value = "obs"

    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="output1_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="output2_type", collection_name="OUT_COLL"),
    ]
    mock_dpr_process_in.unit = False
    mock_dpr_process_in.pipeline = True

    mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket", return_value="s3://out/bucket")

    outputs = build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])

    assert len(outputs) == 2
    assert outputs[0].store_type == "S3"
    assert outputs[1].store_type == "S3"


def test_build_output_products_error_no_storage(
    sample_unit,
    mock_dpr_process_in,
):
    """
    Test RuntimeError when no storage configuration is found for output products.
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = None
    mock_storage.get_storage_for_unit_section.return_value = None
    mock_storage.get_storage_for_pipeline_section.return_value = None

    # Even with two mappings, error is raised if no storage config
    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="output1_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="output2_type", collection_name="OUT_COLL"),
    ]
    mock_dpr_process_in.unit = False
    mock_dpr_process_in.pipeline = False

    with pytest.raises(RuntimeError, match="Couldn't find any storage configuration for output product"):
        build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])


def test_build_output_products_missing_relation_raises(sample_unit, mock_dpr_process_in, mock_store_params, mocker):
    """
    Test that build_output_products raises an error if an output product
    defined in the unit is missing from dpr_process_in.generated_product_to_collection_identifier.
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params
    mock_storage.get_storage_kind.return_value = "obs"

    # sample_unit has "output1" and "output2"
    # We provide only "output1" in generated_product_to_collection_identifier, so "output2" should trigger the error
    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="output1_type", collection_name="OUT_COLL"),
    ]

    mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket", return_value="out-bucket")
    mocker.patch(
        "rs_workflows.payload_generator.uuid4",
        return_value="00000000-0000-0000-0000-000000000000",
    )

    # This should raise RuntimeError with the proposed change
    with pytest.raises(
        RuntimeError,
        match="Missing mapping in generated_product_to_collection_identifier for task table entry 'output2'",
    ):
        build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])


def test_build_output_products_extra_mapping_is_ignored(sample_unit, mock_dpr_process_in, mock_store_params, mocker):
    """
    Extra items in generated_product_to_collection_identifier
    must NOT raise an error and should be ignored.
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params
    mock_storage.get_storage_kind.return_value = "obs"

    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="type1", collection_name="COLL"),
        FlowGeneratedProduct(name="output2", product_type="type2", collection_name="COLL"),
        FlowGeneratedProduct(name="UNKNOWN", product_type="type", collection_name="COLL"),  # extra
    ]

    mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket", return_value="out-bucket")
    mocker.patch(
        "rs_workflows.payload_generator.uuid4",
        return_value="00000000-0000-0000-0000-000000000000",
    )

    result = build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])

    assert len(result) == 2
    assert {r.id for r in result} == {"output1", "output2"}


def test_build_output_products_wildcard_collection_raises(sample_unit, mock_dpr_process_in):
    """
    Test that build_output_products raises an error if the resolved output_collection is '*'.
    """
    # Ensure "output1" is in dpr_process_in and its collection_name resolves to '*'
    # In build_output_products: output_collection = output_product.collection_name if ... else product_type
    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="*"),
    ]
    # We also need to mock output2 in the unit to not trigger the "missing relation" error at the end,
    # OR we provide both. But output1 is enough to trigger the '*' error first.

    # Mock output2 to satisfy the final check if we reach it (but we shouldn't)
    mock_dpr_process_in.generated_product_to_collection_identifier.append(
        FlowGeneratedProduct(name="output2", product_type="type2", collection_name="COLL2"),
    )

    with pytest.raises(RuntimeError, match="cannot be '\\*' if the collection name is not specified"):
        build_output_products(sample_unit, mock_dpr_process_in, MagicMock(), "test-owner", [])


def test_build_output_products_ignores_extra_generated_products(mock_dpr_process_in, mock_store_params, mocker):
    """
    Task table contains a single required output (S01SARRAW).
    generated_product_to_collection_identifier contains that entry + extras per RSPY-1039

    Expected:
    - No error is raised
    - Only S01SARRAW is processed
    - Extra entries are ignored
    """

    unit = {
        "output_products": [
            {
                "name": "S01SARRAW",
                "store_type": "s3",
                "type": "filename",
                "opening_mode": "CREATE",
                "final_product": True,
            },
        ],
    }

    # --- Mapping parameter (flow input) ---
    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="S01SARRAW", product_type="*", collection_name="s01sarraw"),
        FlowGeneratedProduct(name="S01GPSRAW", product_type="*", collection_name="s01gpsraw"),
        FlowGeneratedProduct(name="S01HKMRAW", product_type="*", collection_name="s01hkmraw"),
        FlowGeneratedProduct(name="S01AISRAW", product_type="*", collection_name="s01aisraw"),
    ]

    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "S3"
    mock_storage.get_store_params.return_value = mock_store_params
    mock_storage.get_storage_kind.return_value = "obs"

    mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket", return_value="out-bucket")
    mocker.patch(
        "rs_workflows.payload_generator.uuid4",
        return_value="00000000-0000-0000-0000-000000000000",
    )

    result = build_output_products(unit, mock_dpr_process_in, mock_storage, "test-owner", [])

    assert len(result) == 1
    assert result[0].id == "S01SARRAW"
    assert "s01sarraw" in result[0].path  # ensures correct collection used


# ----------------------------------------------------------------------

# shared_disk / local_disk paths


@pytest.mark.parametrize("kind", ["shared_disk", "local_disk"])
def test_build_input_products_disk_store_params_cleared(sample_unit, kind, mocker):
    """
    For shared_disk and local_disk input products the store_params must be None (disk
    storages don't use S3 credentials)
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, "/mnt/shared/path/to/item"),
    )

    mock_dpr = MagicMock()
    mock_dpr.input_products = [FlowInputProduct(name="S1CADUS", item_id="item_id", collection_name="coll_id")]

    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "my_disk"
    mock_storage.get_storage_kind.return_value = kind
    mock_storage.get_store_params.return_value = None  # disk storages have no StoreParams
    mock_storage.get_disk_storage.return_value = {
        "path": "/mnt/shared/job-uuid",
        "opening_mode": "READ_ONLY",
        "autoclean": False,
    }

    inputs = build_input_products(sample_unit, mock_dpr, mock_storage, MagicMock())

    assert len(inputs) == 1
    inp = inputs[0]
    assert inp.id == "S1CADUS"
    assert inp.path == "/mnt/shared/path/to/item"  # path comes from STAC, not from disk_config
    assert inp.store_params is None  # cleared for disk storages
    assert inp.opening_mode == "READ_ONLY"


@pytest.mark.parametrize("kind", ["shared_disk", "local_disk"])
def test_build_input_products_disk_no_disk_config(sample_unit, kind, mocker):
    """
    When get_disk_storage returns None for a disk kind storage, the opening_mode
    should remain None and store_params should still be None.
    """
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, "/mnt/shared/path/to/item"),
    )

    mock_dpr = MagicMock()
    mock_dpr.input_products = [FlowInputProduct(name="S1CADUS", item_id="item_id", collection_name="coll_id")]

    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "my_disk"
    mock_storage.get_storage_kind.return_value = kind
    mock_storage.get_store_params.return_value = None
    mock_storage.get_disk_storage.return_value = None  # no disk config entry

    inputs = build_input_products(sample_unit, mock_dpr, mock_storage, MagicMock())

    assert len(inputs) == 1
    inp = inputs[0]
    assert inp.store_params is None
    assert inp.opening_mode is None  # no disk_config → opening_mode stays None


@pytest.mark.parametrize("kind", ["shared_disk", "local_disk"])
def test_build_output_products_disk_uses_disk_path(
    sample_unit,
    mock_dpr_process_in,
    kind,
    mocker,
):
    """
    For shared_disk and local_disk output products path must come from disk_config["path"],
    not from a bucket lookup and store_params must be None
    """
    disk_path = "/mnt/shared/job-uuid"
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "my_disk"
    mock_storage.get_storage_kind.return_value = kind
    mock_storage.get_store_params.return_value = None
    mock_storage.get_disk_storage.return_value = {
        "path": disk_path,
        "opening_mode": "CREATE_OVERWRITE",
        "autoclean": True,
    }

    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="out_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="out_type2", collection_name="OUT_COLL2"),
    ]

    mock_find_bucket = mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket")

    outputs = build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])

    mock_find_bucket.assert_not_called()  # disk path → no S3 bucket resolution

    assert len(outputs) == 2
    for out in outputs:
        assert out.path == disk_path
        assert out.store_params is None
        assert out.opening_mode == "CREATE_OVERWRITE"
        assert out.autoclean is True


@pytest.mark.parametrize("kind", ["shared_disk", "local_disk"])
def test_build_output_products_disk_autoclean_false(
    sample_unit,
    mock_dpr_process_in,
    kind,
    mocker,
):
    """
    When autoclean is False in disk_config the OutputProduct must reflect that.
    This is in case for shared_disk (it seems that the local_disk may be discarded in future,
    according to our internal discussions)
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "my_disk"
    mock_storage.get_storage_kind.return_value = kind
    mock_storage.get_store_params.return_value = None
    mock_storage.get_disk_storage.return_value = {
        "path": "/mnt/shared/job-uuid",
        "opening_mode": "CREATE",
        "autoclean": False,
    }

    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="out_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="out_type2", collection_name="OUT_COLL2"),
    ]

    mocker.patch("rs_workflows.payload_generator.find_s3_output_bucket")

    outputs = build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])

    assert len(outputs) == 2
    for out in outputs:
        assert out.autoclean is False


@pytest.mark.parametrize("kind", ["shared_disk", "local_disk"])
def test_build_output_products_disk_missing_path_raises(
    sample_unit,
    mock_dpr_process_in,
    kind,
):
    """
    When the disk_config entry exists but contains no 'path' key,
    a RuntimeError must be raised explaining which storage and product are affected.
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "my_disk"
    mock_storage.get_storage_kind.return_value = kind
    mock_storage.get_store_params.return_value = None
    mock_storage.get_disk_storage.return_value = {
        # 'path' key is intentionally absent
        "opening_mode": "CREATE_OVERWRITE",
        "autoclean": False,
    }

    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="out_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="out_type2", collection_name="OUT_COLL2"),
    ]

    with pytest.raises(RuntimeError, match="has no storage path configured"):
        build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])


@pytest.mark.parametrize("kind", ["shared_disk", "local_disk"])
def test_build_output_products_disk_no_disk_config_raises(
    sample_unit,
    mock_dpr_process_in,
    kind,
):
    """
    When get_disk_storage returns None for a disk kind storage,
    a RuntimeError must be raised (the processor has nowhere to write).
    """
    mock_storage = MagicMock()
    mock_storage.get_storage_for_specific_product.return_value = "my_disk"
    mock_storage.get_storage_kind.return_value = kind
    mock_storage.get_store_params.return_value = None
    mock_storage.get_disk_storage.return_value = None  # no disk config at all

    mock_dpr_process_in.generated_product_to_collection_identifier = [
        FlowGeneratedProduct(name="output1", product_type="out_type", collection_name="OUT_COLL"),
        FlowGeneratedProduct(name="output2", product_type="out_type2", collection_name="OUT_COLL2"),
    ]

    with pytest.raises(RuntimeError, match="has no storage path configured"):
        build_output_products(sample_unit, mock_dpr_process_in, mock_storage, "test-owner", [])
