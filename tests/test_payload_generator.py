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
from unittest.mock import MagicMock

import pytest

from rs_workflows.payload_generator import (
    build_workflow_step,
    generate_payload,
    get_io,
    load_store_params_from_config,
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

# Tests for build_workflow_step


def test_build_workflow_step_valid(sample_unit):
    """Test building a valid WorkflowStep from a well-formed unit."""
    step = build_workflow_step(sample_unit)

    assert isinstance(step, WorkflowStep)
    assert step.name == "unit1"
    assert step.module == "module1"
    assert any(d == {"input1": "input1"} for d in (step.inputs or []))
    assert any(d == {"adf1": "ADF1"} for d in (step.adfs or []))
    assert any(d == {"*.tif": "output1"} for d in (step.outputs or []))


def test_build_workflow_step_missing_key_raises():
    """Ensure missing required keys raise a ValueError."""
    with pytest.raises(ValueError):
        build_workflow_step({"module": "no_name"})


# Tests for get_io


def test_get_io_builds_input_and_output(sample_unit, mock_dpr_process_in, mock_store_params):
    """Verify correct InputProduct and OutputProduct creation."""
    inputs, outputs = get_io(sample_unit, mock_dpr_process_in, mock_store_params)

    assert all(isinstance(i, InputProduct) for i in inputs)
    assert all(isinstance(o, OutputProduct) for o in outputs)
    assert inputs[0].id == "input1"
    assert outputs[0].id == "output1"
    assert outputs[0].store_type == "S3"
    assert outputs[0].path == "s3://mocked/output/path"


def test_get_io_missing_field_raises(mock_dpr_process_in, mock_store_params):
    """Ensure KeyError is raised when input_products is missing required keys."""
    bad_unit = {"input_products": [{"store_type": "S3"}], "output_products": []}
    with pytest.raises(KeyError):
        get_io(bad_unit, mock_dpr_process_in, mock_store_params)


def test_load_store_params_from_config_valid(tmp_path):
    """Test with a valid storage configuration"""
    config_data = {
        "storage": [
            {
                "name": "s3",
                "storage_options": {
                    "key": "S3_KEY",
                    "secret": "S3_SECRET",
                    "endpoint_url": "https://s3_fake",
                    "region_name": "fake_region",
                },
            },
            {
                "name": "shared_disk",
                "relative_path": "/mnt/shared",
                "opening_mode": "CREATE_OVERWRITE",
            },
        ],
    }

    config_file = tmp_path / "storage_configuration.json"
    config_file.write_text(json.dumps(config_data))

    result = load_store_params_from_config(str(config_file))

    assert isinstance(result, StoreParams)
    assert result.storage_options is not None
    # The test should be changed with == 2 IF the load_store_params_from_config
    # is changed back to add all the found elements !
    assert len(result.storage_options) == 1
    # pylint: disable=unsubscriptable-object
    # s3_opts = result.options[0].storage_options[0]
    s3_opts = result.storage_options[0]
    # pylint: enable=unsubscriptable-object
    assert isinstance(s3_opts, StorageOptions)
    assert s3_opts.client_kwargs is not None
    assert s3_opts.client_kwargs["endpoint_url"] == "https://s3_fake"


def test_load_store_params_from_config_missing(tmp_path):
    """Test with a missing storage configuration"""
    missing_file = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        load_store_params_from_config(str(missing_file))


# TESTS FOR generate_payload


def test_generate_payload_success(mocker, sample_unit, mock_dpr_process_in):
    """Test successful generation of the payload schema with mocked Prefect logger."""
    mock_store_params = StoreParams(storage_options=[])
    mocker.patch(
        "rs_workflows.payload_generator.load_store_params_from_config",
        return_value=mock_store_params,
    )

    mock_logger = MagicMock()
    mocker.patch("rs_workflows.payload_generator.get_run_logger", return_value=mock_logger)

    mock_env = MagicMock()
    mock_adfs = [("AUXIP", "s3://bucket/path/to/adf1")]

    payload = generate_payload.fn(
        env=mock_env,
        unit_list=[sample_unit],
        adfs=mock_adfs,
        dpr_process_in=mock_dpr_process_in,
    )

    assert isinstance(payload, PayloadSchema)
    assert isinstance(payload.io, IOConfig)
    assert isinstance(payload.general_configuration, GeneralConfiguration)
    assert len(payload.workflow or []) == 1
    assert payload.io.input_products[0].id == "input1"
    assert payload.io.output_products[0].id == "output1"
    assert payload.io.adfs[0].id == "AUXIP"

    mock_logger.info.assert_any_call("Geting workflow and I/O sections")
    mock_logger.info.assert_any_call("Building the payload")


def test_generate_payload_missing_key_raises(mocker, mock_dpr_process_in):
    """Test that missing keys in unit raise ValueError."""
    mock_store_params = StoreParams(storage_options=[])
    mocker.patch(
        "rs_workflows.payload_generator.load_store_params_from_config",
        return_value=mock_store_params,
    )

    mock_logger = MagicMock()
    mocker.patch("rs_workflows.payload_generator.get_run_logger", return_value=mock_logger)

    bad_unit = {"module": "no_name", "input_products": [], "output_products": []}
    with pytest.raises(ValueError):
        generate_payload.fn(
            env=MagicMock(),
            unit_list=[bad_unit],
            adfs=[],
            dpr_process_in=mock_dpr_process_in,
        )
