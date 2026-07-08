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

"""Test the storage_configuration module"""

from unittest.mock import Mock

import pytest
from prefect.variables import Variable

from rs_workflows.payload_template import StoreParams
from rs_workflows.storage_configuration import StorageConfig

# Prefect variable name for the storage configuration
STORAGE_CONFIG = "processing-storage-configuration"


@pytest.fixture(scope="function", autouse=True)
def _config_data():
    """
    Set prefect variable that contains a dictionary representing a sample configuration for testing StorageConfig.
    Includes product-specific, default unit/pipeline storage, and definitions for S3, local, and shared disk storage.
    """
    value = {
        "product": {
            "specific": [
                {"product_name": "PROD_A", "storage": "s3"},
                {"product_name": "PROD_B", "storage": "local_disk"},
            ],
            "default": {
                "unit": [
                    {"section": "input_products", "storage": "s3_default_in"},
                    {"section": "output_products", "storage": "s3_default_out"},
                ],
                "pipeline": [
                    {"section": "pipeline_input", "storage": "s3_pipeline_in"},
                    {"section": "pipeline_output", "storage": "s3_pipeline_out"},
                ],
                "adfs": {"storage": "s3_adfs"},
            },
        },
        "storage_configuration": [
            {
                "name": "s3",
                "kind": "obs",
                "storage_options": {
                    "key": "${S3_KEY}",
                    "secret": "${S3_SECRET}",
                    "endpoint_url": "${S3_ENDPOINT}",
                    "region_name": "${S3_REGION}",
                },
            },
            {"name": "local_disk", "kind": "local_disk", "opening_mode": "rw", "absolute_path": "/data"},
            {"name": "shared_disk", "kind": "shared_disk", "opening_mode": "r", "absolute_path": "/mnt/shared"},
        ],
    }
    Variable.set(STORAGE_CONFIG, value, overwrite=True)
    yield
    Variable.unset(STORAGE_CONFIG)


@pytest.fixture(name="secrets")
def _secrets():
    """
    Returns a dictionary of mock secrets corresponding to the placeholders in config_data.
    """
    return {
        "S3_KEY": "my_key",
        "S3_SECRET": "my_secret",
        "S3_ENDPOINT": "http://minio",
        "S3_REGION": "us-east-1",
    }


def test_init_and_load(secrets):
    """Test initialization and data loading from the configuration file."""
    sc = StorageConfig(secrets)
    assert sc.data is not None
    assert sc.default_adfs_storage == "s3_adfs"


def test_get_storage_for_specific_product(secrets):
    """Test retrieval of storage name for a specific product."""
    sc = StorageConfig(secrets)
    assert sc.get_storage_for_specific_product("PROD_A") == "s3"
    assert sc.get_storage_for_specific_product("PROD_B") == "local_disk"
    assert sc.get_storage_for_specific_product("NON_EXISTENT") is None


def test_get_storage_for_unit_section(secrets):
    """Test retrieval of storage name for a unit section."""
    sc = StorageConfig(secrets)
    assert sc.get_storage_for_unit_section("input_products") == "s3_default_in"
    assert sc.get_storage_for_unit_section("output_products") == "s3_default_out"
    assert sc.get_storage_for_unit_section("unknown") is None


def test_get_storage_for_pipeline_section(secrets):
    """Test retrieval of storage name for a pipeline section."""
    sc = StorageConfig(secrets)
    assert sc.get_storage_for_pipeline_section("pipeline_input") == "s3_pipeline_in"
    assert sc.get_storage_for_pipeline_section("pipeline_output") == "s3_pipeline_out"
    assert sc.get_storage_for_pipeline_section("unknown") is None


def test_get_store_params_s3(secrets):
    """Test retrieval of StoreParams for an S3 storage configuration."""
    sc = StorageConfig(secrets)
    params = sc.get_store_params("s3")
    assert params is not None
    assert isinstance(params, StoreParams)
    assert params.storage_options is not None
    assert params.storage_options.name == "s3"
    assert params.storage_options.key.get_secret_value() == "my_key"
    assert params.storage_options.secret.get_secret_value() == "my_secret"
    # Depending on how secrets are handled (e.g., stripped), check values.
    # The code does .strip("${}") so it looks for keys in secrets dict.
    assert params.storage_options.client_kwargs["endpoint_url"].get_secret_value() == "http://minio"


def test_get_disk_storage_local_disk(secrets):
    """Test retrieval of disk storage configuration for local disk."""
    sc = StorageConfig(secrets)
    params = sc.get_store_params("local_disk")
    assert params is None
    disk_storage = sc.get_disk_storage("local_disk")
    assert disk_storage is not None
    assert disk_storage["opening_mode"] == "rw"
    assert disk_storage["path"] == f"/data/{sc.job_identifier}"
    assert disk_storage["autoclean"] is True


def test_get_disk_storage_shared_disk(secrets):
    """Test retrieval of disk storage configuration for shared disk."""
    sc = StorageConfig(secrets)
    params = sc.get_store_params("shared_disk")
    assert params is None
    disk_storage = sc.get_disk_storage("shared_disk")
    assert disk_storage is not None
    assert disk_storage["opening_mode"] == "r"
    assert disk_storage["path"] == f"/mnt/shared/{sc.job_identifier}"
    assert disk_storage["autoclean"] is False


def test_get_store_params_missing(secrets):
    """Test retrieval of StoreParams for a non-existent storage name."""
    sc = StorageConfig(secrets)
    assert sc.get_store_params("non_existent_storage") is None


def test_missing_secret_warning(secrets):
    """Test that a warning is logged when a required secret is missing."""
    # Remove a secret from the secrets dict but keep it in config
    del secrets["S3_KEY"]

    mock_logger = Mock()
    sc = StorageConfig(secrets, logger=mock_logger)

    # "s3" should fail to load because of missing secret key
    params = sc.get_store_params("s3")
    assert params is None

    # Verify logger warning
    mock_logger.warning.assert_called()
    assert "Secret value for key 'S3_KEY' not found" in str(mock_logger.warning.call_args)


def test_get_all_storage_names(secrets):
    """Test retrieval of all storage configurations."""
    sc = StorageConfig(secrets)
    names = sc.get_all_storage_names()
    assert "s3" in names
    assert "local_disk" in names
    assert "shared_disk" in names


def test_missing_storage():
    """Test error message when the prefect variable is missing"""
    Variable.unset(STORAGE_CONFIG)
    with pytest.raises(RuntimeError, match=f"Prefect variable {STORAGE_CONFIG!r} is missing"):
        StorageConfig(None)
