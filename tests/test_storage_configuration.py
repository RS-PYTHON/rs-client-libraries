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

import json
from unittest.mock import Mock

from rs_workflows.payload_template import StoreParams
from rs_workflows.storage_configuration import StorageConfig


def test_init_and_load(secrets, config_file):
    """Test initialization and data loading from the configuration file."""
    sc = StorageConfig(secrets, config_file)
    assert sc.data is not None
    assert sc.default_adfs_storage == "s3_adfs"


def test_get_storage_for_specific_product(secrets, config_file):
    """Test retrieval of storage name for a specific product."""
    sc = StorageConfig(secrets, config_file)
    assert sc.get_storage_for_specific_product("PROD_A") == "s3_prod_a"
    assert sc.get_storage_for_specific_product("PROD_B") == "local_disk"
    assert sc.get_storage_for_specific_product("NON_EXISTENT") is None


def test_get_storage_for_unit_section(secrets, config_file):
    """Test retrieval of storage name for a unit section."""
    sc = StorageConfig(secrets, config_file)
    assert sc.get_storage_for_unit_section("input_products") == "s3_default_in"
    assert sc.get_storage_for_unit_section("output_products") == "s3_default_out"
    assert sc.get_storage_for_unit_section("unknown") is None


def test_get_storage_for_pipeline_section(secrets, config_file):
    """Test retrieval of storage name for a pipeline section."""
    sc = StorageConfig(secrets, config_file)
    assert sc.get_storage_for_pipeline_section("pipeline_input") == "s3_pipeline_in"
    assert sc.get_storage_for_pipeline_section("pipeline_output") == "s3_pipeline_out"
    assert sc.get_storage_for_pipeline_section("unknown") is None


def test_get_store_params_s3(secrets, config_file):
    """Test retrieval of StoreParams for an S3 storage configuration."""
    sc = StorageConfig(secrets, config_file)
    params = sc.get_store_params("s3_prod_a")
    assert params is not None
    assert isinstance(params, StoreParams)
    assert params.storage_options is not None
    assert params.storage_options.name == "s3_prod_a"
    assert params.storage_options.key.get_secret_value() == "my_key"
    assert params.storage_options.secret.get_secret_value() == "my_secret"
    # Depending on how secrets are handled (e.g., stripped), check values.
    # The code does .strip("${}") so it looks for keys in secrets dict.
    assert params.storage_options.client_kwargs["endpoint_url"].get_secret_value() == "http://minio"


def test_get_store_params_local_disk(secrets, config_file):
    """Test retrieval of StoreParams for a local disk storage configuration."""
    sc = StorageConfig(secrets, config_file)
    params = sc.get_store_params("local_disk")
    assert params is not None
    assert params.storage_path is not None
    assert params.storage_path.name == "local_disk"
    assert params.storage_path.opening_mode == "rw"
    assert params.storage_path.relative_path == "/data"


def test_get_store_params_shared_disk(secrets, config_file):
    """Test retrieval of StoreParams for a shared disk storage configuration."""
    sc = StorageConfig(secrets, config_file)
    params = sc.get_store_params("shared_disk")
    assert params is not None
    assert params.storage_path is not None
    assert params.storage_path.name == "shared_disk"
    assert params.storage_path.opening_mode == "r"


def test_get_store_params_missing(secrets, config_file):
    """Test retrieval of StoreParams for a non-existent storage name."""
    sc = StorageConfig(secrets, config_file)
    assert sc.get_store_params("non_existent_storage") is None


def test_missing_secret_warning(tmp_path, sample_config_data, secrets):
    """Test that a warning is logged when a required secret is missing."""
    # Remove a secret from the secrets dict but keep it in config
    del secrets["S3_KEY"]

    p = tmp_path / "config_missing_secret.json"
    p.write_text(json.dumps(sample_config_data), encoding="utf-8")

    mock_logger = Mock()
    sc = StorageConfig(secrets, str(p), logger=mock_logger)

    # "s3_prod_a" should fail to load because of missing secret key
    params = sc.get_store_params("s3_prod_a")
    assert params is None

    # Verify logger warning
    mock_logger.warning.assert_called()
    assert "Secret value for key 'S3_KEY' not found" in str(mock_logger.warning.call_args)


def test_get_all_storage_names(secrets, config_file):
    """Test retrieval of all storage configurations."""
    sc = StorageConfig(secrets, config_file)
    all_params = sc.get_all_storage_names()
    names = []
    for p in all_params:
        if p.storage_options:
            names.append(p.storage_options.name)
        elif p.storage_path:
            names.append(p.storage_path.name)

    assert "s3_prod_a" in names
    assert "local_disk" in names
    assert "shared_disk" in names
