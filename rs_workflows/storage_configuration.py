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

"""
Module for managing storage configuration.

This module defines the StorageConfig class, which is responsible for loading
and parsing storage configuration from a JSON file. It establishes mappings
for product-specific storage, default unit storage, and pipeline storage,
and resolves storage credentials using provided secrets.
"""

import json

from rs_workflows.payload_template import StorageOptions, StoragePath, StoreParams


class StorageConfig:
    """
    A class to load and query the storage_configuration.json file.
    """

    def __init__(self, secrets: dict, config_path: str, logger=None):
        with open(config_path, encoding="utf-8") as f:
            self.data = json.load(f)

        # Build quick-lookups for faster access
        self._specific_storage = {item["product_name"]: item["storage"] for item in self.data["product"]["specific"]}

        self._default_unit_storage = {
            item["section"]: item["storage"] for item in self.data["product"]["default"]["unit"]
        }

        self._default_pipeline_storage = {
            item["section"]: item["storage"] for item in self.data["product"]["default"]["pipeline"]
        }
        self._store_params = []

        for conf in self.data["storage_configuration"]:
            if "name" in conf:
                if "storage_options" in conf:
                    try:
                        storage_options = StorageOptions(
                            name=conf["name"],
                            key=secrets[conf["storage_options"]["key"].strip("${}")],
                            secret=secrets[conf["storage_options"]["secret"].strip("${}")],
                            client_kwargs={
                                "endpoint_url": secrets[conf["storage_options"]["endpoint_url"].strip("${}")],
                                "region_name": secrets[conf["storage_options"]["region_name"].strip("${}")],
                            },
                        )
                        self._store_params.append(StoreParams(storage_options=storage_options))
                    except KeyError as ke:
                        logger.warning(
                            f"Secret value for key {ke} not found in the prefect "
                            f"block secrets. This section from template will not be usable.",
                        )
                        continue
                elif conf["name"] in ("shared_disk", "local_disk"):
                    self._store_params.append(
                        StoreParams(
                            storage_path=StoragePath(
                                name=conf["name"],
                                opening_mode=conf["opening_mode"],
                                relative_path=conf["relative_path"],
                            ),
                        ),
                    )

        self.default_adfs_storage = self.data["product"]["default"]["adfs"]["storage"]

    def get_storage_for_specific_product(self, product_name: str) -> str | None:
        """
        Return the storage name for a specific product.
        """
        return self._specific_storage.get(product_name, None)

    def get_storage_for_unit_section(self, section: str) -> str | None:
        """Get storage for a unit section (input_products, output_products, etc.)"""
        return self._default_unit_storage.get(section, None)

    def get_storage_for_pipeline_section(self, section: str) -> str | None:
        """Get storage for a pipeline section (pipeline_input, pipeline_output, other, etc.)"""
        return self._default_pipeline_storage.get(section, None)

    def get_store_params(self, storage_name: str) -> StoreParams | None:
        """
        Return the store parameters for a given storage name
        (e.g., 's3', 'shared_disk', etc.).
        """
        for store_param in self._store_params:
            if store_param.storage_options and store_param.storage_options.name == storage_name:
                return store_param
            if store_param.storage_path and store_param.storage_path.name == storage_name:
                return store_param
        return None

    def get_all_storage_names(self) -> list[StoreParams]:
        """Return a list of all defined storage names."""
        return self._store_params
