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

"""
Module for managing storage configuration.

This module defines the StorageConfig class, which is responsible for loading
and parsing storage configuration from a JSON file. It establishes mappings
for product-specific storage, default unit storage, and pipeline storage,
and resolves storage credentials using provided secrets (through prefect block)

Each storage entry in the configuration file must declare a `kind` field
indicating the storage backend type:

- `obs`         - object storage (s3-compatible), resolved via secrets (prefect block)
- `shared_disk` - shared filesystem mounted into the processing pod. Admin will provide
the shared disk the users can use, thus, the <absolute_path> will reflect the path on the
processing node where the shared disk is mounted. Here, only the ${JOB_IDENTIFIER} variable is
added (an UUID generated once per StorageConfig instance).
- `local_disk`  - local filesystem on the processing node
"""

import os
from uuid import uuid4

from prefect.variables import Variable

from rs_workflows.payload_template import StorageOptions, StoreParams

VALID_STORAGE_KINDS = ("obs", "shared_disk", "local_disk")


class StorageConfig:  # pylint: disable=too-many-instance-attributes
    """
    A class to load and query the storage configuration info.
    """

    def __init__(self, secrets: dict, logger=None):

        # Read data from the prefect variable
        var_name = "processing-storage-configuration"
        self.data = Variable.get(var_name)
        if self.data is None:
            raise RuntimeError(f"Prefect variable {var_name!r} is missing")

        # a single UUID shared across all shared-disk products in one processing run
        self._job_identifier = str(uuid4())

        # build quick-lookups for faster access
        self._specific_storage = {item["product_name"]: item["storage"] for item in self.data["product"]["specific"]}

        self._default_unit_storage = {
            item["section"]: item["storage"] for item in self.data["product"]["default"]["unit"]
        }

        self._default_pipeline_storage = {
            item["section"]: item["storage"] for item in self.data["product"]["default"]["pipeline"]
        }
        self._store_params = []
        self._disk_storages: dict[str, dict] = {}
        # map storage name kind
        self._storage_kinds: dict[str, str] = {}

        for idx, conf in enumerate(self.data["storage_configuration"]):
            if "name" not in conf or "kind" not in conf:
                if logger:
                    logger.warning(
                        f"Storage configuration entry at index {idx} is missing required "
                        "'name' or 'kind' field. This entry will be ignored.",
                    )
                continue

            name = conf["name"]
            kind = conf["kind"]
            self._storage_kinds[name] = kind

            if kind == "obs":
                if "storage_options" not in conf:
                    if logger:
                        logger.warning(
                            f"Storage configuration entry for OBS storage '{name}' is missing "
                            "required 'storage_options' field. This entry will be ignored.",
                        )
                    continue
                try:
                    storage_options = StorageOptions(
                        name=name,
                        key=secrets[conf["storage_options"]["key"].strip("${}")],
                        secret=secrets[conf["storage_options"]["secret"].strip("${}")],
                        client_kwargs={
                            "endpoint_url": secrets[conf["storage_options"]["endpoint_url"].strip("${}")],
                            "region_name": secrets[conf["storage_options"]["region_name"].strip("${}")],
                        },
                    )
                    self._store_params.append(StoreParams(storage_options=storage_options))
                except KeyError as ke:
                    if logger:
                        logger.warning(
                            f"Secret value for key {ke} not found in the prefect "
                            f"block secrets. This section from template will not be usable.",
                        )
                    continue

            elif kind in ("shared_disk", "local_disk"):
                # Build the full path as <absolute_path>/<JOB_IDENTIFIER>
                absolute_path = conf.get("absolute_path")
                if absolute_path is None or absolute_path.strip() == "":
                    if logger:
                        logger.warning(
                            f"Storage configuration entry for {kind} storage '{name}' is missing "
                            "required 'absolute_path' field or it is empty. This entry will be ignored.",
                        )
                    continue
                full_path = os.path.join(absolute_path, self._job_identifier)
                # For local_disk, autoclean is always True;
                # for shared_disk, read it from the config (defaults to False)
                autoclean = True if kind == "local_disk" else conf.get("autoclean", False)
                self._disk_storages[name] = {
                    "path": full_path,
                    "opening_mode": conf.get("opening_mode", "CREATE_OVERWRITE"),
                    "autoclean": autoclean,
                }

        self.default_adfs_storage = self.data["product"]["default"]["adfs"]["storage"]

    @property
    def job_identifier(self) -> str:
        """Return the UUID used as JOB_IDENTIFIER for this flow run."""
        return self._job_identifier

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
        (e.g., 's3', 'my_shared_disk_1', etc.).
        """
        for store_param in self._store_params:
            if store_param.storage_options and store_param.storage_options.name == storage_name:
                return store_param
        return None

    def get_disk_storage(self, storage_name: str) -> dict | None:
        """Return the disk storage configuration for a given storage name."""
        return self._disk_storages.get(storage_name, None)

    def get_storage_kind(self, storage_name: str) -> str | None:
        """
        Return the kind ('obs', 'shared_disk', 'local_disk') for a given storage name.
        """
        return self._storage_kinds.get(storage_name, None)

    def get_all_storage_names(self) -> list[str]:
        """Return a list of all defined storage names."""
        names = []
        for store_param in self._store_params:
            if store_param.storage_options:
                names.append(store_param.storage_options.name)
        names.extend(self._disk_storages.keys())
        return names
