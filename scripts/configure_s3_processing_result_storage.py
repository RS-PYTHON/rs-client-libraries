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

"""Register the shared-disk Prefect result storage used by S3 processing."""

import os
from pathlib import Path

from prefect.filesystems import LocalFileSystem
from prefect.variables import Variable

PREFECT_VAR_NAME = "processing-storage-configuration"


def get_storage_path():
    """
    Resolve and return the absolute path for the shared disk storage configured
    in Prefect variables.

    This function reads the Prefect variable named by `PREFECT_VAR_NAME` and
    extracts `storage_configuration`. It then scans the entries to find the
    one matching the following criteria:

    - The entry is a dict
    - `entry["kind"] == "shared_disk"`
    - The entry provides both `entry["name"]` and `entry["absolute_path"]`
    - If `entry["opening_mode"]` is set, it must be `CREATE_OVERWRITE` (case-insensitive) to ensure the path is writable

    Returns:
        str: The selected storage entry's `absolute_path`

    Raises:
        RuntimeError:
            If the Prefect variable cannot be loaded, if `storage_configuration` is
            missing or not a list, or if no suitable writable shared disk entry is
            found.
    """
    try:
        result = Variable.get(PREFECT_VAR_NAME)
    except Exception as exc:
        raise RuntimeError(
            f"Unable to load Prefect variable {PREFECT_VAR_NAME!r}",
        ) from exc
    storage_configuration = result.get("storage_configuration")
    if not storage_configuration or not isinstance(storage_configuration, list):
        raise RuntimeError(
            "Failed to resolve Prefect values, storage_configuration is not a list",
        )
    for entry in storage_configuration:
        if not isinstance(entry, dict):
            continue
        if entry.get("kind") != "shared_disk":
            continue
        if not entry.get("name") or not entry.get("absolute_path"):
            continue
        opening_mode = entry.get("opening_mode")
        # make sure the path is not read only
        if opening_mode is not None and opening_mode.upper() != "CREATE_OVERWRITE":
            continue
        return entry.get("absolute_path").rstrip("/")
    raise RuntimeError(f"Failed to get the shared mounted path from the Prefect values: {storage_configuration}")


BLOCK_NAME = "s3-processing-shared-results"
SHARED_RESULTS_PATH = Path(f"{get_storage_path()}/prefect-results")


def main() -> None:
    """Create the shared directory and register its LocalFileSystem block."""
    SHARED_RESULTS_PATH.mkdir(parents=True, exist_ok=True)
    if not os.access(SHARED_RESULTS_PATH, os.W_OK):
        raise RuntimeError(f"Shared result path is not writable: {SHARED_RESULTS_PATH}")

    block_id = LocalFileSystem(basepath=str(SHARED_RESULTS_PATH)).save(BLOCK_NAME, overwrite=True)
    print(
        f"Saved Prefect block local-file-system/{BLOCK_NAME} with id={block_id} " f"and basepath={SHARED_RESULTS_PATH}",
    )


if __name__ == "__main__":
    main()
