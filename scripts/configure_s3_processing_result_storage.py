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

BLOCK_NAME = "s3-processing-shared-results"
SHARED_RESULTS_PATH = Path("/mnt/share/cs-01/prefect-results")


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
