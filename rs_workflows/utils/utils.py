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

"""General utilities"""

import re
from os.path import commonprefix
from typing import Any


def search_by_name(values: list[dict[str, Any]], name: str) -> Any:
    """Return the first item whose "name" field matches the given value."""
    return next((obj for obj in values if obj.get("name") == name), None)


def get_common_and_relative_paths(paths: list[str]) -> tuple[str, list[str]]:
    """
    Returns the common folder prefix of the given paths and their escaped
    relative parts.

    The common prefix is truncated to the last `/` so it always represents
    a directory.

    Args:
        paths (list[str]): List of absolute paths or URIs.

    Returns:
        tuple[str, list[str]]:
            - Common folder ending with `/`
            - Escaped relative parts for each path
    """
    # Compute longest common prefix
    common_prefix = commonprefix(paths)
    # Ensure prefix stops on folder boundary
    common_folder = common_prefix[: common_prefix.rfind("/") + 1]
    # List relative names inside common folder
    relative_parts = [re.escape(path.removeprefix(common_folder)) for path in paths]
    return common_folder, relative_parts
