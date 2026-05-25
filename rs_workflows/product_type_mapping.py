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

"""Lightweight product type mapping helpers for workflow execution."""

import os
import re
from functools import lru_cache
from pathlib import Path

import yaml

# Keep this helper lightweight for workflow execution: do not import rs-server-common here.
# The packaged YAML is a copy of the server mapping used to resolve legacy product types.
DEFAULT_PTYPE_MAPPING_FILE = Path(__file__).resolve().parents[1] / "config" / "product_type_mapping.yaml"


@lru_cache
def load_product_type_mapping(path: str | None = None) -> list[dict]:
    """Load product type mapping data from the packaged YAML file or an override path."""
    # PTYPE_MAPPING_CONFIG lets deployments override the packaged mapping without changing code.
    mapping_path = path or os.environ.get("PTYPE_MAPPING_CONFIG")
    mapping_file = Path(mapping_path) if mapping_path else DEFAULT_PTYPE_MAPPING_FILE
    with mapping_file.open("r", encoding="utf-8") as file:
        mapping_data = yaml.safe_load(file)

    types = mapping_data.get("types") if isinstance(mapping_data, dict) else None
    if not isinstance(types, list) or not types:
        raise RuntimeError(f"Invalid product type mapping file: {mapping_file}")

    return types


def find_product_type(product_type: str, mapping_data: list[dict] | None = None) -> dict:
    """Find the first mapping entry whose legacyType matches the given product type."""
    mapping = mapping_data or load_product_type_mapping()
    default = {key: None for key in mapping[0]}

    for item in mapping:
        pattern = item.get("legacyType", "")
        try:
            # Most legacyType values are exact strings, but some entries are regex patterns.
            if re.fullmatch(pattern, product_type):
                return item
        except (TypeError, re.error):
            # Keep the same defensive fallback as rs-server-common for invalid or empty patterns.
            if pattern == product_type:
                return item

    return default
