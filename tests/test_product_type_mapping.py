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

"""Unit tests for product type mapping helpers."""

import pytest

from rs_workflows.product_type_mapping import (
    find_product_type,
    load_product_type_mapping,
)


@pytest.fixture(autouse=True)
def clear_product_type_mapping_cache():
    """Keep cached YAML mappings from leaking between tests."""
    # load_product_type_mapping is lru-cached in production so repeated workflow
    # calls do not reload the same YAML file. Tests exercise different paths and
    # environment values, so each test needs a clean cache on entry and exit.
    load_product_type_mapping.cache_clear()
    yield
    load_product_type_mapping.cache_clear()


def test_load_product_type_mapping_from_path(tmp_path):
    """Load mapping entries from an explicit YAML path."""
    # Use a tiny synthetic mapping instead of the packaged configuration so the
    # test only verifies loader behavior, not the contents of the real mapping.
    mapping_file = tmp_path / "product_type_mapping.yaml"
    mapping_file.write_text(
        """
types:
  - productType: S01SIWRAW
    legacyType: IW_RAW__0N
    mission: S1
""",
        encoding="utf-8",
    )

    # Passing a path explicitly must take precedence over the default packaged
    # config and should return the raw list under the YAML "types" key.
    mapping = load_product_type_mapping(str(mapping_file))

    assert mapping == [
        {
            "productType": "S01SIWRAW",
            "legacyType": "IW_RAW__0N",
            "mission": "S1",
        },
    ]


def test_load_product_type_mapping_uses_environment_override(monkeypatch, tmp_path):
    """Use PTYPE_MAPPING_CONFIG when no explicit path is provided."""
    # Deployments can override the packaged mapping with PTYPE_MAPPING_CONFIG.
    # The test writes a temporary file and points the env var at it to keep the
    # assertion isolated from the developer machine and installed package data.
    mapping_file = tmp_path / "override_mapping.yaml"
    mapping_file.write_text(
        """
types:
  - productType: S03OLCL0_
    legacyType: OL_0_EFR___
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("PTYPE_MAPPING_CONFIG", str(mapping_file))

    # No path is passed here on purpose: this is the environment override path.
    assert load_product_type_mapping() == [
        {
            "productType": "S03OLCL0_",
            "legacyType": "OL_0_EFR___",
        },
    ]


@pytest.mark.parametrize(
    "contents",
    [
        "types: []\n",
        "[]\n",
    ],
)
def test_load_product_type_mapping_rejects_invalid_yaml_shape(tmp_path, contents):
    """Reject files that do not expose a non-empty types list."""
    # The helper expects a mapping shaped like {"types": [...]}. Empty lists and
    # non-dict YAML values should fail early because find_product_type relies on
    # at least one entry to build its default "not found" response.
    mapping_file = tmp_path / "invalid_mapping.yaml"
    mapping_file.write_text(contents, encoding="utf-8")

    with pytest.raises(RuntimeError, match="Invalid product type mapping file"):
        load_product_type_mapping(str(mapping_file))


def test_find_product_type_matches_exact_legacy_type():
    """Return the first entry whose legacyType matches exactly."""
    # Most mappings are plain legacy product type strings. The implementation
    # uses re.fullmatch for all entries, which still handles exact strings.
    mapping_data = [
        {
            "productType": "S01SIWRAW",
            "legacyType": "IW_RAW__0N",
            "mission": "S1",
        },
    ]

    assert find_product_type("IW_RAW__0N", mapping_data) == mapping_data[0]


def test_find_product_type_matches_regex_legacy_type():
    """Support regex legacyType entries from the packaged mapping."""
    # Some legacyType values intentionally describe a family of SAFE product
    # names. This protects the regex matching path used by entries such as the
    # Sentinel-1 GRD mapping in the packaged YAML.
    mapping_data = [
        {
            "productType": "S01SSMGRD",
            "legacyType": "S[1-6]_GRD[FHM]_1[AS]",
        },
    ]

    assert find_product_type("S1_GRDH_1S", mapping_data) == mapping_data[0]


def test_find_product_type_returns_default_when_no_match_found():
    """Return a default mapping shape with None values when no entry matches."""
    # Callers expect a dict with the same keys as a real mapping entry. This is
    # why the "not found" result is not simply None: downstream code can still
    # read mapping["productType"] and decide how to fail.
    mapping_data = [
        {
            "productType": "S01SIWRAW",
            "legacyType": "IW_RAW__0N",
            "mission": "S1",
        },
    ]

    assert find_product_type("UNKNOWN_TYPE", mapping_data) == {
        "productType": None,
        "legacyType": None,
        "mission": None,
    }


def test_find_product_type_falls_back_for_invalid_regex_pattern():
    """Invalid regex patterns can still match by direct equality."""
    # The production helper mirrors rs-server-common's defensive behavior:
    # if a malformed legacyType cannot be compiled as regex, it can still match
    # when the requested product type is exactly the same string.
    mapping_data = [
        {
            "productType": "INVALID_PATTERN_MATCH",
            "legacyType": "[",
        },
    ]

    assert find_product_type("[", mapping_data) == mapping_data[0]
