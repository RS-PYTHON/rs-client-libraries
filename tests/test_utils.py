# Copyright 2024 CS Group
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

"""Unit tests for utility funtions."""

import pytest
import requests  # type: ignore
import responses

from rs_common.utils import get_href_service, read_response_error
from rs_workflows.catalog_flow import resolve_collection


@responses.activate
def test_response_error():
    """Test reading responses errors."""

    dummy_href = "https://DUMMY_HREF"
    detail = "detail message"
    error = "error message"
    content = "response content"
    timeout = 10  # seconds

    responses.get(url=dummy_href, status=500, json={"detail": detail})
    assert read_response_error(requests.get(dummy_href, timeout=timeout)) == detail

    responses.get(url=dummy_href, status=500, json={"error": error})
    assert read_response_error(requests.get(dummy_href, timeout=timeout)) == error

    responses.get(url=dummy_href, status=500, body=content)
    assert read_response_error(requests.get(dummy_href, timeout=timeout)) == content


def test_get_href_service(
    set_db_env_var,  # pylint: disable=unused-argument
):
    """Test the get_href_service function."""

    rs_server_href = "https://dummy-rs-server-href/endpoint/"
    assert get_href_service(rs_server_href, "RSPY_HOST_CATALOG") == "https://dummy-catalog/catalog"
    assert get_href_service(rs_server_href, "RSPY_HOST_CADIP") == "https://dummy-cadip/cadip"
    assert get_href_service(rs_server_href, "RSPY_HOST_AUXIP") == "https://dummy-auxip/auxip"
    assert get_href_service(rs_server_href, "RSPY_HOST_PRIP") == "https://dummy-prip/prip"
    assert get_href_service(rs_server_href, "RSPY_HOST_STAGING") == "https://dummy-staging/staging"
    assert get_href_service(rs_server_href, "RSPY_HOST_UNKNWON") == rs_server_href.rstrip("/")


def test_resolve_collection_tuple():
    """Check resolve_collection works with tuple (product_type, collection) values."""
    input_collections = [
        {"output_folder1": ("product_type_1", "collection_1")},
        {"output_folder2": ("product_type_2", "collection_2")},
    ]
    assert resolve_collection("product_type_1", input_collections) == "collection_1"
    assert resolve_collection("product_type_2", input_collections) == "collection_2"
    with pytest.raises(ValueError):
        resolve_collection("tip_42", input_collections)


def test_resolve_collection_string():
    """Check resolve_collection works with string values as product types."""
    input_collections = [{"output_folder1": "product_type_1"}, {"output_folder2": "product_type_2"}]
    assert resolve_collection("product_type_1", input_collections) == "product_type_1"
    assert resolve_collection("product_type_2", input_collections) == "product_type_2"
    with pytest.raises(ValueError):
        # Not found in input_collections
        resolve_collection("tip_42", input_collections)


def test_resolve_collection_dict_mixed():
    """Check resolve_collection works with mixed dict: tuple and string values."""
    input_collections = {"output_folder1": ("product_type_1", "collection_1"), "output_folder2": "product_type_2"}

    # Tuple case: returns collection part
    assert resolve_collection("product_type_1", input_collections) == "collection_1"

    # String case: returns the string itself
    assert resolve_collection("product_type_2", input_collections) == "product_type_2"

    # Unknown product type should raise
    with pytest.raises(ValueError):
        resolve_collection("tip_42", input_collections)
