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

"""Unit tests for utility funtions."""

import json
from contextlib import suppress

import pytest
import requests  # type: ignore
import responses
from prefect.blocks.system import Secret
from pydantic import SecretStr

from rs_common import prefect_utils
from rs_common.utils import get_href_service, read_response_error
from rs_workflows.catalog_flow import resolve_collection
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessedItemMetadata,
    FlowGeneratedProduct,
    FlowInputProduct,
)
from tests.conftest import (
    MOCKED_RSPY_WEBSITE,
    OWNER_ID,
    S3_ACCESSKEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRETKEY,
)

RSPY_APIKEY = "RSPY_APIKEY"


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


def test_resolve_collection_generated_product(mocker):
    """Check resolve_collection works with a list of FlowGeneratedProduct instances."""
    mocker.patch("rs_workflows.catalog_flow.get_run_logger")

    input_collections: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="product_name_1", product_type="product_type_1", collection_name="collection_1"),
        FlowGeneratedProduct(name="product_name_2", product_type="product_type_2", collection_name="collection_2"),
        FlowGeneratedProduct(name="product_name_3", product_type="*", collection_name="collection_3"),
    ]

    # Exact match
    meta_1 = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="product_type_1",
        output_product_id="product_name_1",
    )
    assert resolve_collection(meta_1, input_collections) == "collection_1"

    # Match with wildcard type
    meta_3 = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="any_type",
        output_product_id="product_name_3",
    )
    assert resolve_collection(meta_3, input_collections) == "collection_3"

    # Wildcard name is NOT supported
    input_wildcard_name: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="*", product_type="product_type_4", collection_name="collection_4"),
    ]
    meta_4 = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="product_type_4",
        output_product_id="any_name",
    )
    with pytest.raises(ValueError):
        resolve_collection(meta_4, input_wildcard_name)

    # Priority: Exact type should be picked even if a wildcard match exists earlier for the same name
    priority_collections: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="exact_name", product_type="*", collection_name="wildcard_type"),
        FlowGeneratedProduct(name="exact_name", product_type="exact_type", collection_name="exact_match"),
    ]
    meta_priority = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="exact_type",
        output_product_id="exact_name",
    )
    assert resolve_collection(meta_priority, priority_collections) == "exact_match"

    # Protection: product_type="*" requires collection_name to be specified
    invalid_wildcard: list[FlowGeneratedProduct] = [
        FlowGeneratedProduct(name="product_name", product_type="*", collection_name=None),
    ]
    meta_wildcard = DprProcessedItemMetadata(
        stac_item=mocker.Mock(),
        product_type="any_type",
        output_product_id="product_name",
    )
    with pytest.raises(RuntimeError, match=r"cannot be '\*' if the collection name is not specified"):
        resolve_collection(meta_wildcard, invalid_wildcard)


async def setup_worklow_test_env(env_vars: dict[str, str] | None = None):
    """Set up secret blocks needed for correct execution of workflows in Prefect"""
    # Environment variables for all users. For these test we don't need specific values
    # so it creates an empty secret. See test_prefect_utils.py for a real case example.
    # Use an empty dictionary if input_dict is None
    # Default arguments are evaluated once when the function is defined, not each
    # time the function is called. If env_vars = {} would have been used and modify env_vars in one call,
    # this modified dictionary would persists for subsequent calls, which can lead to bugs.
    # Using env_vars = None and creating a new empty dictionary inside this function avoids this issue.
    env_vars = env_vars if env_vars is not None else {}
    # Serialize dictionary to a JSON string and wrap it in SecretStr
    secret_value = SecretStr(json.dumps(env_vars))

    # Remove the existing blocks, if any
    user_block_name = prefect_utils.format_env_user(prefect_utils.BLOCK_NAME_ENV_USER, OWNER_ID)
    with suppress(ValueError):
        await Secret.delete(prefect_utils.BLOCK_NAME_ENV_GLOBAL)
    with suppress(ValueError):
        await Secret.delete(user_block_name)

    await Secret(
        value=secret_value,
    ).save(  # type: ignore[arg-type]
        prefect_utils.BLOCK_NAME_ENV_GLOBAL,
        overwrite=True,
    )

    # Create prefect block for current user
    await Secret(
        value={  # type: ignore[arg-type]
            "RSPY_WEBSITE": MOCKED_RSPY_WEBSITE,
            "RSPY_APIKEY": RSPY_APIKEY,
            "S3_ACCESSKEY": S3_ACCESSKEY,
            "S3_SECRETKEY": S3_SECRETKEY,
            "S3_REGION": S3_REGION,
            "S3_ENDPOINT": S3_ENDPOINT,
        },
    ).save(user_block_name, overwrite=True)


def test_flow_input_product_items():
    """Test that the items method of FlowInputProduct returns the correct items."""
    product = FlowInputProduct(
        name="input1",
        cadip_session="session123",
        collection_name="collectionA",
    )

    items = dict(product.items())

    assert items["name"] == "input1"
    assert items["cadip_session"] == "session123"
    assert items["collection_name"] == "collectionA"


def test_flow_generated_product_items_with_collection():
    """Test that the items() of FlowGeneratedProduct returns the correct items when collection_name is provided."""
    product = FlowGeneratedProduct(
        name="output1",
        product_type="TYPE_A",
        collection_name="collectionB",
    )

    items = dict(product.items())

    assert items["name"] == "output1"
    assert items["product_type"] == "TYPE_A"
    assert items["collection_name"] == "collectionB"


def test_flow_generated_product_items_without_collection():
    """Test that the items() of FlowGeneratedProduct returns the correct items when collection_name is not provided."""
    product = FlowGeneratedProduct(
        name="output2",
        product_type="TYPE_B",
    )

    items = dict(product.items())

    assert items["name"] == "output2"
    assert items["product_type"] == "TYPE_B"
    assert items["collection_name"] is None


def test_auxiliary_product_mapping_items():
    """Test that the items() of AuxiliaryProductMapping returns the correct items."""
    mapping = AuxiliaryProductMapping(
        product_type="*",
        collection_name="aux_collection",
    )

    items = dict(mapping.items())

    assert items["product_type"] == "*"
    assert items["collection_name"] == "aux_collection"
