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
from rs_workflows.flow_utils import GeneratedProduct
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
    """Check resolve_collection works with a list of GeneratedProduct instances."""
    mocker.patch("rs_workflows.catalog_flow.get_run_logger")

    input_collections: list[GeneratedProduct | dict] = [
        GeneratedProduct(name="product_name_1", product_type="product_type_1", collection_name="collection_1"),
        {"name": "product_name_2", "product_type": "product_type_2", "collection_name": "collection_2"},
    ]

    assert resolve_collection("product_type_1", input_collections) == "collection_1"
    assert resolve_collection("product_type_2", input_collections) == "collection_2"

    # Unknown product type should raise
    with pytest.raises(ValueError):
        resolve_collection("tip_42", input_collections)


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
