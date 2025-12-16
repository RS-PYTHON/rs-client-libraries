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

"""Test rs-client-libraries osam functions"""


import getpass
import tempfile
from unittest.mock import AsyncMock

import anyio
import pytest
import responses
from starlette import status

from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_client.osam_client import OsamClient
from rs_client.rs_client import RsClient

DUMMY_HREF = "https://DUMMY_HREF"
OWNER_ID = getpass.getuser()


@pytest.fixture(name="osam_client")
def get_osam_client() -> DprClient:
    """Create an oam client"""
    client = RsClient(
        rs_server_href=DUMMY_HREF,
        rs_server_api_key="RS_SERVER_API_KEY",
        owner_id=OWNER_ID,
        logger=None,
    )
    return client.get_osam_client()


@responses.activate
async def test_get_credentials(osam_client: OsamClient):
    """Test the get credentials endpoint"""

    mocked_credentials = {
        "access_key": "ak_value",
        "secret_key": "sk_value",
        "endpoint": "endpoint_value",
        "region": "region_value",
    }

    # Mock response from osam service
    responses.add(
        method=responses.GET,
        url=f"{DUMMY_HREF}/storage/account/credentials",
        json=mocked_credentials,
        status=status.HTTP_200_OK,
    )

    assert (await osam_client.get_credentials()).model_dump() == mocked_credentials
