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

"""OsamClient class implementation."""

import asyncio
import logging

from pydantic import BaseModel

from rs_client.rs_client import TIMEOUT, RsClient
from rs_common.utils import get_href_service


class BucketCredentials(BaseModel):
    """Bucket credentials, with the same field names as those returned by the osam service."""

    access_key: str
    secret_key: str
    endpoint: str
    region: str


class OsamClient(RsClient):
    """
    OsamClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    def __init__(
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        logger: logging.Logger | None = None,
    ):
        """
        Initializes an OsamClient instance.

        Args:
            rs_server_href (str | None): The URL of the RS-Server. Pass None for local mode.
            rs_server_api_key (str | None): API key for authentication.
            logger (logging.Logger | None, optional): Logger instance (default: None).
        """
        super().__init__(
            rs_server_href,
            rs_server_api_key,
            None,
            logger,
        )

    @property
    def href_service(self) -> str:
        """
        Return the RS-Server OSAM URL hostname.
        This URL can be overwritten using the RSPY_HOST_OSAM env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_OSAM")

    async def get_credentials(self, timeout: int = TIMEOUT) -> BucketCredentials:
        """Get user credentials from cloud provider."""
        response = await asyncio.to_thread(
            self.http_session.get,
            f"{self.href_service}/storage/account/credentials",
            timeout=timeout,
            **self.apikey_headers,
        )
        response.raise_for_status()
        return BucketCredentials.model_validate(response.json())
