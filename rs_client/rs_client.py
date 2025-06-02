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

"""RsClient class implementation."""

import logging
import os
import sys

import requests
from cachetools import TTLCache, cached

from rs_common import utils
from rs_common.logging import Logging
from rs_common.utils import AuthInfo

APIKEY_HEADER = "x-api-key"

# Timeout in seconds
TIMEOUT = 30

# API Key Manager URL used to get an API Key information.
# Works only in cluster mode. This endpoint is not exposed outside the cluster.
RSPY_UAC_CHECK_URL = os.getenv("RSPY_UAC_CHECK_URL", "")


class RsClient:  # pylint: disable=too-many-instance-attributes
    """
    Client for interacting with the RS-Server services:
    - rs-server-staging
    - rs-server-cadip
    - rs-server-auxip
    - rs-server-catalog

    This class provides methods to authenticate and interact with RS-Server,
    manage STAC collections, and handle API requests.

    Attributes:
        rs_server_href (str | None): RS-Server URL. Pass None for local mode.
        rs_server_api_key (str | None): API key for RS-Server authentication.
        rs_server_oauth2_cookie (str | None): OAuth2 session cookie read from
            the `RSPY_OAUTH2_COOKIE` environment variable.
        owner_id (str | None): Only used in catalog client, see description there
        logger (logging.Logger): Logger instance for logging messages.
        local_mode (bool): Indicates whether the client is running in local mode.
        apikey_headers (dict): API key headers for HTTP requests.
        http_session (requests.Session): HTTP session for handling requests.
    """

    def __init__(  # pylint: disable=too-many-branches, too-many-arguments, too-many-positional-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None = None,
        owner_id: str | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initializes an RsClient instance.

        Args:
            rs_server_href (str | None): The URL of the RS-Server. Pass None for local mode.
            rs_server_api_key (str | None, optional): API key for authentication (default: None).
            owner_id (str | None, optional): ID of the catalog owner (default: None).
            logger (logging.Logger | None, optional): Logger instance (default: None).

        Raises:
            RuntimeError: If neither an API key nor an OAuth2 cookie is provided for RS-Server authentication.
        """
        self.rs_server_href: str | None = rs_server_href
        self.rs_server_api_key: str | None = rs_server_api_key
        self.rs_server_oauth2_cookie: str | None = os.getenv("RSPY_OAUTH2_COOKIE")
        self.owner_id: str | None = owner_id or os.getenv("RSPY_HOST_USER")
        self.logger: logging.Logger = logger or Logging.default(__name__)

        # Remove trailing / character(s) from the URL
        if self.rs_server_href:
            self.rs_server_href = self.rs_server_href.strip().rstrip("/").strip()

        # We are in local mode if the URL is undefined.
        # Env vars are used instead to determine the different services URL.
        self.local_mode = not bool(self.rs_server_href)

        # We are in hybrid mode if not on local mode and the API Key Manager check URL is undefined.
        # NOTE: maybe later we could define this hybrid mode in a different way.
        self.hybrid_mode = (not self.local_mode) and (not RSPY_UAC_CHECK_URL)

        if (not self.local_mode) and (not self.rs_server_api_key) and (not self.rs_server_oauth2_cookie):
            raise RuntimeError("API key or OAuth2 cookie is mandatory for RS-Server authentication")

        # For HTTP request headers
        self.apikey_headers: dict = (
            {"headers": {APIKEY_HEADER: self.rs_server_api_key}} if self.rs_server_api_key else {}
        )

        # HTTP requests session with cookies
        self.http_session = requests.Session()
        if self.rs_server_oauth2_cookie:
            self.http_session.cookies.set("session", self.rs_server_oauth2_cookie)

    def log_and_raise(self, message: str, original: Exception):
        """
        Logs an error message and raises a RuntimeError.

        This method logs the provided error message using the class logger
        and raises a `RuntimeError`, preserving the original exception as the cause.

        Args:
            message (str): The error message to log.
            original (Exception): The original exception that caused the error.

        Raises:
            RuntimeError: The logged error message, with the original exception as the cause.
        """
        self.logger.exception(message)
        raise RuntimeError(message) from original

    def oauth2_security(self) -> AuthInfo:
        """
        Returns:
            Authentication information from the user keycloak account, associated to the authentication cookie.
        """

        # In local mode, we have no authentication, so return empty results
        if self.local_mode:
            return AuthInfo(user_login="", iam_roles=[], apikey_config={})

        # Call the endpoint to retrieve the user information
        response = self.http_session.get(f"{self.rs_server_href}/auth/me")
        if not response.ok:
            raise RuntimeError(f"OAuth2 status code {response.status_code}: {utils.read_response_error(response)}")

        # Decode the JSON response
        contents = response.json()
        return AuthInfo(
            user_login=contents["user_login"],
            iam_roles=contents["iam_roles"],
            apikey_config={},  # no API key config here
        )

    # The following variable is needed for the tests to pass
    apikey_security_cache: TTLCache = TTLCache(maxsize=sys.maxsize, ttl=120)

    @cached(cache=apikey_security_cache)
    def apikey_security(self) -> AuthInfo:
        """
        Check the api key validity. Cache an infinite (sys.maxsize) number of results for 120 seconds.

        Returns:
            Authentication information from the keycloak account, associated to the api key.
        """

        # In local mode, we have no API key, so return empty results
        if self.local_mode:
            return AuthInfo(user_login="", iam_roles=[], apikey_config={})

        # Request the API key manager, pass user-defined api key in http header
        self.logger.debug("Call the API key manager")
        response = self.http_session.get(RSPY_UAC_CHECK_URL, **self.apikey_headers, timeout=TIMEOUT)
        if not response.ok:
            raise RuntimeError(
                f"API key manager status code {response.status_code}: {utils.read_response_error(response)}",
            )

        # Read the api key info.
        # Note: for now, config is an empty dict.
        contents = response.json()
        return AuthInfo(
            user_login=contents["user_login"],
            iam_roles=contents["iam_roles"],
            apikey_config=contents["config"],
        )

    @property
    def oauth2_user_login(self) -> str:
        """Return the user login from the keycloak account, associated to the authentication cookie."""
        return self.oauth2_security().user_login

    @property
    def apikey_user_login(self) -> str:
        """Return the user login from the keycloak account, associated to the api key."""
        return self.apikey_security().user_login

    @property
    def oauth2_iam_roles(self) -> list[str]:
        """
        Return the IAM (Identity and Access Management) roles from the keycloak account,
        associated to the authentication cookie
        """
        return self.oauth2_security().iam_roles

    @property
    def apikey_iam_roles(self) -> list[str]:
        """
        Return the IAM (Identity and Access Management) roles from the keycloak account,
        associated to the api key.
        """
        return self.apikey_security().iam_roles

    @property
    def apikey_config(self) -> dict:
        """Return the config from the keycloak account, associated to the api key."""
        return self.apikey_security().apikey_config

    @property
    def href_service(self):
        """Implemented by child classes"""

    #############################
    # Get child class instances #
    #############################

    def get_auxip_client(self, **kwargs) -> "AuxipClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class AuxipClient, with the same attributes as this "self" instance.
        """
        from rs_client.stac.auxip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            AuxipClient,
        )

        return AuxipClient(self.rs_server_href, self.rs_server_api_key, self.logger, **kwargs)

    def get_cadip_client(self, **kwargs) -> "CadipClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class CadipClient, with the same attributes as this "self" instance.
        """
        from rs_client.stac.cadip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            CadipClient,
        )

        return CadipClient(self.rs_server_href, self.rs_server_api_key, self.logger, **kwargs)

    def get_catalog_client(self, **kwargs) -> "CatalogClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class CatalogClient, with the same attributes as this "self" instance.
        """
        from rs_client.stac.catalog_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            CatalogClient,
        )

        return CatalogClient(
            self.rs_server_href,
            self.rs_server_api_key,
            self.owner_id,
            self.logger,
            **kwargs,
        )

    def get_staging_client(self) -> "StagingClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class StagingClient, with the same attributes as this "self" instance.
        """
        from rs_client.ogcapi.staging_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            StagingClient,
        )

        return StagingClient(self.rs_server_href, self.rs_server_api_key, None, self.logger)

    def get_dpr_client(self) -> "DprClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class DprClient, with the same attributes as this "self" instance.
        """
        from rs_client.ogcapi.dpr_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            DprClient,
        )

        return DprClient(self.rs_server_href, self.rs_server_api_key, None, self.logger)
