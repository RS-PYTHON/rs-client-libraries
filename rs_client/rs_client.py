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

import getpass
import logging
import os
import re
import sys
from datetime import datetime
from functools import lru_cache
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import requests
from cachetools import TTLCache, cached
from pystac import CatalogType, Collection, Item, Link, RelType
from pystac.layout import HrefLayoutStrategy
from pystac_client import Client, CollectionSearch, Modifiable
from pystac_client.collection_client import CollectionClient
from pystac_client.item_search import (
    BBoxLike,
    DatetimeLike,
    FieldsLike,
    FilterLike,
    IDsLike,
    IntersectsLike,
    ItemSearch,
    QueryLike,
    SortbyLike,
)
from pystac_client.stac_api_io import StacApiIO, Timeout
from requests import Request, Response

from rs_common import utils
from rs_common.config import DATETIME_FORMAT, ECadipStation
from rs_common.logging import Logging
from rs_common.utils import AuthInfo

APIKEY_HEADER = "x-api-key"

# Timeout in seconds
TIMEOUT = 30


class RsClient:  # pylint: disable=too-many-instance-attributes
    """
    RsClient class implementation.

    Attributes:
        rs_server_href (str): RS-Server URL. In local mode, pass None.
        rs_server_api_key (str): API key for RS-Server authentication.
        rs_server_oauth2_cookie (str): session cookie that contains the OAuth2 authentication read from the
                                       RSPY_OAUTH2_COOKIE environment variable.
        owner_id (str): ID of the owner of the STAC catalog collections (no special characters allowoed).
                        By default, this is the user login from the keycloak account, associated to the API key.
                        Or, in local mode, this is the local system username.
                        Else, your API Key must give you the rights to read/write on this catalog owner.
                        This owner ID is also used in the RS-Client logging.
        logger (logging.Logger): Logging instance.
        local_mode (bool): Local mode or hybrid/cluster mode.
        apikey_headers (dict): API key in a dict, ready-to-use in HTTP request headers.
        http_session (Session): HTTP requests session with cookies.
    """

    def __init__(
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None = None,
        owner_id: str | None = None,
        logger: logging.Logger | None = None,
        stac_href: str = None,  # Flag to enable pystac_client for specific subclasses
        headers: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        ignore_conformance: Optional[bool] = None,
        modifier: Optional[Callable[[Client], None]] = None,
        request_modifier: Optional[Callable[[Request], Union[Request, None]]] = None,
        stac_io: Optional[StacApiIO] = None,
        timeout: Optional[Timeout] = TIMEOUT,
    ):
        """RsClient class constructor."""
        self.rs_server_href: str | None = rs_server_href
        self.rs_server_api_key: str | None = rs_server_api_key
        self.rs_server_oauth2_cookie: str | None = os.getenv("RSPY_OAUTH2_COOKIE")
        self.owner_id: str | None = owner_id or ""
        self.logger: logging.Logger = logger or Logging.default(__name__)

        # Remove trailing / character(s) from the URL
        if self.rs_server_href:
            self.rs_server_href = self.rs_server_href.strip().rstrip("/").strip()

        # We are in local mode if the URL is undefined.
        # Env vars are used instead to determine the different services URL.
        self.local_mode = not bool(self.rs_server_href)

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

        # Determine automatically the owner id
        if not self.owner_id:
            # In local mode, we use the local system username
            if self.local_mode:
                self.owner_id = getpass.getuser()

            # In hybrid/cluster mode, we retrieve the OAuth2 or API key login
            else:
                self.owner_id = self.apikey_user_login if self.rs_server_api_key else self.oauth2_user_login

        # Remove special characters
        self.owner_id = re.sub(r"[^a-zA-Z0-9]+", "", self.owner_id)

        if not self.owner_id:
            raise RuntimeError("The owner ID is empty or only contains special characters")

        self.logger.debug(f"Owner ID: {self.owner_id!r}")

        # Initialize pystac_client.Client only if required (for CadipClient, AuxipClient, StacClient)
        self.stac_client: Optional[Client] = None
        if stac_href:
            if stac_io is None:
                stac_io = StacApiIO(  # This is what is done in pystac_client/client.py::from_file
                    headers=headers,
                    parameters=parameters,
                    request_modifier=request_modifier,
                    timeout=timeout,
                )
            # Save the OAuth2 authentication cookie in the pystac client cookies
            if self.rs_server_oauth2_cookie:
                stac_io.session.cookies.set("session", self.rs_server_oauth2_cookie)
            self.stac_client = Client.open(
                stac_href,
                headers=self.apikey_headers,
                parameters=parameters,
                ignore_conformance=ignore_conformance,
                modifier=modifier,
                request_modifier=request_modifier,
                stac_io=stac_io,
                timeout=timeout,
            )

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

        # self.logger.warning(
        #     f"TODO: use {self.rs_server_href}/apikeymanager/auth/check_key instead, see: "
        #     "https://pforge-exchange2.astrium.eads.net/jira/browse/RSPY-257",
        # )
        # Does not work in hybrid mode for now because this URL is not exposed.
        check_url = os.environ["RSPY_UAC_CHECK_URL"]

        # Request the API key manager, pass user-defined api key in http header
        # check_url = f"{self.rs_server_href}/apikeymanager/auth/check_key"
        self.logger.debug("Call the API key manager")
        response = self.http_session.get(check_url, **self.apikey_headers, timeout=TIMEOUT)
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

    #############################
    # Get child class instances #
    #############################

    def get_auxip_client(self, **kwargs) -> "AuxipClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class AuxipClient, with the same attributes as this "self" instance.
        """
        from rs_client.auxip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            AuxipClient,
        )

        return AuxipClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, self.logger, **kwargs)

    def get_cadip_client(self, station: ECadipStation, **kwargs) -> "CadipClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class CadipClient, with the same attributes as this "self" instance.

        Args:
            station (ECadipStation): Cadip station
        """
        from rs_client.cadip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            CadipClient,
        )

        return CadipClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, station, self.logger, **kwargs)

    def get_stac_client(self, **kwargs) -> "StacClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class StacClient, with the same attributes as this "self" instance.
        """
        from rs_client.stac_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            StacClient,
        )

        return StacClient(
            self.rs_server_href,
            self.rs_server_api_key,
            self.owner_id,
            self.logger,
            **kwargs,
        )

    def get_staging_client(self) -> "StagingClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class AuxipClient, with the same attributes as this "self" instance.
        """
        from rs_client.staging_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            StagingClient,
        )

        return StagingClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, self.logger)

    def get_collection_id(self, collection_id: str):
        """
        Return the full collection name as: <owner_id>:<collection_id>

        Args:
            owner_id (str): Collection owner ID. If missing, we use self.owner_id.
            collection_id (str): Collection name
        """
        return collection_id

    ############################
    # Call RS-Server endpoints #
    ############################

    def search_stations(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        start_date: datetime,
        stop_date: datetime,
        limit: Union[int, None] = None,
        sortby: Union[str, None] = None,
        timeout: int = TIMEOUT,
    ) -> list:
        """Retrieve a list of items from the specified endpoint.

        This function queries the specified endpoint to retrieve a list of items available in the
        station (CADIP, ADGS, LTA ...) that were collected by the satellite within the provided time range,
        starting from 'start_date' up to 'stop_date' (inclusive).

        Args:
            start_date (datetime): The start date of the time range.
            stop_date (datetime): The stop date of the time range.
            timeout (int): The timeout duration for the HTTP request.
            limit (int, optional): The maximum number of results to return. Defaults to None.
            sortby (str, optional): The attribute to sort the results by. Defaults to None.

        Returns:
            items (list): The list of items (features) available at the station within the specified time range.

        Raises:
            RuntimeError: if the endpoint can't be reached

        Notes:
            - This function queries the specified endpoint with a time range to retrieve information about
            available files.
            - It constructs a payload with the start and stop dates in ISO 8601 format and sends a GET
            request to the endpoint.
            - The response is expected to be a STAC Compatible formatted JSONResponse, containing information about
             available items.
            - The function converts a STAC FeatureCollection to a Python list.
        """

        payload = {
            "datetime": start_date.strftime(DATETIME_FORMAT)
            + "/"  # 2014-01-01T12:00:00Z/2023-12-30T12:00:00Z",
            + stop_date.strftime(DATETIME_FORMAT),
        }

        if limit:
            payload["limit"] = str(limit)
        if sortby:
            payload["sortby"] = str(sortby)
        try:
            response = self.http_session.get(
                self.href_search,  # pylint: disable=no-member # ("self" is AuxipClient or CadipClient)
                params=payload,
                timeout=timeout,
                **self.apikey_headers,
            )
            # print(f"response.json() = {response.json()}")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            self.logger.exception(f"Could not get the response from the station search endpoint: {e}")
            raise RuntimeError("Could not get the response from the station search endpoint") from e

        try:
            if response.ok:
                return response.json()
            self.logger.error(f"Error: {response.status_code} : {response.json()}")
        except KeyError as e:
            raise RuntimeError("Wrong format of search endpoint answer") from e

        return []

    ################################
    # Specific STAC implementation #
    ################################

    @lru_cache()
    def get_collection(self, collection_id: str) -> Union[Collection, CollectionClient]:
        """Get the requested collection as <owner_id>:<collection_id>"""
        return self.stac_client.get_collection(self, collection_id)

    def search_inside_collection(  # pylint: disable=too-many-arguments
        self,
        *,
        collection_id: str,
        method: Optional[str] = "POST",
        max_items: Optional[int] = None,
        limit: Optional[int] = None,
        ids: Optional[IDsLike] = None,
        bbox: Optional[BBoxLike] = None,
        intersects: Optional[IntersectsLike] = None,
        timestamp: Optional[DatetimeLike] = None,
        query: Optional[QueryLike] = None,
        stac_filter: Optional[FilterLike] = None,
        filter_lang: Optional[str] = None,
        sortby: Optional[SortbyLike] = None,
        fields: Optional[FieldsLike] = None,
    ) -> ItemSearch:
        """Search items inside a specific collection."""

        return self.stac_client.search(
            method=method,
            max_items=max_items,
            limit=limit,
            ids=ids,
            collections=[collection_id],
            bbox=bbox,
            intersects=intersects,
            datetime=timestamp,
            query=query,
            filter=stac_filter,
            filter_lang=filter_lang,
            sortby=sortby,
            fields=fields,
        )

    def get_item(  # pylint: disable=too-many-arguments
        self,
        collection_id: str,
        item_id: str,
    ):
        """Get an item from a specific collection."""

        # Retrieve the collection
        collection = self.stac_client.get_collection(collection_id)
        if collection:
            # Retrieve the specific item from the collection
            item = collection.get_item(item_id)
            if not item:
                print(f"Item with ID '{item_id}' not found in collection '{collection_id}'.")
        else:
            print(f"Collection with ID '{collection_id}' not found.")
        return item

    def get_collections(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
    ) -> Iterator[Collection]:
        """Retrieve a list of the available stac collections.

        This function queries the specified endpoint to retrieve all the collections the user is allowed to use.

        Return:
            Iterator[Union[Collection, CollectionClient]]: Collections in Catalog/API
        """

        # Get all available collections
        return self.stac_client.get_collections()

    def get_landing(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        timeout: int = TIMEOUT,
    ) -> list:
        """Retrieve a list of items from the specified endpoint.

        This function queries the specified endpoint to retrieve all the collectionthe user is allowed to use.

        Args:
            timeout (int): The timeout duration for the HTTP request.


        Returns:
            collections (list): The list of collections available at the station for the usage.

        Raises:
            RuntimeError: if the endpoint can't be reached

        Notes:
            - The response is expected to be a STAC Compatible formatted JSONResponse, containing information about
             available items.
            - The function converts a STAC FeatureCollection to a Python list.
        """

        try:
            response = self.http_session.get(
                self.href_landing,  # pylint: disable=no-member # ("self" is AuxipClient or CadipClient)
                timeout=timeout,
                **self.apikey_headers,
            )

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            self.logger.exception(f"Could not get the response from the endpoint {self.href_all_collections}: {e}")
            raise RuntimeError(
                "Could not get the response from the station search " f"endpoint {self.href_all_collections}",
            ) from e

        try:
            if response.ok:
                # return [Collection(**collection) for collection in response.json()["collections"]]
                return response.json()
            self.logger.error(f"Error: {response.status_code} : {response.json()}")
        except KeyError as e:
            raise RuntimeError("Wrong format of search endpoint answer") from e

        return None

    def get_queryables(self, collection_id) -> Dict[str, Any]:
        """Get quryables of a collection."""
        return self.stac_client.get_merged_queryables([collection_id])

    #################################################
    # Properties to be implemented by child classes #
    #################################################
    @property
    def get_collection_name(self):
        """Get name of the colleciton from the subclass."""

    @property
    def href_search(self):
        """Implemented by AuxipClient and CadipClient."""

    @property
    def href_landing(self):
        """Implemented by AuxipClient and CadipClient."""

    @property
    def href_all_collections(self):
        """Implemented by AuxipClient and CadipClient."""

    @property
    def href_collection(self, collection_id):
        """Implemented by AuxipClient and CadipClient."""

    @property
    def href_collection_all_items(self, collection_id):
        """Implemented by AuxipClient and CadipClient."""

    @property
    def href_collection_item(self, collection_id, item_id):
        """Implemented by AuxipClient and CadipClient."""

    @property
    def href_collection_queryables(self, collection_id):
        """Implemented by AuxipClient and CadipClient."""

    @property
    def href_queryables(self):
        """Implemented by AuxipClient and CadipClient."""
