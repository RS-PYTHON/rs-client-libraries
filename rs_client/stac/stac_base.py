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

"""StacBase class implementation."""

import logging
from collections.abc import Callable, Iterator
from functools import lru_cache, wraps
from typing import Any, cast

import requests
from pystac import Collection, Item, ItemCollection
from pystac_client import Client
from pystac_client.collection_client import CollectionClient
from pystac_client.exceptions import APIError
from pystac_client.stac_api_io import StacApiIO, Timeout
from requests import Request

from rs_client.rs_client import APIKEY_HEADER, TIMEOUT, RsClient


def handle_api_error(func):
    """
    Decorator to handle APIError exceptions in methods that interact with pystac-client.

    This decorator wraps methods of the StacBase class that call `self.ps_client`,
    catching `APIError` exceptions and logging them using the instance's `logger`.

    If the `logger` attribute is not found or is `None`, it falls back to printing the error.

    Args:
        func (Callable): The method to be wrapped.

    Returns:
        Callable: The wrapped method that catches and logs `APIError` exceptions,
        then raises a RuntimeError.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except APIError as e:
            error_message = f"Pystac client returned exception: {e}"
            if hasattr(self, "logger") and self.logger:
                self.logger.exception(error_message)
            else:
                print(error_message)  # Fallback logging
            raise RuntimeError(error_message) from e

    return wrapper


class StacBase(RsClient):
    """
    Base class for interacting with a STAC (SpatioTemporal Asset Catalog) API using pystac-client.

    This class provides methods to retrieve STAC collections, items, queryables, and perform searches.
    """

    @handle_api_error
    def __init__(  # pylint: disable=too-many-branches, too-many-arguments, too-many-positional-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None = None,
        owner_id: str | None = None,
        logger: logging.Logger | None = None,
        stac_href: str | None = None,  # Flag to enable pystac_client for specific subclasses
        headers: dict[str, str] | None = None,
        parameters: dict[str, Any] | None = None,
        ignore_conformance: bool | None = None,
        modifier: Callable[[Collection | Item | ItemCollection | dict[Any, Any]], None] | None = None,
        request_modifier: Callable[[Request], Request | None] | None = None,
        stac_io: StacApiIO | None = None,
        timeout: Timeout | None = TIMEOUT,
    ):
        """
        Initialize the StacBase instance.

        Args:
            rs_server_href (str | None): URL of the RS server.
            rs_server_api_key (str | None, optional): API key for authentication.
            owner_id (str | None, optional): Owner identifier.
            logger (logging.Logger | None, optional): Logger instance.
            stac_href (str | None): STAC API URL.
            headers (Optional[Dict[str, str]], optional): HTTP headers.
            parameters (Optional[Dict[str, Any]], optional): Additional query parameters.
            ignore_conformance (Optional[bool], optional): Whether to ignore conformance.
            modifier (Callable, optional): Function to modify collection, item, or item collection.
            request_modifier (Optional[Callable[[Request], Union[Request, None]]], optional):
                                                                Function to modify requests.
            stac_io (Optional[StacApiIO], optional): Custom STAC API I/O handler.
            timeout (Optional[Timeout], optional): Request timeout.
        """
        # call RsClient init
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)

        # Initialize pystac_client.Client only if required (for CadipClient, AuxipClient, StacClient)
        if not stac_href:
            raise RuntimeError("No stac href provided")
        # pystac_client may throw APIError exception this is handled by the decorator handle_api_error
        self.stac_href = stac_href
        if rs_server_api_key:
            if headers is None:
                headers = {}
            headers[APIKEY_HEADER] = rs_server_api_key
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
        self.ps_client = Client.open(
            stac_href,
            headers=headers,
            parameters=parameters,
            ignore_conformance=ignore_conformance,
            modifier=modifier,
            request_modifier=request_modifier,
            stac_io=stac_io,
            timeout=timeout,
        )

    ################################
    # Specific STAC implementation #
    ################################
    @handle_api_error
    def get_landing(self) -> dict:
        """
        Retrieve the STAC API landing page.

        Returns:
            dict: The landing page response.

        Raises:
            RuntimeError: If an API error occurs.
        """

        return self.ps_client.to_dict()

    @handle_api_error
    def get_collections(self) -> Iterator[Collection]:
        """
        Retrieve available STAC collections the user has permission to access.

        Returns:
            Iterator[Collection]: An iterator over available collections.

        Raises:
            RuntimeError: If an API error occurs.
        """

        # Get all the available collections
        return self.ps_client.get_collections()

    @lru_cache
    @handle_api_error
    def get_collection(self, collection_id: str) -> Collection | CollectionClient:
        """
        Retrieve a specific STAC collection by ID.

        Args:
            collection_id (str): The ID of the collection.

        Returns:
            Union[Collection, CollectionClient]: The requested collection.

        Raises:
            RuntimeError: If an API error occurs.
        """
        return self.ps_client.get_collection(collection_id)

    @handle_api_error
    def get_items(self, collection_id: str, items_ids: list[str] | None = None) -> Iterator["Item"]:
        """
        Retrieve all items or specific items from a collection.

        Args:
            collection_id (str): The ID of the collection.
            items_ids (Union[str, None], optional): Specific item ID(s) to retrieve.

        Returns:
            Iterator[Item]: An iterator over retrieved items.
        Raises:
            RuntimeError: If an API error occurs.
        """

        # Retrieve the collection
        collection = self.ps_client.get_collection(collection_id)
        # Retrieve a list of items
        if items_ids:
            self.logger.info(f"Retrieving specific items from collection '{collection_id}'.")
            return collection.get_items(*items_ids)
        # Retrieve all items
        self.logger.info(f"Retrieving all items from collection '{collection_id}'.")
        return collection.get_items()

    @handle_api_error
    def get_item(self, collection_id: str, item_id: str) -> Item | None:
        """
        Retrieve a specific item from a collection.

        Args:
            collection_id (str): The collection ID.
            item_id (str): The item ID.

        Returns:
            Item | None: The retrieved item or None if not found.

        Raises:
            RuntimeError: If an API error occurs.
        """

        # Retrieve the collection
        collection = self.ps_client.get_collection(collection_id)
        item = collection.get_item(item_id)
        if not item:
            self.logger.error(f"Item with ID '{item_id}' not found in collection '{collection_id}'.")
        return item

    @handle_api_error
    def get_collection_queryables(self, collection_id) -> dict[str, Any]:
        """
        Retrieve queryable fields for a specific collection.

        Args:
            collection_id (str): The collection ID.

        Returns:
            Dict[str, Any]: Dictionary of queryable fields.

        Raises:
            RuntimeError: If an API error occurs.
        """

        return self.ps_client.get_merged_queryables([collection_id])

    def get_queryables(self) -> dict[str, Any]:
        """
        Retrieve queryable fields for all collections in the STAC API. These are the available terms for
        usage when writing filter expressions in /search endpoint for all the collections
        NOTE: the pystac-client library doesn't have a function for this action, so the direct call of
        the endpoint is needed

        Returns:
            Dict[str, Any]: Dictionary of queryable fields.

        Raises:
            RuntimeError: If an exception occurs from the request level.
        """
        try:
            href_queryables = self.stac_href + "queryables"
            response = self.http_session.get(
                href_queryables,
                **self.apikey_headers,
                timeout=TIMEOUT,
            )
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            self.logger.exception(f"Could not get the response from the endpoint {href_queryables}: {e}")
            raise RuntimeError(
                f"Could not get the response from the endpoint {href_queryables}",
            ) from e
        if not response.ok:
            raise RuntimeError(f"Could not get queryables from {href_queryables}")
        try:
            json_data = response.json()
            return cast(dict[str, Any], json_data)  # Explicitly cast to Dict[str, Any]
        except ValueError as e:
            raise RuntimeError(f"Invalid JSON response from {href_queryables}") from e

    @handle_api_error
    def search(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        **kwargs,
    ) -> ItemCollection | None:
        """
        Perform a STAC search using query parameters.

        Returns:
            ItemCollection | None: Retrieved item collection or None if not found.

        Raises:
            RuntimeError: If an API error occurs.
        """
        kwargs.pop("owner_id", None)
        kwargs["datetime"] = kwargs.pop("timestamp", None)
        kwargs["filter"] = kwargs.pop("stac_filter", None)

        try:
            items_search = self.ps_client.search(**kwargs)

            return items_search.item_collection()
        except NotImplementedError:
            self.logger.exception(
                "The API does not conform to the STAC API Item Search spec"
                "or does not have a link with a 'rel' type of 'search' ",
            )
        return None
