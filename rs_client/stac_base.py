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
from functools import lru_cache, wraps
from typing import Any, Callable, Dict, Iterator, Optional, Union, cast

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
    StacBase class implementation.

    Attributes:

    """

    def __init__(  # pylint: disable=too-many-branches, too-many-arguments, too-many-positional-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None = None,
        owner_id: str | None = None,
        logger: logging.Logger | None = None,
        stac_href: str | None = None,  # Flag to enable pystac_client for specific subclasses
        headers: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        ignore_conformance: Optional[bool] = None,
        modifier: Callable[[Collection | Item | ItemCollection | dict[Any, Any]], None] | None = None,
        request_modifier: Optional[Callable[[Request], Union[Request, None]]] = None,
        stac_io: Optional[StacApiIO] = None,
        timeout: Optional[Timeout] = TIMEOUT,
    ):
        """StacBase class constructor."""
        # call RsClient init
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)

        # Initialize pystac_client.Client only if required (for CadipClient, AuxipClient, StacClient)
        if not stac_href:
            raise RuntimeError("No stac href provided")
        try:
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
        except APIError as e:
            self.logger.exception(f"An exception occured while creating the stac client: {e}")
            raise RuntimeError(
                "An exception occured while creating the stac client",
            ) from e

    ################################
    # Specific STAC implementation #
    ################################
    @handle_api_error
    def get_landing(self) -> dict:
        """Access the landing page"""

        return self.ps_client.to_dict()

    @handle_api_error
    def get_collections(self) -> Iterator[Collection]:
        """Retrieve a list of the available stac collections.

        It uses the ps_client function to retrieve all collections the user has permission to access.

        Return:
            Iterator[Union[Collection, CollectionClient]]: Collections in Catalog/API
        """

        # Get all available collections
        return self.ps_client.get_collections()

    @lru_cache()
    @handle_api_error
    def get_collection(self, collection_id: str) -> Union[Collection, CollectionClient, None]:
        """Get the requested collection"""

        collection = None
        try:
            collection = self.ps_client.get_collection(collection_id)
        except APIError as e:
            self.logger.exception(f"An error occurred while retrieving the collection: {e}")
        return collection

    @handle_api_error
    def get_items(self, collection_id: str) -> Iterator["Item"] | None:
        """Get all items from a specific collection."""

        # Retrieve the collection
        collection = self.ps_client.get_collection(collection_id)
        if collection:
            # Retrieve all items
            return collection.get_items()
        self.logger.error(f"Collection with ID '{collection_id}' not found.")
        return None

    @handle_api_error
    def get_item(self, collection_id: str, item_id: str) -> Item | None:
        """Get an item from a specific collection."""

        # Retrieve the collection
        collection = self.ps_client.get_collection(collection_id)
        if collection:
            item = collection.get_item(item_id)
            if not item:
                self.logger.error(f"Item with ID '{item_id}' not found in collection '{collection_id}'.")
        else:
            self.logger.error(f"Collection with ID '{collection_id}' not found.")
            return None
        return item

    @handle_api_error
    def get_collection_queryables(self, collection_id) -> Dict[str, Any]:
        """Get queryables for a collection."""

        return self.ps_client.get_merged_queryables([collection_id])

    def get_queryables(self) -> Dict[str, Any]:
        """Get terms available for use when writing filter expressions in /search endpoint for all collections."""

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
            return cast(Dict[str, Any], json_data)  # Explicitly cast to Dict[str, Any]
        except ValueError as e:
            raise RuntimeError(f"Invalid JSON response from {href_queryables}") from e

    @handle_api_error
    def search(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        **kwargs,
    ) -> ItemCollection | None:
        """Retrieve a list of items by calling the ps_client function"""
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
