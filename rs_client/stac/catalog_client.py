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

"""Implement the class CatalogClient that inherits from pystact_client Client."""

from __future__ import annotations

import getpass
import logging
import re
from collections.abc import Iterator

from pystac import Collection, Item, Link, RelType
from pystac_client import Client
from pystac_client.collection_client import CollectionClient
from pystac_client.item_search import ItemSearch
from requests import HTTPError, Response

from rs_client.rs_client import TIMEOUT
from rs_client.stac.stac_base import StacBase
from rs_common import utils
from rs_common.utils import get_href_service


class CatalogClient(StacBase):  # type: ignore # pylint: disable=too-many-ancestors
    """CatalogClient inherits from both rs_client.RsClient and pystac_client.Client. The goal of this class is to
    allow an user to use RS-Server services more easily than calling REST endpoints directly.

    Attributes:
        owner_id (str): The owner of the STAC catalog collections (no special characters allowed).
            If not set, we try to read it from the RSPY_HOST_USER environment variable. If still not set:
            - In local mode, it takes the system username.
            - In cluster mode, it is deduced from the API key or OAuth2 login = your keycloak username.
            - In hybrid mode, we raise an Exception.
            If owner_id is different than your keycloak username, then make sure that your keycloak account has
            the rights to read/write on this catalog owner.
            owner_id is also used in the RS-Client logging.
    """

    ##################
    # Initialisation #
    ##################

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str | None,
        logger: logging.Logger | None = None,
        **kwargs,
    ):
        """CatalogClient class constructor.

        Args:
            rs_server_href (str | None): The URL of the RS-Server. Pass None for local mode.
            rs_server_api_key (str | None, optional): API key for authentication (default: None).
            owner_id (str | None, optional): ID of the catalog owner (default: None).
            logger (logging.Logger | None, optional): Logger instance (default: None).

        Raises:
            RuntimeError: If neither an API key nor an OAuth2 cookie is provided for RS-Server authentication.
            RuntimeError: If the computed owner ID is empty or contains only special characters.
        """
        super().__init__(
            rs_server_href,
            rs_server_api_key,
            owner_id,
            logger,
            get_href_service(rs_server_href, "RSPY_HOST_CATALOG") + "/catalog/",
            **kwargs,
        )

        # Determine automatically the owner id
        if not self.owner_id:
            # In local mode, we use the local system username
            if self.local_mode:
                self.owner_id = getpass.getuser()

            # In hybrid mode, the API Key Manager check URL is not accessible and there is no OAuth2
            # so the owner id must be set explicitly by the user.
            elif self.hybrid_mode:
                raise RuntimeError(
                    "In hybrid mode, the owner_id must be set explicitly by parameter or environment variable",
                )

            # In cluster mode, we retrieve the OAuth2 or API key login
            else:
                self.owner_id = self.apikey_user_login if self.rs_server_api_key else self.oauth2_user_login

        # Remove special characters
        self.owner_id = re.sub(r"[^a-zA-Z0-9]+", "", self.owner_id)

        if not self.owner_id:
            raise RuntimeError("The owner ID is empty or only contains special characters")

        self.logger.debug(f"Owner ID: {self.owner_id!r}")

    ##############
    # Properties #
    ##############

    @property
    def href_service(self) -> str:
        """
        Return the RS-Server Catalog URL hostname.
        This URL can be overwritten using the RSPY_HOST_CATALOG env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """

        return get_href_service(self.rs_server_href, "RSPY_HOST_CATALOG")

    def full_collection_id(self, owner_id: str | None, collection_id: str, concat_char: str | None = None) -> str:
        """
        Generates a full collection identifier by concatenating the owner ID and collection ID.

        This function constructs a full collection ID by combining the provided `owner_id` (or a
        default owner ID from `self.owner_id`) with `collection_id` using a specified separator.

        Parameters:
            owner_id (str | None): The owner identifier. If `None`, it defaults to `self.owner_id`.
            collection_id (str): The collection identifier that must always be provided.
            concat_char (str | None, optional): The character used to concatenate `owner_id`
                                                and `collection_id`. Defaults to ":".

        Returns:
            str: A string representing the full collection ID, formatted as:
                `"owner_id:collection_id"` by default or using the specified `concat_char`, that may
                be `_`.

        Raises:
            - **AttributeError**: If `self.owner_id` is not set and `owner_id` is `None`,
                causing an attempt to concatenate a `NoneType` with a string.

        Notes:
            - This function is useful in scenarios where collections are stored with unique
                identifiers that require owner prefixes for proper scoping.
        """

        if not concat_char:
            concat_char = ":"
        return f"{owner_id or self.owner_id}{concat_char}{collection_id}"

    #####################
    # Utility functions #
    #####################

    def raise_for_status(self, response: Response, ignore: list[int] | None = None):
        """
        Raises :class:`HTTPError`, if one occurred.

        Args:
            response: HTTP response
            ignore: ignore error its status code is in this list
        """
        if ignore and (response.status_code in ignore):
            return
        try:
            response.raise_for_status()
        except HTTPError as error:
            message = f"{error.args[0]}\nDetail: {utils.read_response_error(response)}"
            raise HTTPError(message, response=response)  # pylint: disable=raise-missing-from

    def clear_collection_cache(self):
        """Clear the lru_caches because they still contains the old collection."""
        StacBase.get_collection.cache_clear()  # pylint: disable=no-member
        Client.get_collection.cache_clear()  # pylint: disable=no-member

    def process_response(self, response: Response, raise_for_status: bool, ignore: list[int] | None = None) -> Response:
        """
        Process the HTTP response.

        Args:
            response: The HTTP response to process.
            raise_for_status: If True, raise an error for HTTP errors.
            ignore: A list of status codes to ignore.

        Returns:
            The processed HTTP response.
        """
        if raise_for_status:
            self.raise_for_status(response, ignore)
        self.clear_collection_cache()
        return response

    ################################
    # Specific STAC implementation #
    ################################

    # STAC read opperations. These can be done with pystac_client (by calling super StacBase functions)

    def get_collection(  # type: ignore
        self,
        collection_id: str,
        owner_id: str | None = None,
    ) -> Collection | CollectionClient:
        """Get the requested collection"""
        return super().get_collection(self.full_collection_id(owner_id, collection_id, ":"))

    def get_items(
        self,
        collection_id: str,
        items_ids: list[str] | None = None,
        owner_id: str | None = None,
    ) -> Iterator[Item]:
        """Get all items from a specific collection."""
        return super().get_items(self.full_collection_id(owner_id, collection_id, ":"), items_ids)

    def get_item(self, collection_id: str, item_id: str, owner_id: str | None = None):
        """Get an item from a specific collection."""
        return super().get_item(self.full_collection_id(owner_id, collection_id, ":"), item_id)

    def search(  # type: ignore # pylint: disable=too-many-arguments, arguments-differ
        self,
        **kwargs,
    ) -> ItemSearch | None:
        """Search items inside a specific collection."""

        if "collections" in kwargs:
            kwargs["collections"] = [
                self.full_collection_id(kwargs.get("owner_id"), collection, "_") for collection in kwargs["collections"]
            ]  # type: ignore
        return super().search(**kwargs)  # type: ignore

    # end of STAC read opperations

    # STAC write opperations. These can't be done with pystac_client
    # - add_collection
    # - remove_collection
    # - update_collection
    # - add_item
    # - remove_item
    # - update_item

    def add_collection(
        self,
        collection: Collection,
        add_public_license: bool = True,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
        raise_for_status: bool = True,
    ) -> Response:
        """Update the collection links, then post the collection into the catalog.

        Args:
            collection (Collection): STAC collection
            add_public_license (bool): If True, add a public domain license field and link.
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.
            raise_for_status (bool): If True, raise HTTPError in case of server error.

        Returns:
            JSONResponse (json): The response of the request.

        Raises:
            HTTPError in case of server error.
        """

        full_owner_id = owner_id or self.owner_id

        # Use owner_id:collection_id instead of just the collection ID, before adding the links,
        # so the links contain the full owner_id:collection_id
        short_collection_id = collection.id
        full_collection_id = self.full_collection_id(owner_id, short_collection_id)
        collection.id = full_collection_id

        # Default description
        if not collection.description:
            collection.description = f"This is the collection {short_collection_id} from user {full_owner_id}."

        # Add the owner_id as an extra field
        collection.extra_fields["owner"] = full_owner_id

        # Add public domain license
        if add_public_license:
            collection.license = "public-domain"
            collection.add_link(
                Link(
                    rel=RelType.LICENSE,
                    target="https://creativecommons.org/licenses/publicdomain/",
                    title="public-domain",
                ),
            )

        # Restore the short collection_id at the root of the collection
        collection.id = short_collection_id

        # Check that the collection is compliant to STAC
        collection.validate_all()

        # Post the collection to the catalog
        response = self.http_session.post(
            f"{self.href_service}/catalog/collections",
            json=collection.to_dict(),
            **self.apikey_headers,
            timeout=timeout,
        )
        return self.process_response(response, raise_for_status)

    def remove_collection(
        self,
        collection_id: str,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
        raise_for_status: bool = True,
    ) -> Response:
        """Remove/delete a collection from the catalog.

        Args:
            collection_id (str): The collection id.
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.
            raise_for_status (bool): If True, raise HTTPError in case of server error.

        Returns:
            JSONResponse (json): The response of the request.

        Raises:
            HTTPError in case of server error.
        """
        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Remove the collection from the server catalog
        response = self.http_session.delete(
            f"{self.href_service}/catalog/collections/{full_collection_id}",
            **self.apikey_headers,
            timeout=timeout,
        )
        return self.process_response(response, raise_for_status, ignore=[404])

    def update_collection(
        self,
        collection: Collection | CollectionClient | dict,
        timeout: int = TIMEOUT,
        raise_for_status: bool = True,
    ) -> Response:
        """Put/update a collection in the catalog.

        Args:
            collection: The collection contents.
            timeout (int): The timeout duration for the HTTP request.
            raise_for_status (bool): If True, raise HTTPError in case of server error.

        Returns:
            JSONResponse (json): The response of the request.

        Raises:
            HTTPError in case of server error.
        """

        # Convert to dict
        col_dict: dict = collection if isinstance(collection, dict) else collection.to_dict()

        # Get the collection owner_id and remove it from the collection id
        # which is like <owner_id>_<short_collection_id>
        owner_id = col_dict["owner"]
        collection_id = col_dict["id"].removeprefix(f"{owner_id}_")
        col_dict["id"] = collection_id

        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Update the collection in the server catalog
        response = self.http_session.put(
            f"{self.href_service}/catalog/collections/{full_collection_id}",
            json=col_dict,
            **self.apikey_headers,
            timeout=timeout,
        )
        return self.process_response(response, raise_for_status)

    def add_item(  # type: ignore # pylint: disable=arguments-renamed
        self,
        collection_id: str,
        item: Item,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
        raise_for_status: bool = True,
    ) -> Response:
        """Update the item links, then post the item into the catalog.

        Args:
            collection_id (str): The collection id.
            item (Item): STAC item to update and post
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.
            raise_for_status (bool): If True, raise HTTPError in case of server error.

        Returns:
            JSONResponse (json): The response of the request.

        Raises:
            HTTPError in case of server error.
        """
        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Check that the item is compliant to STAC
        item.validate()

        # Post the item to the catalog
        response = self.http_session.post(
            f"{self.href_service}/catalog/collections/{full_collection_id}/items",
            json=item.to_dict(),
            **self.apikey_headers,
            timeout=timeout,
        )
        return self.process_response(response, raise_for_status)

    def remove_item(  # type: ignore # pylint: disable=arguments-differ
        self,
        collection_id: str,
        item_id: str,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
        raise_for_status: bool = True,
    ) -> Response:
        """Remove/delete an item from a collection.

        Args:
            collection_id (str): The collection id.
            item_id (str): The item id.
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.
            raise_for_status (bool): If True, raise HTTPError in case of server error.

        Returns:
            JSONResponse (json): The response of the request.

        Raises:
            HTTPError in case of server error.
        """
        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Remove the collection from the server catalog
        response = self.http_session.delete(
            f"{self.href_service}/catalog/collections/{full_collection_id}/items/{item_id}",
            **self.apikey_headers,
            timeout=timeout,
        )
        return self.process_response(response, raise_for_status)

    def update_item(
        self,
        item: Item | dict,
        timeout: int = TIMEOUT,
        raise_for_status: bool = True,
    ) -> Response:
        """Put/update an item in the catalog.

        Args:
            item: The item contents.
            timeout (int): The timeout duration for the HTTP request.
            raise_for_status (bool): If True, raise HTTPError in case of server error.

        Returns:
            JSONResponse (json): The response of the request.

        Raises:
            HTTPError in case of server error.
        """

        # Convert to dict
        item_dict: dict = item if isinstance(item, dict) else item.to_dict()

        # Get the collection owner_id and remove it from the collection id
        # which is like <owner_id>_<short_collection_id>
        owner_id = item_dict["properties"]["owner"]
        collection_id = item_dict["collection"].removeprefix(f"{owner_id}_")
        item_dict["collection"] = collection_id

        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Update the item in the server catalog
        response = self.http_session.put(
            f"{self.href_service}/catalog/collections/{full_collection_id}/items/{item_dict['id']}",
            json=item_dict,
            **self.apikey_headers,
            timeout=timeout,
        )
        return self.process_response(response, raise_for_status)

    # end of STAC write opperations
