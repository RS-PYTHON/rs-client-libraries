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

import pystac
from pystac import Collection, Item, Link, RelType
from pystac_client.collection_client import CollectionClient
from pystac_client.item_search import ItemSearch
from requests import Response

from rs_client.rs_client import TIMEOUT
from rs_client.stac.stac_base import StacBase
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

        kwargs["collections"] = [
            self.full_collection_id(kwargs["owner_id"], collection, "_") for collection in kwargs["collections"]
        ]  # type: ignore
        return super().search(**kwargs)  # type: ignore

    # end of STAC read opperations

    # STAC write opperations. These can't be done with pystac_client
    # - add_collection
    # - remove_collection
    # - add_item
    # - remove_item

    def add_collection(
        self,
        collection: Collection,
        add_public_license: bool = True,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
    ) -> Response:
        """Update the collection links, then post the collection into the catalog.

        Args:
            collection (Collection): STAC collection
            add_public_license (bool): If True, add a public domain license field and link.
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            JSONResponse (json): The response of the request.
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

        # Update the links
        self.ps_client.add_child(collection)

        # Restore the short collection_id at the root of the collection
        collection.id = short_collection_id

        # Check that the collection is compliant to STAC
        collection.validate_all()

        # Post the collection to the catalog
        return self.http_session.post(
            f"{self.href_service}/catalog/collections",
            json=collection.to_dict(),
            **self.apikey_headers,
            timeout=timeout,
        )

    def remove_collection(
        self,
        collection_id: str,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
    ) -> Response:
        """Remove/delete a collection from the catalog.

        Args:
            collection_id (str): The collection id.
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            JSONResponse: The response of the request.
        """
        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Remove the collection from the "child" links of the local catalog instance
        collection_link = f"{self.ps_client.self_href.rstrip('/')}/collections/{full_collection_id}"
        self.ps_client.links = [
            link
            for link in self.ps_client.links
            if not ((link.rel == pystac.RelType.CHILD) and (link.href == collection_link))
        ]

        # We need to clear the cache for this and parent "get_collection" methods
        # because their returned value must be updated.
        self.ps_client.get_collection.cache_clear()

        # Remove the collection from the server catalog
        return self.http_session.delete(
            f"{self.href_service}/catalog/collections/{full_collection_id}",
            **self.apikey_headers,
            timeout=timeout,
        )

    def add_item(  # type: ignore # pylint: disable=arguments-renamed
        self,
        collection_id: str,
        item: Item,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
    ) -> Response:
        """Update the item links, then post the item into the catalog.

        Args:
            collection_id (str): The collection id.
            item (Item): STAC item to update and post
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            JSONResponse: The response of the request.
        """
        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Check that the item is compliant to STAC
        item.validate()

        # Get the collection from the catalog
        collection = self.get_collection(collection_id, owner_id)

        # Update the item  contents
        collection.add_item(item)  # type: ignore

        # Post the item to the catalog
        return self.http_session.post(
            f"{self.href_service}/catalog/collections/{full_collection_id}/items",
            json=item.to_dict(),
            **self.apikey_headers,
            timeout=timeout,
        )

    def remove_item(  # type: ignore # pylint: disable=arguments-differ
        self,
        collection_id: str,
        item_id: str,
        owner_id: str | None = None,
        timeout: int = TIMEOUT,
    ) -> Response:
        """Remove/delete an item from a collection.

        Args:
            collection_id (str): The collection id.
            item_id (str): The item id.
            owner_id (str, optional): Collection owner ID. If missing, we use self.owner_id.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            JSONResponse: The response of the request.
        """
        # owner_id:collection_id
        full_collection_id = self.full_collection_id(owner_id, collection_id)

        # Remove the collection from the server catalog
        return self.http_session.delete(
            f"{self.href_service}/catalog/collections/{full_collection_id}/items/{item_id}",
            **self.apikey_headers,
            timeout=timeout,
        )

    # end of STAC write opperations
