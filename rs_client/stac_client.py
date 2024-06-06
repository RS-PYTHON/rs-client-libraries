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

"""Implement the class StacCLient that inherits from pystact_client Client."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Union

import pystac
import requests
from pystac import CatalogType, Collection, Item, Link, RelType
from pystac.layout import HrefLayoutStrategy
from pystac_client import Client, Modifiable
from pystac_client.collection_client import CollectionClient
from pystac_client.stac_api_io import StacApiIO, Timeout
from requests import Request, Response

from rs_client.rs_client import APIKEY_HEADER, TIMEOUT, RsClient


class StacClient(RsClient, Client):  # type: ignore # pylint: disable=too-many-ancestors
    """StacClient inherits from both rs_client.RsClient and pystac_client.Client. The goal of this class is to
    allow an user to use RS-Server services more easily than calling REST endpoints directly.
    """

    ##################
    # Initialisation #
    ##################

    def __init__(  # pylint: disable=too-many-arguments,super-init-not-called # super-init is called in .open(...)
        self,
        id: str,  # pylint: disable=redefined-builtin
        description: str,
        title: Optional[str] = None,
        stac_extensions: Optional[List[str]] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
        href: Optional[str] = None,
        catalog_type: CatalogType = CatalogType.ABSOLUTE_PUBLISHED,
        strategy: Optional[HrefLayoutStrategy] = None,
        *,
        modifier: Optional[Callable[[Modifiable], None]] = None,
        **kwargs: Dict[str, Any],
    ):
        """
        Constructor. Called only by pystac.
        As an user: don't use this directly, call the open(...) class method instead or RsClient.get_stac_client(...).
        """

        # Call manually the parent pystac Client constructor.
        # The RsClient constructor will be called manually later.
        Client.__init__(
            self,
            id=id,
            description=description,
            title=title,
            stac_extensions=stac_extensions,
            extra_fields=extra_fields,
            href=href,
            catalog_type=catalog_type,
            strategy=strategy,
            modifier=modifier,
            **kwargs,
        )

    @classmethod
    def open(  # type: ignore  # pylint: disable=arguments-renamed, too-many-arguments
        cls,
        # RsClient parameters
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str | None,
        logger: logging.Logger | None = None,
        # pystac Client parameters
        headers: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        ignore_conformance: Optional[bool] = None,
        modifier: Optional[Callable[[Modifiable], None]] = None,
        request_modifier: Optional[Callable[[Request], Union[Request, None]]] = None,
        stac_io: Optional[StacApiIO] = None,
        timeout: Optional[Timeout] = TIMEOUT,
    ) -> StacClient:
        """Create a new StacClient instance."""

        if rs_server_api_key:
            if headers is None:
                headers = {}
            headers[APIKEY_HEADER] = rs_server_api_key

        client: StacClient = super().open(  # type: ignore
            cls.__href_catalog(rs_server_href) + "/catalog/",
            headers,
            parameters,
            ignore_conformance,
            modifier,
            request_modifier,
            stac_io,
            timeout,
        )

        # Manual call to the parent RsClient constructor
        RsClient.__init__(
            client,
            rs_server_href=rs_server_href,
            rs_server_api_key=rs_server_api_key,
            owner_id=owner_id,
            logger=logger,
        )

        return client

    ##############
    # Properties #
    ##############

    @property
    def href_catalog(self) -> str:
        """
        Return the RS-Server catalog URL hostname.
        This URL can be overwritten using the RSPY_HOST_CATALOG env variable (used e.g. for local mode).
        Either it should just be the RS-Server URL.
        """
        return self.__href_catalog(self.rs_server_href)

    @staticmethod
    def __href_catalog(rs_server_href) -> str:
        if from_env := os.getenv("RSPY_HOST_CATALOG", None):
            return from_env.rstrip("/")
        if not rs_server_href:
            raise RuntimeError("RS-Server URL is undefined")
        return rs_server_href.rstrip("/")

    def full_collection_id(self, owner_id: str | None, collection_id: str):
        """
        Return the full collection name as: <owner_id>:<collection_id>

        Args:
            owner_id (str): Collection owner ID. If missing, we use self.owner_id.
            collection_id (str): Collection name
        """
        return f"{owner_id or self.owner_id}:{collection_id}"

    ################################
    # Specific STAC implementation #
    ################################

    @lru_cache()
    def get_collection(self, collection_id: str, owner_id: str | None = None) -> Union[Collection, CollectionClient]:
        """Get the requested collection as <owner_id>:<collection_id>"""
        full_collection_id = self.full_collection_id(owner_id, collection_id)
        return Client.get_collection(self, full_collection_id)

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
        self.add_child(collection)

        # Restore the short collection_id at the root of the collection
        collection.id = short_collection_id

        # Check that the collection is compliant to STAC
        collection.validate_all()

        # Post the collection to the catalog
        return requests.post(
            f"{self.href_catalog}/catalog/collections",
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
        collection_link = f"{self.self_href.rstrip('/')}/collections/{full_collection_id}"
        self.links = [
            link for link in self.links if not ((link.rel == pystac.RelType.CHILD) and (link.href == collection_link))
        ]

        # We need to clear the cache for this and parent "get_collection" methods
        # because their returned value must be updated.
        self.get_collection.cache_clear()
        Client.get_collection.cache_clear()

        # Remove the collection from the server catalog
        return requests.delete(
            f"{self.href_catalog}/catalog/collections/{full_collection_id}",
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

        # Get the collection from the catalog
        collection = self.get_collection(collection_id, owner_id)

        # Update the item  contents
        collection.add_item(item)

        # Post the item to the catalog
        return requests.post(
            f"{self.href_catalog}/catalog/collections/{full_collection_id}/items",
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
        return requests.delete(
            f"{self.href_catalog}/catalog/collections/{full_collection_id}/items/{item_id}",
            **self.apikey_headers,
            timeout=timeout,
        )
