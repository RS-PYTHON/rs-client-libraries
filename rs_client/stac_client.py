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

import json
import logging
import os
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Union

import requests
from pystac import CatalogType, Collection, Item
from pystac.layout import HrefLayoutStrategy
from pystac_client import Client, Modifiable
from pystac_client.collection_client import CollectionClient
from pystac_client.stac_api_io import StacApiIO, Timeout
from requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_400_BAD_REQUEST

from rs_client.rs_client import APIKEY_HEADER, TIMEOUT, RsClient


class StacClient(RsClient, Client):
    """StacClient inherits from pystac_client.Client. The goal of this class is to
    allow an user to use RS-Server services more easily than calling REST endpoints directly.

    Args:
        Client : The pystac_client that StacClient inherits from.
    """

    ##################
    # Initialisation #
    ##################

    def __init__(  # pylint: disable=too-many-arguments
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
    def open(  # pylint: disable=arguments-renamed, too-many-arguments
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

        client: StacClient = super().open(
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
            return from_env
        if not rs_server_href:
            raise RuntimeError("RS-Server URL is undefined")
        return rs_server_href.rstrip("/")

    ################################
    # Specific STAC implementation #
    ################################

    def validate_collection(self, collection: dict) -> bool:
        """Check if a collection is STAC compliant.

        Args:
            collection (dict): The collection to be checked

        Returns:
            bool: True if the collection is conform, False otherwise
        """
        mandatory_elements = ["id", "description", "license", "extent", "links", "stac_version"]
        for element in mandatory_elements:
            if element not in collection:
                return False
        return True

    @lru_cache()
    def get_collection(self, collection_id: str, owner_id: str = None) -> Union[Collection, CollectionClient]:
        complete_collection_id = f"{owner_id or self.owner_id}:{collection_id}"
        return super().get_collection(complete_collection_id)

    def create_new_collection(  # pylint: disable=too-many-arguments
        self,
        collection_id: str,
        extent: dict,
        href_license: str = "https://creativecommons.org/licenses/publicdomain/",
        collection_license: str = "public-domain",
        stac_version: str = "1.0.0",
        description: str = "",
        owner_id: str = "",
    ) -> dict:
        """Create a new collection.

        Args:
            collection_id (str): The Collection id.
            extent (dict): Contains spatial and temporal coverage.
            href_license (_type_, optional): The href of the license.
            Defaults to "https://creativecommons.org/licenses/publicdomain/".
            collection_license (str, optional): The license name. Defaults to "public-domain".
            stac_version (str, optional): The stac_version. Defaults to "1.0.0".
            description (str, optional): The collection description. Defaults to "".
            owner_id (str, optional): The owner id. Defaults to None.

        Returns:
            dict: A new collection.
        """
        owner_id = owner_id if owner_id else self.owner_id
        description = description if description else f"This is the collection {collection_id} from user {owner_id}."
        new_collection = {
            "id": collection_id,
            "type": "Collection",
            "owner": owner_id,
            "links": [
                {
                    "rel": "items",
                    "type": "application/geo+json",
                    "href": f"{self.href_catalog}/collections/{collection_id}/items",
                },
                {"rel": "parent", "type": "application/json", "href": f"{self.href_catalog}/"},
                {"rel": "root", "type": "application/json", "href": f"{self.href_catalog}/"},
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"{self.href_catalog}/collections/{collection_id}",
                },
                {
                    "rel": "license",
                    "href": href_license,
                    "title": collection_license,
                },
            ],
            "extent": extent,
            "license": collection_license,
            "description": description,
            "stac_version": stac_version,
        }
        return new_collection

    def post_collection(self, collection: json, timeout: int = TIMEOUT) -> JSONResponse:
        """Create a new collection and post it in the Catalog.

        Args:
            collection (json): The collection to post.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            JSONResponse: The response of the request.
        """
        if not self.validate_collection(collection):
            return JSONResponse(content="Collection format is Invalid", status_code=HTTP_400_BAD_REQUEST)
        return requests.post(
            f"{self.href_catalog}/catalog/collections",
            json=collection,
            **self.apikey_headers,
            timeout=timeout,
        )

    def delete_collection(self, collection_id: str, owner_id: str = "", timeout: int = TIMEOUT) -> JSONResponse:
        """Delete a collection.

        Args:
            collection_id (str): The collection id.
            owner_id (str, optional): The owner id. Defaults to None.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            JSONResponse: The response of the request.
        """
        return requests.delete(
            f"{self.href_catalog}/catalog/collections/{owner_id or self.owner_id}:{collection_id}",
            **self.apikey_headers,
            timeout=timeout,
        )

    def add_item(self, collection: Collection, item: Item, owner_id: str = "", timeout: int = TIMEOUT) -> JSONResponse:
        """Update the item links then post the item to the catalog.

        Args:
            collection (Collection): STAC collection
            item (Item): STAC item to update and post
            owner_id (str, optional): The owner id. Defaults to None.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            JSONResponse: The response of the request.
        """
        # Update the item
        collection.add_item(item)

        # Post to the catalog
        return requests.post(
            f"{self.href_catalog}/catalog/collections/{owner_id or self.owner_id}:{collection.id}/items",
            json=item.to_dict(),
            **self.apikey_headers,
            timeout=timeout,
        )
