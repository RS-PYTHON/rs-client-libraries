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

import json
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Union

import requests
from pystac import CatalogType, Collection
from pystac.layout import HrefLayoutStrategy
from pystac_client import Client, Modifiable
from pystac_client.collection_client import CollectionClient
from pystac_client.stac_api_io import StacApiIO, Timeout
from requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_400_BAD_REQUEST


class StacClient(Client):
    """StacClient inherits from pystac_client.Client. The goal of this class is to
    allow an user to use RS-Server services more easily than calling REST endpoints directly.

    Args:
        Client : The pystac_client that StacClient inherits from.
    """

    rs_server_api_key: str
    rs_server_href: str
    owner_id: str

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
        super().__init__(
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
        url: str,
        rs_server_api_key: str,
        rs_server_href: str,
        owner_id: str,
        headers: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        ignore_conformance: Optional[bool] = None,
        modifier: Optional[Callable[[Modifiable], None]] = None,
        request_modifier: Optional[Callable[[Request], Union[Request, None]]] = None,
        stac_io: Optional[StacApiIO] = None,
        timeout: Optional[Timeout] = None,
    ) -> "StacClient":
        if headers is None:
            headers = {"x-api-key": rs_server_api_key}
        else:
            headers["x-api-key"] = rs_server_api_key

        client: StacClient = super().open(
            url,
            headers,
            parameters,
            ignore_conformance,
            modifier,
            request_modifier,
            stac_io,
            timeout,
        )
        client.rs_server_api_key = rs_server_api_key
        client.rs_server_href = rs_server_href
        client.owner_id = owner_id
        return client

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
        if owner_id:
            complete_collection_id = f"{owner_id}:{collection_id}"
        else:
            complete_collection_id = f"{self.owner_id}:{collection_id}"
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
                    "href": f"{self.rs_server_href}/collections/{collection_id}/items",
                },
                {"rel": "parent", "type": "application/json", "href": f"{self.rs_server_href}/"},
                {"rel": "root", "type": "application/json", "href": f"{self.rs_server_href}/"},
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"{self.rs_server_href}/collections/{collection_id}",
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

    def post_collection(self, collection: json) -> JSONResponse:
        """Create a new collection and post it in the Catalog.

        Args:
            collection (json): The collection to post.

        Returns:
            JSONResponse: The response of the request.
        """
        if not self.validate_collection(collection):
            return JSONResponse(content="Collection format is Invalid", status_code=HTTP_400_BAD_REQUEST)
        headers = {"x-api-key": self.rs_server_api_key}
        return requests.post(f"{self.rs_server_href}/catalog/collections", json=collection, headers=headers, timeout=10)

    def delete_collection(self, collection_id: str, owner_id: str = "") -> JSONResponse:
        """Delete a collection.

        Args:
            collection_id (str): The collection id.
            owner_id (str, optional): The owner id. Defaults to None.

        Returns:
            JSONResponse: The response of the request.
        """
        headers = {"x-api-key": self.rs_server_api_key}
        if owner_id:
            response = requests.delete(
                f"{self.rs_server_href}/catalog/collections/{owner_id}:{collection_id}",
                headers=headers,
                timeout=10,
            )
        else:
            response = requests.delete(
                f"{self.rs_server_href}/catalog/collections/{self.owner_id}:{collection_id}",
                headers=headers,
                timeout=10,
            )
        return response

    @property
    def href_catalog(self) -> str:
        """
        Return the RS-Server catalog URL hostname.
        This URL can be overwritten using the RSPY_HOST_CATALOG env variable (used e.g. for local mode).
        Either it should just be the RS-Server URL.
        """
        if from_env := os.getenv("RSPY_HOST_CATALOG", None):
            return from_env
        if not self.rs_server_href:
            raise RuntimeError("RS-Server URL is undefined")
        return self.rs_server_href.rstrip("/")
