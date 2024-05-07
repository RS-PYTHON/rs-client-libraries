""" contains the class StacCLient that inherits from pystact_client Client."""

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
)

from functools import lru_cache
import re
from pystac_client import Client, Modifiable
from pystac_client.stac_api_io import StacApiIO, Timeout

from pystac_client.collection_client import CollectionClient
from pystac import CatalogType, Collection
from pystac.layout import HrefLayoutStrategy
from requests import Request


class StacClient(Client):
    """StacClient inherits from pystac_client.Client. The goal of this class is to
    allow an user to use RS-Server services more easily than calling REST endpoints directly.

    Args:
        Client : The pystac_client that StacClient inherits from.
    """

    rs_server_api_key: str
    rs_server_href: str
    owner_id: str

    def __init__(
        self,
        id: str,
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

    # @lru_cache()
    # def get_collection(self, collection_id: str) -> Union[Collection, CollectionClient]:
    #     owner_collection_regex = r".*:.*"
    #     if not re.match(owner_collection_regex, collection_id):
    #         collection_id = f"{self.owner_id}:{collection_id}"
    #     return super().get_collection(collection_id)
