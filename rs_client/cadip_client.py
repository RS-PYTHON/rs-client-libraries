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

"""CadipClient class implementation."""

import logging
import os
from datetime import datetime

import requests

from rs_client.rs_client import TIMEOUT, RsClient
from rs_common.config import DATETIME_FORMAT, ECadipStation, EPlatform
from rs_common.utils import get_href_service


class CadipClient(RsClient):
    """
    CadipClient class implementation.

    Attributes: see :py:class:`RsClient`
        station (ECadipStation): Cadip station
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str | None,
        station: ECadipStation,
        logger: logging.Logger | None = None,
        **kwargs
    ):
        """CadipClient class constructor."""
        super().__init__(rs_server_href, 
                         rs_server_api_key, 
                         owner_id, 
                         logger, 
                         get_href_service(rs_server_href, "RSPY_HOST_CADIP") + "/cadip/", 
                         **kwargs)
        try:
            self.station: ECadipStation = ECadipStation[station]
        except KeyError as e:
            self.logger.exception(f"There is no such CADIP station: {station}")
            raise RuntimeError(f"There is no such CADIP station: {station}") from e

    @property
    def href_cadip(self) -> str:
        """
        Return the RS-Server CADIP URL hostname.
        This URL can be overwritten using the RSPY_HOST_CADIP env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_CADIP")

    @property
    def href_search(self) -> str:
        """Return the RS-Server hostname and path where the CADIP search endpoint is deployed."""
        return f"{self.href_cadip}/cadip/search"

    @property
    def station_name(self) -> str:
        """Return the station name."""
        return self.station.value  # TO BE DISCUSSED: maybe just return "CADIP"
    
    @property
    def href_landing(self):
        """Return the RS-Server hostname and path where the landing page endpoint is deployed."""
        return f"{self.href_cadip}/cadip"
    
    @property
    def href_all_collections(self):
        """Return the RS-Server hostname and path from where all the existent collections can be retrieved."""
        return f"{self.href_cadip}/cadip/collections"

    @property
    def href_collection(self, collection_id):
        """Return the RS-Server hostname and path from where one collection can be retrieved."""
        return f"{self.href_cadip}/cadip/collections/{collection_id}"

    @property
    def href_collection_all_items(self, collection_id):
        """Return the RS-Server hostname and path from where all the existent items 
        from a collection can be retrieved.
        """
        return f"{self.href_cadip}/cadip/collections/{collection_id}/items"

    @property
    def href_collection_item(self, collection_id, item_id):
        """Return the RS-Server hostname and path from where one item from a collection can be retrieved.
        The item_id is in fact a cadip session_id, which has more then one assets (files)
        """
        return f"{self.href_cadip}/cadip/collections/{collection_id}/items/{item_id}"

    @property
    def href_collection_queryables(self, collection_id):
        """Return the RS-Server hostname and path endpoint for query a collection."""
        return f"{self.href_cadip}/cadip/collections/{collection_id}/queryables"
    
    @property
    def href_queryables(self):
        """Return the RS-Server hostname and path endpoint for general query."""
        return f"{self.href_cadip}/cadip/collections/queryables"

    ############################
    # Call RS-Server endpoints #
    ############################

    