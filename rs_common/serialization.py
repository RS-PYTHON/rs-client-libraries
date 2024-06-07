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

"""(De-)Serialization utility functions for the workflows."""

from datetime import datetime, timezone
from typing import Type, cast

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_client.rs_client import RsClient
from rs_client.stac_client import StacClient
from rs_common.config import DATETIME_FORMAT, ECadipStation


class RsClientSerialization:  # pylint: disable=too-few-public-methods
    """
    We can't pass a RsClient instance to the workflow because it causes (de-)serialization issues.
    So instead we pass the instance parameters, that will be used to recreate a new instance
    from the workflow.

    TODO: also (de-)serialize the pystac_client.Client.open(...) parameters ?

    Attributes:
        cls (Class): RsClient child class type
        rs_server_href (str): RS-Server URL. In local mode, pass None.
        rs_server_api_key (str): API key for RS-Server authentication.
        owner_id (str): ID of the owner of the STAC catalog collections (no special characters allowoed).
        station (ECadipStation): Cadip station (if applicable)

    """

    def __init__(self, client: RsClient):
        """
        Serialize a RsClient instance.

        Args:
            client (RsClient): RsClient instance
        """

        # Save the parameters, except the logging (which should not be (de-)serialized neither.
        self.cls: Type[RsClient] = type(client)
        self.rs_server_href: str | None = client.rs_server_href
        self.rs_server_api_key: str | None = client.rs_server_api_key
        self.owner_id: str | None = client.owner_id

        self.station: ECadipStation | None = None
        if self.cls == CadipClient:
            self.station = cast(CadipClient, client).station

    def deserialize(self, logger=None) -> RsClient:
        """
        Recreate a new RsClient instance from the serialized parameters.

        Return:
            client (RsClient): RsClient instance
            logger (logging.Logger): Logging instance.
        """

        # Init parent class
        client = RsClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, logger)

        # Return child instance
        if self.cls == AuxipClient:
            return client.get_auxip_client()
        if self.cls == CadipClient:
            return client.get_cadip_client(self.station)  # type: ignore
        if self.cls == StacClient:
            return client.get_stac_client()
        raise ValueError(f"Unknown RsClient type: {self.cls}")

    @property
    def get_flow_name(self):
        """Return a unique flow name for Prefect."""
        return f"{self.owner_id}_{datetime.now(timezone.utc).strftime(DATETIME_FORMAT)}"
