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
from typing import Any

from rs_client.stac_base import StacBase
from rs_common.config import ECadipStation
from rs_common.utils import get_href_service


class CadipClient(StacBase):
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
        station: str,
        logger: logging.Logger | None = None,
        **kwargs: dict[str, Any],
    ):
        """
        Initializes a CadipClient instance.

        Args:
            rs_server_href (str | None): The URL of the RS-Server. Pass None for local mode.
            rs_server_api_key (str | None): API key for authentication.
            owner_id (str | None): ID of the catalog owner.
            station (EAuxipStation | str): The CADIP station identifier.
            logger (logging.Logger | None, optional): Logger instance (default: None).
            **kwargs: Arbitrary keyword arguments that may include:
                - `headers` (Optional[Dict[str, str]])
                - `parameters` (Optional[Dict[str, Any]])
                - `ignore_conformance` (Optional[bool])
                - `modifier` (Callable[[Collection | Item | ItemCollection | dict[Any, Any]], None] | None)
                - `request_modifier` (Optional[Callable[[Request], Union[Request, None]]])
                - `stac_io` (Optional[StacApiIO])
                - `timeout` (Optional[Timeout])

        Raises:
            RuntimeError: If the provided station is not a valid CADIP station.
        """
        super().__init__(
            rs_server_href,
            rs_server_api_key,
            owner_id,
            logger,
            get_href_service(rs_server_href, "RSPY_HOST_CADIP") + "/cadip/",
            **kwargs,
        )
        try:
            self.station: ECadipStation = ECadipStation[station] if isinstance(station, str) else station
        except KeyError as e:
            self.log_and_raise(f"There is no such CADIP station: {station}", e)

    @property
    def href_service(self) -> str:
        """
        Return the RS-Server CADIP URL hostname.
        This URL can be overwritten using the RSPY_HOST_CADIP env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_CADIP") + "/cadip"

    @property
    def station_name(self) -> str:
        """Return the station name."""
        return self.station.value
