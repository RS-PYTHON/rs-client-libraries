"""CadipClient class implementation."""

import logging

from rs_client.stac_client import StacClient
from rs_common.config import ECadipStation, EPlatform


class CadipClient(StacClient):
    """
    CadipClient class implementation.

    Attributes: see :py:class:`RsClient`
        station (ECadipStation): Cadip station
        platforms (list[PlatformEnum]): platform list.
    """

    def __init__(
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str,
        station: ECadipStation,
        platforms: list[EPlatform],
        logger: logging.Logger | None = None,
    ):
        """CadipClient class constructor."""
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)
        self.station: ECadipStation = station
        self.platforms: list[EPlatform] = platforms

    def href(self) -> str:
        """Return the RS-Server hostname and path where the CADIP endpoints are deployed."""
        return f"{self.hostname_for('cadip')}/cadip/{self.station.value}/cadu"

    def station_name(self) -> str:
        """Return the station name."""
        return f"CADIP/{self.station.value}"
