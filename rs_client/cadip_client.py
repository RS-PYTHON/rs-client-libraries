"""CadipClient class implementation."""

import logging

from rs_client.rs_client import RsClient
from rs_common.config import ECadipStation, EPlatform


class CadipClient(RsClient):
    """
    CadipClient class implementation.

    Attributes: see :py:class:`RsClient`
        station (ECadipStation): Cadip station
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
        super().__init__(rs_server_href, rs_server_api_key, owner_id, platforms, logger)
        self.station: ECadipStation = station

    def href(self) -> str:
        """Return the RS-Server hostname and path for this class."""
        return f"{self.hostname_for('cadip')}/cadip/{self.station}/cadu"

    def station_name(self) -> str:
        """Return the station name."""
        return self.station.value
