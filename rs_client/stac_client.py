"""StacClient class implementation."""

import logging

from rs_client.rs_client import RsClient


class StacClient(RsClient):
    """
    StacClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    def __init__(
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str,
        logger: logging.Logger | None = None,
    ):
        """StacClient class constructor."""
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)

    def href(self) -> str:
        """Return the RS-Server hostname and path where the child class endpoints (ADGS, CADIP, ...) are deployed."""
        return ""  # not applicable

    def station_name(self) -> str:
        """Return the station name for CADIP ("INS", "MPS", ...) or just "ADGS" for ADGS."""
        return ""  # not applicable
