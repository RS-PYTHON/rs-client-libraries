"""AuxipClient class implementation."""

import logging

from rs_client.rs_client import RsClient
from rs_common.config import EPlatform


class AuxipClient(RsClient):
    """
    AuxipClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    def __init__(
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str,
        platforms: list[EPlatform],
        logger: logging.Logger | None = None,
    ):
        """AuxipClient class constructor."""
        super().__init__(rs_server_href, rs_server_api_key, owner_id, platforms, logger)

    def href(self) -> str:
        """Return the RS-Server hostname and path for this class."""
        return f"{self.hostname_for('adgs')}/adgs/aux"

    def station_name(self) -> str:
        """Return "ADGS"."""
        return "ADGS"
