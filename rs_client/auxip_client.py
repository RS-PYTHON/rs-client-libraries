"""AuxipClient class implementation."""

import logging

from rs_client.stac_client import StacClient
from rs_common.config import EPlatform


class AuxipClient(StacClient):
    """
    AuxipClient class implementation.

    Attributes: see :py:class:`RsClient`
        platforms (list[PlatformEnum]): platform list.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str,
        platforms: list[EPlatform],
        logger: logging.Logger | None = None,
    ):
        """AuxipClient class constructor."""
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)
        self.platforms: list[EPlatform] = platforms

    def href(self) -> str:
        """Return the RS-Server hostname and path where the ADGS endpoints are deployed."""
        return f"{self.hostname_for('adgs')}/adgs/aux"

    def station_name(self) -> str:
        """Return "ADGS"."""
        return "ADGS"
