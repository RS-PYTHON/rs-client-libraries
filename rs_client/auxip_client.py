"""AuxipClient class implementation."""

import logging
import os

from rs_client.stac_client import StacClient


class AuxipClient(StacClient):
    """
    AuxipClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str,
        logger: logging.Logger | None = None,
    ):
        """AuxipClient class constructor."""
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)

    @property
    def href_adgs(self) -> str:
        """
        Return the RS-Server ADGS URL hostname.
        This URL can be overwritten using the RSPY_HOST_ADGS env variable (used e.g. for local mode).
        Either it should just be the RS-Server URL.
        """
        if from_env := os.getenv("RSPY_HOST_ADGS", None):
            return from_env
        if self.rs_server_href is None:
            raise RuntimeError("RS-Server URL is undefined")
        return self.rs_server_href.rstrip("/")

    @property
    def href_search(self) -> str:
        """Return the RS-Server hostname and path where the ADGS search endpoint is deployed."""
        return f"{self.href_adgs}/adgs/aux/search"

    @property
    def href_staging(self) -> str:
        """Return the RS-Server hostname and path where the ADGS staging endpoint is deployed."""
        return f"{self.href_adgs}/adgs/aux"

    @property
    def href_status(self) -> str:
        """Return the RS-Server hostname and path where the ADGS status endpoint is deployed."""
        return f"{self.href_adgs}/adgs/aux/status"

    @property
    def station_name(self) -> str:
        """Return "ADGS"."""
        return "ADGS"
