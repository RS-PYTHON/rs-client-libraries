"""StacClient class implementation."""

import os

from rs_client.rs_client import RsClient


class StacClient(RsClient):  # pylint: disable=abstract-method
    """
    StacClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    @property
    def href_catalog(self) -> str:
        """
        Return the RS-Server catalog URL hostname.
        This URL can be overwritten using the RSPY_HOST_CATALOG env variable (used e.g. for local mode).
        Either it should just be the RS-Server URL.
        """
        if from_env := os.getenv("RSPY_HOST_CATALOG", None):
            return from_env
        if not self.rs_server_href:
            raise RuntimeError("RS-Server URL is undefined")
        return self.rs_server_href.rstrip("/")
