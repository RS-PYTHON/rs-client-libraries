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

"""AuxipClient class implementation."""

import os

from rs_client.rs_client import RsClient
from rs_common.config import AUXIP_STATION


class AuxipClient(RsClient):
    """
    AuxipClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    @property
    def href_adgs(self) -> str:
        """
        Return the RS-Server ADGS URL hostname.
        This URL can be overwritten using the RSPY_HOST_ADGS env variable (used e.g. for local mode).
        Either it should just be the RS-Server URL.
        """
        if from_env := os.getenv("RSPY_HOST_ADGS", None):
            return from_env.rstrip("/")
        if not self.rs_server_href:
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
        """Return "AUXIP"."""
        return AUXIP_STATION
