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
