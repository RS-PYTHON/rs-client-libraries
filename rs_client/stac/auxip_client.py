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

import logging
from typing import Any

from rs_client.stac.stac_base import StacBase
from rs_common.utils import get_href_service


class AuxipClient(StacBase):
    """
    AuxipClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        logger: logging.Logger | None = None,
        **kwargs: dict[str, Any],
    ):
        """
        Initializes an AuxipClient instance.

        Args:
            rs_server_href (str | None): The URL of the RS-Server. Pass None for local mode.
            rs_server_api_key (str | None): API key for authentication.
            logger (logging.Logger | None, optional): Logger instance (default: None).
            **kwargs: Arbitrary keyword arguments that may include:
                - `headers` (Optional[Dict[str, str]])
                - `parameters` (Optional[Dict[str, Any]])
                - `ignore_conformance` (Optional[bool])
                - `modifier` (Callable[[Collection | Item | ItemCollection | dict[Any, Any]], None] | None)
                - `request_modifier` (Optional[Callable[[Request], Union[Request, None]]])
                - `stac_io` (Optional[StacApiIO])
                - `timeout` (Optional[Timeout])
        """
        super().__init__(
            rs_server_href,
            rs_server_api_key,
            None,
            logger,
            get_href_service(rs_server_href, "RSPY_HOST_ADGS") + "/auxip/",
            **kwargs,
        )

    @property
    def href_service(self) -> str:
        """
        Return the RS-Server ADGS URL hostname.
        This URL can be overwritten using the RSPY_HOST_ADGS env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_ADGS") + "/auxip"
