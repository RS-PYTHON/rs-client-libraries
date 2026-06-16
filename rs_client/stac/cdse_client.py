# Copyright 2023-2026 Airbus, CS Group
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

"""CdseClient class implementation."""

import logging
from typing import Any

from rs_client.stac.stac_base import StacBase

CDSE_STAC_HREF = "https://stac.dataspace.copernicus.eu/v1/"


class CdseClient(StacBase):
    """
    CdseClient class implementation.

    Attributes: see :py:class:`RsClient`
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        logger: logging.Logger | None = None,
        **kwargs: dict[str, Any],
    ):
        """
        Initializes a CdseClient instance.

        Args:
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
        super().__init__(None, None, None, logger, CDSE_STAC_HREF, **kwargs)

    @property
    def href_service(self) -> str:
        """
        Return the CDSE STAC URL.
        """
        return CDSE_STAC_HREF
