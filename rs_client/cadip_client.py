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

"""CadipClient class implementation."""

import logging
import os
from datetime import datetime

import requests

from rs_client.rs_client import TIMEOUT, RsClient
from rs_common.config import DATETIME_FORMAT, ECadipStation, EPlatform


class CadipClient(RsClient):
    """
    CadipClient class implementation.

    Attributes: see :py:class:`RsClient`
        station (ECadipStation): Cadip station
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str,
        station: ECadipStation,
        logger: logging.Logger | None = None,
    ):
        """CadipClient class constructor."""
        super().__init__(rs_server_href, rs_server_api_key, owner_id, logger)
        try:
            self.station: ECadipStation = ECadipStation[station]
        except KeyError as e:
            self.logger.exception(f"There is no such CADIP station: {station}")
            raise RuntimeError(f"There is no such CADIP station: {station}") from e

    @property
    def href_cadip(self) -> str:
        """
        Return the RS-Server CADIP URL hostname.
        This URL can be overwritten using the RSPY_HOST_CADIP env variable (used e.g. for local mode).
        Either it should just be the RS-Server URL.
        """
        if from_env := os.getenv("RSPY_HOST_CADIP", None):
            return from_env.rstrip("/")
        if not self.rs_server_href:
            raise RuntimeError("RS-Server URL is undefined")
        return self.rs_server_href.rstrip("/")

    @property
    def href_search(self) -> str:
        """Return the RS-Server hostname and path where the CADIP search endpoint is deployed."""
        return f"{self.href_cadip}/cadip/{self.station.value}/cadu/search"

    @property
    def href_session(self) -> str:
        """Return the RS-Server hostname and path where the CADIP search session endpoint is deployed."""
        return f"{self.href_cadip}/cadip/{self.station.value}/session"

    @property
    def href_staging(self) -> str:
        """Return the RS-Server hostname and path where the CADIP staging endpoint is deployed."""
        return f"{self.href_cadip}/cadip/{self.station.value}/cadu"

    @property
    def href_status(self) -> str:
        """Return the RS-Server hostname and path where the CADIP status endpoint is deployed."""
        return f"{self.href_cadip}/cadip/{self.station.value}/cadu/status"

    @property
    def station_name(self) -> str:
        """Return the station name."""
        return self.station.value  # TO BE DISCUSSED: maybe just return "CADIP"

    ############################
    # Call RS-Server endpoints #
    ############################

    def search_sessions(  # pylint: disable=too-many-arguments
        self,
        session_ids: list[str] | None = None,
        start_date: datetime | None = None,
        stop_date: datetime | None = None,
        platforms: list[EPlatform] | None = None,
        timeout: int = TIMEOUT,
    ) -> list[dict]:  # TODO return pystac.ItemCollection instead
        """Endpoint to retrieve list of sessions from any CADIP station.

        Args:
            timeout (int): The timeout duration for the HTTP request.
            session_ids (list[str]): Session identifiers
                (eg: ["S1A_20170501121534062343"] or ["S1A_20170501121534062343, S1A_20240328185208053186"])
            start_date (datetime): Start date of the time interval
            stop_date (datetime): Stop date of the time interval
            platforms (list[PlatformEnum]): platform list
        """

        payload = {}
        if session_ids:
            payload["id"] = ",".join(session_ids)
        if platforms:
            payload["platform"] = ",".join([platform.value for platform in platforms])
        if start_date:
            payload["start_date"] = start_date.strftime(DATETIME_FORMAT)
        if stop_date:
            payload["stop_date"] = stop_date.strftime(DATETIME_FORMAT)
        try:
            response = requests.get(
                self.href_session,
                params=payload,
                timeout=timeout,
                **self.apikey_headers,
            )
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            self.logger.exception(f"Could not get the response from the session search endpoint: {e}")
            raise RuntimeError("Could not get the response from the session search endpoint") from e

        sessions = []
        try:
            if response.ok:
                for session_info in response.json()["features"]:
                    sessions.append(session_info)
            else:
                self.logger.error(f"Error: {response.status_code} : {response.json()}")
        except KeyError as e:
            raise RuntimeError("Wrong format of session search endpoint answer") from e

        return sessions
