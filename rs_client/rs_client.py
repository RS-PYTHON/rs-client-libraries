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

"""RsClient class implementation."""

import logging
from datetime import datetime
from typing import Union

import requests

from rs_common.config import DATETIME_FORMAT, ECadipStation, EDownloadStatus
from rs_common.logging import Logging


class RsClient:
    """
    RsClient class implementation.

    Attributes:
        rs_server_href (str): RS-Server URL.
        rs_server_api_key (str): API key for RS-Server authentication.
        owner_id (str): Owner of the catalog collections. The API key must give us the right to read/write this owner
                        collections in the catalog. This owner ID is also used in the RS-Client logging.
        logger (logging.Logger): Logging instance.
    """

    def __init__(
        self,
        rs_server_href: str | None,
        rs_server_api_key: str | None,
        owner_id: str,
        logger: logging.Logger | None = None,
    ):
        """RsClient class constructor."""
        self.rs_server_href: str | None = rs_server_href
        self.rs_server_api_key: str | None = rs_server_api_key
        self.owner_id: str = owner_id
        self.logger: logging.Logger = logger or Logging.default(__name__)

        self.apikey_headers: dict = {"headers": {"x-api-key": rs_server_api_key}} if rs_server_api_key else {}

    #############################
    # Get child class instances #
    #############################

    def get_auxip_client(self) -> "AuxipClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class AuxipClient, with the same attributes as this "self" instance.
        """
        from rs_client.auxip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            AuxipClient,
        )

        return AuxipClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, self.logger)

    def get_cadip_client(
        self,
        station: ECadipStation,
    ) -> "CadipClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class CadipClient, with the same attributes as this "self" instance.

        Args:
            station (ECadipStation): Cadip station
        """
        from rs_client.cadip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            CadipClient,
        )

        return CadipClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, station, self.logger)

    def get_stac_client(self) -> "StacClient":  # type: ignore # noqa: F821
        """
        Return an instance of the child class StacClient, with the same attributes as this "self" instance.
        """
        from rs_client.stac_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            StacClient,
        )

        return StacClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, self.logger)

    ############################
    # Call RS-Server endpoints #
    ############################

    def staging_status(self, filename, timeout: int) -> EDownloadStatus:
        """Check the status of a file download from the specified rs-server endpoint.

        This function sends a GET request to the rs-server endpoint with the filename as a query parameter
        to retrieve the status of the file download. If the response is successful and contains a 'status'
        key in the JSON response, the function returns the corresponding download status enum value. If the
        response is not successful or does not contain the 'status' key, the function returns a FAILED status.

        Args:
            filename (str): The name of the file for which to check the status.
            timeout (int): The timeout duration for the HTTP request.

        Returns:
            EDownloadStatus: The download status enum value based on the response from the endpoint.
        """

        # TODO: check the status for a certain timeout if http returns NOK ?
        try:
            response = requests.get(
                self.href_status,  # pylint: disable=no-member # ("self" is AuxipClient or CadipClient)
                params={"name": filename},
                timeout=timeout,
                **self.apikey_headers,
            )

            eval_response = response.json()
            if (
                response.ok
                and "name" in eval_response.keys()
                and filename == eval_response["name"]
                and "status" in eval_response.keys()
            ):
                return EDownloadStatus(eval_response["status"])

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            self.logger.exception(f"Status endpoint exception: {e}")

        return EDownloadStatus.FAILED

    def staging(self, filename: str, timeout: int, s3_path: str = "", tmp_download_path: str = ""):
        """Stage a file for download.

        This method stages a file for download by sending a request to the staging endpoint
        with optional parameters for S3 path and temporary download path.

        Args:
            filename (str): The name of the file to be staged for download.
            timeout (int): The timeout duration for the HTTP request.
            s3_path (str, optional): The S3 path where the file will be stored after download.
                Defaults to an empty string.
            tmp_download_path (str, optional): The temporary download path for the file.
                Defaults to an empty string.

        Raises:
            RuntimeError: If an error occurs while staging the file.

        """

        # dictionary to be used for payload request
        payload = {}
        # some protections for the optional args
        if s3_path:
            payload["obs"] = s3_path
        if tmp_download_path:
            payload["local"] = tmp_download_path

        # update the filename to be ingested
        payload["name"] = filename
        try:
            # logger.debug(f"Calling  {endpoint} with payload {payload}")
            response = requests.get(
                self.href_staging,  # pylint: disable=no-member # ("self" is AuxipClient or CadipClient)
                params=payload,
                timeout=timeout,
                **self.apikey_headers,
            )
            self.logger.debug(f"Download start endpoint returned in {response.elapsed.total_seconds()}")
            if not response.ok:
                self.logger.error(f"The download endpoint returned error for file {filename}\n")
                raise RuntimeError(f"The download endpoint returned error for file {filename}")
        except (
            requests.exceptions.RequestException,
            requests.exceptions.Timeout,
            requests.exceptions.ReadTimeout,
        ) as e:
            self.logger.exception(f"Staging file exception for {filename}:", e)
            raise RuntimeError(f"Staging file exception for {filename}") from e

    def search_stations(  # pylint: disable=too-many-arguments
        self,
        start_date: datetime,
        stop_date: datetime,
        timeout: int,
        limit: Union[int, None] = None,
        sortby: Union[str, None] = None,
    ) -> list:
        """Retrieve a list of files from the specified endpoint.

        This function queries the specified endpoint to retrieve a list of files available in the
        station (CADIP, ADGS, LTA ...) that were collected by the stalleite within the provided time range,
        starting from 'start_date' up to 'stop_date' (inclusive).

        Args:
            start_date (datetime): The start date of the time range.
            stop_date (datetime): The stop date of the time range.
            timeout (int): The timeout duration for the HTTP request.
            limit (int, optional): The maximum number of results to return. Defaults to None.
            sortby (str, optional): The attribute to sort the results by. Defaults to None.

        Returns:
            files (list): A list of files (in stac format) available at the endpoint within the specified time range.

        Raises:
            RuntimeError: if the endpoint can't be reached

        Notes:
            - This function queries the specified endpoint with a time range to retrieve information about
            available files.
            - It constructs a payload with the start and stop dates in ISO 8601 format and sends a GET
            request to the endpoint.
            - The response is expected to be in JSON format with STAC, containing information about available files.
                EODAG should return the information in STACH format through JSON
            - The function then extracts file information from the response and returns a list of files.
        """

        payload = {
            "datetime": start_date.strftime(DATETIME_FORMAT)
            + "/"  # 2014-01-01T12:00:00Z/2023-12-30T12:00:00Z",
            + stop_date.strftime(DATETIME_FORMAT),
        }
        if limit:
            payload["limit"] = str(limit)
        if sortby:
            payload["sortby"] = str(sortby)
        try:
            response = requests.get(
                self.href_search,  # pylint: disable=no-member # ("self" is AuxipClient or CadipClient)
                params=payload,
                timeout=timeout,
                **self.apikey_headers,
            )
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            self.logger.exception(f"Could not get the response from the station search endpoint: {e}")
            raise RuntimeError("Could not get the response from the station search endpoint") from e

        files = []
        try:
            if response.ok:
                for file_info in response.json()["features"]:
                    files.append(file_info)
            else:
                self.logger.error(f"Error: {response.status_code} : {response.json()}")
        except KeyError as e:
            raise RuntimeError("Wrong format of search endpoint answer") from e

        return files

    ##############################################
    # Methods to be implemented by child classes #
    ##############################################

    @property
    def href_search(self) -> str:
        """Implemented by AuxipClient and CadipClient."""
        raise NotImplementedError

    @property
    def href_staging(self) -> str:
        """Implemented by AuxipClient and CadipClient."""
        raise NotImplementedError

    @property
    def href_status(self) -> str:
        """Implemented by AuxipClient and CadipClient."""
        raise NotImplementedError
