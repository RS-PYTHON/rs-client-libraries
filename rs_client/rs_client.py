"""RsClient class implementation."""

import logging
from datetime import datetime
from typing import Union

import requests

from rs_common.config import ECadipStation, EDownloadStatus, EPlatform
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

    def get_auxip_client(self) -> "AuxipClient":  # type: ignore
        """
        Return an instance of the child class AuxipClient, with the same attributes as this "self" instance.
        """
        from rs_client.auxip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            AuxipClient,
        )

        return AuxipClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, self.logger)

    def get_cadip_client(self, station: ECadipStation, platforms: list[EPlatform]) -> "CadipClient":  # type: ignore
        """
        Return an instance of the child class CadipClient, with the same attributes as this "self" instance.

        Args:
            station (ECadipStation): Cadip station
            platforms (list[PlatformEnum]): platform list.
        """
        from rs_client.cadip_client import (  # pylint: disable=import-outside-toplevel,cyclic-import
            CadipClient,
        )

        return CadipClient(self.rs_server_href, self.rs_server_api_key, self.owner_id, station, platforms, self.logger)

    def get_stac_client(self) -> "StacClient":  # type: ignore
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
            apikey_headers (dict): The apikey used for request (may be empty)
            endpoint (str): The rs-server endpoint URL to query for the file status.
            filename (str): The name of the file for which to check the status.

        Returns:
            EDownloadStatus: The download status enum value based on the response from the endpoint.

        """
        # TODO: check the status for a certain timeout if http returns NOK ?
        try:
            response = requests.get(
                self.href_status,
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

    def staging(self, filename: str, timeout: int, s3_path: str = None, tmp_download_path: str = None):
        """Prefect task function to stage (=download/ingest) files.

        This prefect task function access the RS-Server endpoints that start the download of files and
        check the status for the actions

        Args:
            config (PrefectTaskConfig): Configuration object containing details about the files to be downloaded.

        Raises:
            None: This function does not raise any exceptions.

        Returns:
            failed_failes: A list of files which could not be downloaded and / or uploaded to the s3.
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
                self.href_staging,
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
        """Retrieve a list of files from the specified endpoint within the given time range.

        This function queries the specified endpoint to retrieve a list of files available in the
        station (CADIP, ADGS, LTA ...) within the provided time range, starting from 'start_date' up
        to 'stop_date' (inclusive).

        Args:
            apikey_headers (dict): The apikey used for request (may be empty)
            endpoint (str): The URL endpoint to query for file information.
            start_date (datetime): The start date/time of the time range.
            stop_date (datetime, optional): The stop date/time of the time range.
            sortby (str, optional): Sort by +/-fieldName (ascending/descending), default is "-datetime"

        Returns:
            files (list): A list of files (in stac format) available at the endpoint within the specified time range.

        Raises:
            - RuntimeError if the endpoint can't be reached

        Notes:
            - This function queries the specified endpoint with a time range to retrieve information about
            available files.
            - It constructs a payload with the start and stop dates in ISO 8601 format and sends a GET
            request to the endpoint.
            - The response is expected to be in JSON format, containing information about available files.
            - The function then extracts file information from the response and returns a list of files.

        """

        payload = {
            "datetime": start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            + "/"  # 2014-01-01T12:00:00Z/2023-12-30T12:00:00Z",
            + stop_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if limit:
            payload["limit"] = str(limit)
        if sortby:
            payload["sortby"] = str(sortby)
        try:
            response = requests.get(
                self.href_search,
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
