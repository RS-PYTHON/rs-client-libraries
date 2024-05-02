import enum
from datetime import datetime
from typing import Any, Union

import requests

from rs_workflows.utils.logging import Logging

CADIP = ["CADIP", "INS", "MPS", "MTI", "NSG", "SGS"]
ADGS = "ADGS"


class EDownloadStatus(str, enum.Enum):
    """
    Download status enumeration.
    """

    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    DONE = "DONE"


class RsClient:
    def __init__(
        self,
        apikey: str,
        rs_server_href: str,
        rs_server_href_endpoint: str,
        rs_server_href_catalog: str,
        owner_id: str,
        station: str,
        platform: str,
        logger: Any,
    ):
        """Init function of RsClient class"""
        self.apikey = apikey
        self.apikey_headers = RsClient.create_apikey_headers(apikey)
        self.rs_server_href = rs_server_href.rstrip("/") + "/" + rs_server_href_endpoint.lstrip("/")
        self.status_endpoint = self.rs_server_href + "/status"
        self.rs_server_href_catalog = rs_server_href_catalog
        self.owner_id = owner_id
        self.station = station
        self.platform = platform
        self.logger = logger
        if not self.logger:
            self.logger = Logging.default("RsClient")

    @staticmethod
    def create_apikey_headers(apikey):
        """Create the apikey

        This function creates the apikey headers used when calling the endpoints. This may be empty
        Args:
            apikey_headers (dict): A dictionary with the apikey
        """

        return {"headers": {"x-api-key": apikey}} if apikey else {}

    def check_status(self, filename, endpoint_timeout):
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
                self.status_endpoint,
                params={"name": filename},
                timeout=endpoint_timeout,
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

    def staging_file(self, filename, s3_path, tmp_download_path, staging_endpoint_timeout):
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
                self.rs_server_href,
                params=payload,
                timeout=staging_endpoint_timeout,
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

    def get_station_files_list(  # pylint: disable=too-many-arguments
        self,
        start_date: datetime,
        stop_date: datetime,
        search_endpoint_timeout,
        limit: Union[int, None] = None,
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
        try:
            response = requests.get(
                self.rs_server_href + "/search",
                params=payload,
                timeout=search_endpoint_timeout,
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


class AuxipClient(RsClient):
    def __init__(
        self,
        apikey: str,
        rs_server_href: str,
        rs_server_href_catalog: str,
        owner_id: str,
        platform: str,
        logger: Any,
    ):
        """Init function of AuxipClient class"""
        super().__init__(apikey, rs_server_href, "/adgs/aux", rs_server_href_catalog, owner_id, ADGS, platform, logger)


class CadipClient(RsClient):
    def __init__(
        self,
        apikey: str,
        rs_server_href: str,
        rs_server_href_catalog: str,
        owner_id: str,
        station: str,
        platform: str,
        logger: Any,
    ):
        """Init function of CadipClient class"""
        if station not in CADIP:
            self.logger.error(f"Unknown CADIP station type: {station}")
            raise RuntimeError("Unknown CADIP station type: {station}")

        super().__init__(
            apikey,
            rs_server_href,
            f"/cadip/{station}/cadu",
            rs_server_href_catalog,
            owner_id,
            station,
            platform,
            logger,
        )
        self.station = station
