"""Docstring will be here."""
import enum
import logging
import sys
import time
from datetime import datetime

import numpy as np
import requests
from prefect import exceptions, flow, get_run_logger, task
from prefect_dask.task_runners import DaskTaskRunner

CADIP = "CADIP"
ADGS = "ADGS"

DOWNLOAD_FILE_TIMEOUT = 180  # in seconds
SET_PREFECT_LOGGING_LEVEL = "DEBUG"
ENDPOINT_TIMEOUT = 2  # in seconds


class EDownloadStatus(str, enum.Enum):
    """
    Download status enumeration.
    """

    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    DONE = "DONE"


def get_general_logger(logger_name):
    """Get a general logger with the specified name.

    Args:
        logger_name (str): The name of the logger.

    Returns:
        logging.Logger: A logger instance.
    """

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    logger.propagate = False
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter("[%(asctime)-20s] [%(name)-10s] [%(levelname)-6s] %(message)s"))
    logger.addHandler(console_handler)
    return logger


def check_status(endpoint, filename):
    """Check the status of a file download from the specified rs-server endpoint.

    This function sends a GET request to the rs-server endpoint with the filename as a query parameter
    to retrieve the status of the file download. If the response is successful and contains a 'status'
    key in the JSON response, the function returns the corresponding download status enum value. If the
    response is not successful or does not contain the 'status' key, the function returns a FAILED status.

    Args:
        endpoint (str): The rs-server endpoint URL to query for the file status.
        filename (str): The name of the file for which to check the status.

    Returns:
        EDownloadStatus: The download status enum value based on the response from the endpoint.

    """
    # TODO: check the status for a certain timeout if http returns NOK ?
    try:
        response = requests.get(
            endpoint,
            params={"name": filename},
            timeout=ENDPOINT_TIMEOUT,
        )

        eval_response = response.json()
        if (
            response.ok
            and "name" in eval_response.keys()
            and filename == eval_response["name"]
            and "status" in eval_response.keys()
        ):
            return EDownloadStatus(eval_response["status"])

    except requests.exceptions.RequestException as e:
        print(f"Request exception caught when calling for status endpoint: {e}")

    return EDownloadStatus.FAILED


def update_stac_catalog(url: str, user: str, mission: str, stac_file_info: dict):
    # TODO ! Implement this when the catalog PUT endpoint will be ready
    """
    Each time a chunk is downloaded, publish it on catalog (with STAC
    metadata returned in the first step + the file downloaded in S3 bucket)
    RS-Server /catalog/rs-ops/collections/sx_chunk/items/{chunkid}
    """
    catalog_endpoint = url + f"/catalog/{user}/collections/{mission}/items/{stac_file_info}"
    print(f"Endpoint to be used to insert the item info  within the catalog: {catalog_endpoint}")
    # response = requests.put(catalog_endpoint, params=stac_file_info)
    return True


class PrefectCommonConfig:  # pylint: disable=too-few-public-methods
    """Common configuration to Prefect tasks and flows.
    Base class for configuration to prefect tasks and flows that ingest files from different stations (cadip, adgs...)

    Attributes:
        user (str): The user associated with the configuration.
        url (str): The URL for the station.
        station (str): The station identifier.
        mission (str): The mission identifier.
        tmp_download_path (str): The temporary download path.
        s3_path (str): The S3 path for storing downloaded files.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user,
        url,
        station,
        mission,
        tmp_download_path,
        s3_path,
    ):
        self.user: str = user
        self.url: str = url
        self.station: str = station
        self.mission: str = mission
        self.tmp_download_path: str = tmp_download_path
        self.s3_path: str = s3_path


class PrefectTaskConfig(PrefectCommonConfig):  # pylint: disable=too-few-public-methods
    """Configuration for Prefect tasks.

    This class (inherits PrefectCommonConfig) encapsulates the configuration parameters needed for a Prefect task.
    It includes information such as the user, rs-serve endpoint, list of files
    to be downloaded from the station, in STAC format (to be used when insertion to STAC
    endpoint is called), temporary download path (local to where the endpoint is hosted),
    S3 path, and additional parameters like index and maximum retries.

    Attributes:
        task_files_stac (list[dict]): List of files with stac info to be processed by the task.
        idx (int): Index of the task.
        max_retries (int): Maximum number of retries for the task.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user,
        url,
        station,
        mission,
        tmp_download_path,
        s3_path,
        task_files_stac,
        idx,
        max_retries: int = 3,
    ):
        """
        Initialize the PrefectTaskConfig object with provided parameters.
        """

        super().__init__(user, url, station, mission, tmp_download_path, s3_path)
        self.task_files_stac: list[dict] = task_files_stac
        # temp, to be removed in production
        self.idx: int = idx
        self.max_retries: int = max_retries


@task
def ingest_files(config: PrefectTaskConfig):
    """Prefect task function to ingest files.

    This prefect task function access the RS-Server endpoints that start the download of files and
    check the status for the actions

    Args:
        config (PrefectTaskConfig): Configuration object containing details about the files to be downloaded.

    Raises:
        None: This function does not raise any exceptions.

    Returns:
        failed_failes: A list of files which could not be downloaded and / or uploaded to the s3.
    """

    try:
        logger = get_run_logger()
        logger.setLevel(SET_PREFECT_LOGGING_LEVEL)
    except exceptions.MissingContextError:
        logger = get_general_logger(f"task_dwn_{config.idx}")
        logger.info("Could not get the prefect logger due to missing context")

    # some protections for the optional args
    if config.s3_path is None:
        config.s3_path = ""
    if config.tmp_download_path is None:
        config.tmp_download_path = ""
    logger.debug("Task %s: List with requested files to be downloaded: %s\n\n", config.idx, config.task_files_stac)

    endpoint = create_endpoint(config.url, config.station)
    # create the status endpoint for a later usage
    status_endpoint = endpoint + "/status"
    # list with failed files
    downloaded_files_indices = []
    failed_failes = config.task_files_stac.copy()
    # Call the download endpoint for each requested file
    for i, file_stac in enumerate(config.task_files_stac):
        payload = {"name": file_stac["id"]}
        if len(config.tmp_download_path) > 0:
            payload["local"] = config.tmp_download_path
        if len(config.s3_path) > 0:
            payload["obs"] = config.s3_path
        try:
            response = requests.get(endpoint, params=payload, timeout=ENDPOINT_TIMEOUT)
            logger.debug("response = %s", response)
            if not response.ok:
                logger.error(
                    "Task %s: The download endpoint returned error for file %s...\n",
                    config.idx,
                    file_stac["id"],
                )
                continue
        except requests.exceptions.RequestException as e:
            logger.error("Task %s: Request exception caught: %s", config.idx, e)
            continue

        logger.debug("Task %s: response = %s", config.idx, response.ok)

        status = EDownloadStatus.FAILED
        # check the status of the file until is downloaded before requesting the next
        # just for demo, the timeout should be otherwise defined by config
        timeout = DOWNLOAD_FILE_TIMEOUT  # 3 minutes
        while status != EDownloadStatus.DONE and timeout > 0:
            status = check_status(status_endpoint, file_stac["id"])
            logger.info(
                "Task %s: The download progress for file %s is %s",
                config.idx,
                file_stac["id"],
                status.name,
            )
            time.sleep(1)
            timeout -= 1
        if status == EDownloadStatus.DONE:
            logger.info("Task %s: File %s has been properly downloaded...\n", config.idx, file_stac["id"])
            # TODO: call the STAC endpoint to insert it into the catalog !!
            update_stac_catalog(config.url, config.user, config.mission, file_stac)
            downloaded_files_indices.append(i)
        else:
            logger.error(
                "Task %s: Error in downloading the file %s. Timeout was %s from %s\n",
                config.idx,
                file_stac["id"],
                timeout,
                DOWNLOAD_FILE_TIMEOUT,
            )

    logger.debug(f"downloaded_files_indices = {downloaded_files_indices}")
    logger.debug(f"len(failed_failes) = {len(failed_failes)}")
    logger.debug(f"failed_failes = {failed_failes}")
    for idx in sorted(downloaded_files_indices, reverse=True):
        logger.debug(f"idx = {idx}")
        logger.debug(f"failed_failes[idx] = {failed_failes[idx]}")
        del failed_failes[idx]

    return failed_failes


def get_station_files_list(endpoint: str, start_date: datetime, stop_date: datetime):
    """Retrieve a list of files from the specified endpoint within the given time range.

    This function queries the specified endpoint to retrieve a list of files available in the
    station (CADIP, ADGS, LTA ...) within the provided time range, starting from 'start_date' up
    to 'stop_date' (inclusive).

    Args:
        endpoint (str): The URL endpoint to query for file information.
        start_date (datetime): The start date/time of the time range.
        stop_date (datetime, optional): The stop date/time of the time range.

    Returns:
        files: A list of files (in stac format) available at the endpoint within the specified time range.

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
    try:
        response = requests.get(endpoint + "/search", params=payload, timeout=ENDPOINT_TIMEOUT)
    except requests.exceptions.RequestException as e:
        print(f"Request exception caught: {e}")
        raise RuntimeError("Could not connect to the search endpoint") from e
    print(f"response.link = {response.url}")
    files = []
    try:
        if response.ok:
            for file_info in response.json()["features"]:
                files.append(file_info)
    except KeyError as e:
        print("No 'features' key in the dictionary response from the search endpoint")
        raise RuntimeError("Wrong format of search endpoint answer") from e

    return files


def create_endpoint(url, station):
    """Create a rs-server endpoint URL based on the provided base URL and station type.

    This function constructs and returns a specific endpoint URL based on the provided
    base URL and the type of station. The supported station types are "ADGS" and "CADIP" for the time being
    For other station types, a RuntimeError is raised.

    Args:
        url (str): The base URL to which the station-specific path will be appended.
        station (str): The type of station for which the endpoint is being created. Supported
            values are "ADGS" and "CADIP".

    Returns:
        str: The constructed endpoint URL.

    Raises:
        RuntimeError: If the provided station type is not supported.

    Notes:
        - This function constructs a station-specific endpoint URL by appending a path
          based on the station type to the provided base URL.
        - For "ADGS" stations, the endpoint path is "/adgs/aux/".
        - For "CADIP" stations, the endpoint path is "/cadip/CADIP/cadu/".
        - If an unsupported station type is provided, a RuntimeError is raised.

    """
    # url = http://127.0.0.1:8000
    match station:
        case "ADGS":
            return url.rstrip("/") + "/adgs/aux"
        case "CADIP":
            return url.rstrip("/") + "/cadip/CADIP/cadu"
        case _:
            print("Unkown station")
            raise RuntimeError("Unknown station !")


class PrefectFlowConfig(PrefectCommonConfig):  # pylint: disable=too-few-public-methods
    """Configuration class for Prefect flow.

    This class inherits the PrefectCommonCongig and represents the configuration for a
    Prefect flow

    Attributes:
        max_workers (int): The maximum number of workers for the Prefect flow.
        start_datetime (datetime): The start datetime of the files that the station should return
        stop_datetime (datetime): The stop datetime of the files that the station should return
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user,
        url,
        station,
        mission,
        tmp_download_path,
        s3_path,
        max_workers,
        start_datetime,
        stop_datetime,
    ):
        """
        Initialize the PrefectFlowConfig object with provided parameters.
        """
        super().__init__(user, url, station, mission, tmp_download_path, s3_path)
        self.max_workers: int = max_workers
        self.start_datetime: datetime = start_datetime
        self.stop_datetime: datetime = stop_datetime


@flow(task_runner=DaskTaskRunner())
def download_flow(config: PrefectFlowConfig):
    """Prefect flow for downloading files from a station.

    This flow orchestrates the download process by obtaining the list of files from the search endpoint (provided
    station), splitting the list into tasks based on the number of workers, and submitting tasks for ingestion.

    Args:
        config (PrefectFlowConfig): Configuration object containing details about the download flow.

    Returns:
        bool: True if the flow execution is successful, False otherwise.

    Raises:
        None: This function does not raise any exceptions.
    """
    # get the Prefect logger
    try:
        logger = get_run_logger()
        logger.setLevel(SET_PREFECT_LOGGING_LEVEL)
    except exceptions.MissingContextError:
        logger = get_general_logger("flow_dwn")
        logger.info("Could not get the prefect logger due to missing context")

    try:
        endpoint = create_endpoint(config.url, config.station)

        # get the list with files from the search endpoint
        files_stac = get_station_files_list(endpoint, config.start_datetime, config.stop_datetime)

        # distribute the filenames evenly in a number of lists equal with
        # the minimum between number of runners and files to be downloaded
        try:
            tasks_files_stac = [
                x.tolist() for x in [*np.array_split(files_stac, min(config.max_workers, len(files_stac)))]
            ]
            logger.debug(f"type(tasks_files_stac) = {type(tasks_files_stac)}")
        except ValueError:
            logger.warning("No task will be started, the requested number of tasks is 0 !")
            tasks_files_stac = []

        logger.info("List with files found in station = %s\n\n", files_stac)
        idx = 0
        for files_stac in tasks_files_stac:
            ingest_files.submit(
                PrefectTaskConfig(
                    config.user,
                    config.url,
                    config.station,
                    config.mission,
                    config.tmp_download_path,
                    config.s3_path,
                    files_stac,
                    idx,
                ),
            )
            idx += 1
    except RuntimeError as e:
        logger.error("Exception caught: %s", e)
        return False

    return True
