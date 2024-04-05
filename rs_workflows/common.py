"""Docstring will be here."""
import enum
import logging

# import pprint
import sys
import time
from datetime import datetime

import numpy as np
import requests
from prefect import exceptions, flow, get_run_logger, task
from prefect_dask.task_runners import DaskTaskRunner

CADIP = ["CADIP", "INS", "MPS", "MTI", "NSG", "SGS"]
ADGS = "ADGS"

DOWNLOAD_FILE_TIMEOUT = 180  # in seconds
SET_PREFECT_LOGGING_LEVEL = "DEBUG"
ENDPOINT_TIMEOUT = 2  # in seconds
SEARCH_ENDPOINT_TIMEOUT = 60  # in seconds
CATALOG_REQUEST_TIMEOUT = 20  # in seconds

RSPY_APIKEY = "RSPY_APIKEY"


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


def create_apikey_headers(apikey):
    """Create the apikey

    This function creates the apikey headers used when calling the endpoints. This may be empty
    Args:
        apikey_headers (dict): A dictionary with the apikey
    """
    apikey_headers = {}
    if apikey:
        apikey_headers = {"headers": {"x-api-key": apikey}}

    return apikey_headers


def check_status(apikey_headers, endpoint, filename, logger):
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
            endpoint,
            params={"name": filename},
            timeout=ENDPOINT_TIMEOUT,
            **apikey_headers,
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
        logger.exception(f"Status endpoint exception: {e}")

    return EDownloadStatus.FAILED


def update_stac_catalog(  # pylint: disable=too-many-arguments
    apikey_headers: dict,
    url: str,
    user: str,
    mission: str,
    stac_file_info: dict,
    obs: str,
    logger,
):
    """Update the STAC catalog with file information.

    Args:
        apikey_headers (dict): The apikey used for request (may be empty)
        url (str): The URL of the catalog.
        user (str): The user identifier.
        mission (str): The mission identifier.
        stac_file_info (dict): Information in stac format about the downloaded file.
        obs (str): The S3 bucket location where the file has been saved.
        logger: The logger object for logging.

    Returns:
        bool: True if the file information is successfully updated in the catalog, False otherwise.
    """
    # add mission
    stac_file_info["collection"] = f"{mission}_aux"
    # add bucket location where the file has been saved
    stac_file_info["assets"]["file"]["href"] = f"{obs.rstrip('/')}/{stac_file_info['id']}"
    # add a fake geometry polygon (the whole globe)
    stac_file_info["geometry"] = {
        "type": "Polygon",
        "coordinates": [
            [
                [180, -90],
                [180, 90],
                [-180, 90],
                [-180, -90],
                [180, -90],
            ],
        ],
    }
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(stac_file_info)

    catalog_endpoint = url.rstrip("/") + f"/catalog/collections/{user}:{mission}_aux/items/"
    try:
        response = requests.post(
            catalog_endpoint,
            json=stac_file_info,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **apikey_headers,
        )
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Request exception caught: %s", e)
        return False
    try:
        if not response.ok:
            logger.error(f"The catalog update endpoint: {response.json()}")
    except requests.exceptions.JSONDecodeError:
        logger.exception(f"Could not get the json body from response: {response}")
    return response.status_code == 200


class PrefectCommonConfig:  # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """Common configuration to Prefect tasks and flows.
    Base class for configuration to prefect tasks and flows that ingest files from different stations (cadip, adgs...)

    Attributes:
        user (str): The user associated with the configuration.
        url (str): The URL for the endpoints that handle the station (search, download, status).
        url_catalog (str): The URL for the endpoints that handle the catalog (search, publish).
        station (str): The station identifier.
        mission (str): The mission identifier.
        tmp_download_path (str): The temporary download path.
        s3_path (str): The S3 path for storing downloaded files.
        apikey (str): The api key used when calling the RS Server endpoints
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user,
        url,
        url_catalog,
        station,
        mission,
        tmp_download_path,
        s3_path,
        apikey,
    ):
        self.user: str = user
        self.url: str = url
        self.url_catalog: str = url_catalog
        self.station: str = station
        self.mission: str = mission
        self.tmp_download_path: str = tmp_download_path
        self.s3_path: str = s3_path
        self.apikey: str = apikey


class PrefectTaskConfig(PrefectCommonConfig):  # pylint: disable=too-few-public-methods
    """Configuration for Prefect tasks.

    This class (inherits PrefectCommonConfig) encapsulates the configuration parameters needed for a Prefect task.
    It includes information such as the user, rs-serve endpoint, list of files
    to be downloaded from the station, in STAC format (to be used when insertion to STAC
    endpoint is called), temporary download path (local to where the endpoint is hosted),
    S3 path, and additional parameters like index and maximum retries.

    Attributes:
        task_files_stac (list[dict]): List of files with stac info to be processed by the task.
        max_retries (int): Maximum number of retries for the task.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user,
        url,
        url_catalog,
        station,
        mission,
        tmp_download_path,
        s3_path,
        apikey,
        task_files_stac,
        max_retries: int = 3,
    ):
        """
        Initialize the PrefectTaskConfig object with provided parameters.
        """

        super().__init__(user, url, url_catalog, station, mission, tmp_download_path, s3_path, apikey)
        self.task_files_stac: list[dict] = task_files_stac
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
        logger = get_general_logger("task_dwn")
        logger.info("Could not get the prefect logger due to missing context")

    # dictionary to be used for payload request
    payload = {}
    # some protections for the optional args
    if config.s3_path is not None and len(config.s3_path) > 0:
        payload["obs"] = config.s3_path
    if config.tmp_download_path is not None and len(config.tmp_download_path) > 0:
        payload["local"] = config.tmp_download_path
    # logger.debug("Files to be downloaded:")
    # pp = pprint.PrettyPrinter(indent=4)
    # for f in config.task_files_stac:
    #    pp.pprint(f)
    # sys.stdout.flush()

    # get the endpoint
    endpoint = create_endpoint(config.url, config.station)
    # create the apikey_headers
    apikey_headers = create_apikey_headers(config.apikey)
    # list with failed files
    downloaded_files_indices = []
    failed_failes = config.task_files_stac.copy()
    # Call the download endpoint for each requested file
    for i, file_stac in enumerate(config.task_files_stac):
        # update the filename to be ingested
        payload["name"] = file_stac["id"]
        try:
            logger.debug(f"Calling  {endpoint} with payload {payload}")
            start_p = datetime.now()
            response = requests.get(endpoint, params=payload, timeout=ENDPOINT_TIMEOUT, **apikey_headers)
            logger.debug(f"Download start endpoint returned in {(datetime.now() - start_p)}")
            if not response.ok:
                logger.error(
                    "The download endpoint returned error for file %s...\n",
                    file_stac["id"],
                )
                continue
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logger.exception("Request exception caught: %s", e)
            continue

        # monitor the status of the file until it is completely downloaded before initiating the next download request
        status = check_status(apikey_headers, endpoint + "/status", file_stac["id"], logger)
        # just for the demo the timeout is hardcoded, it should be otherwise defined somewhere in the configuration
        timeout = DOWNLOAD_FILE_TIMEOUT  # 3 minutes
        while status in [EDownloadStatus.NOT_STARTED, EDownloadStatus.IN_PROGRESS] and timeout > 0:
            logger.info(
                "The download progress for file %s is %s",
                file_stac["id"],
                status.name,
            )
            time.sleep(1)
            timeout -= 1
            status = check_status(apikey_headers, endpoint + "/status", file_stac["id"], logger)
        if status == EDownloadStatus.DONE:
            logger.info("File %s has been properly downloaded...", file_stac["id"])
            # TODO: call the STAC endpoint to insert it into the catalog !!
            if update_stac_catalog(
                apikey_headers,
                config.url_catalog,
                config.user,
                config.mission,
                file_stac,
                config.s3_path,
                logger,
            ):
                logger.info(f"File well published: {file_stac['id']}\n")
                # save the index of the well ingested file
                downloaded_files_indices.append(i)
            else:
                logger.error(f"Could not publish file: {file_stac['id']}")
        else:
            logger.error(
                "Error in downloading the file %s (status %s). Timeout was %s from %s\n",
                file_stac["id"],
                status.name,
                timeout,
                DOWNLOAD_FILE_TIMEOUT,
            )
    # remove all the well ingested files
    # return only those that failed
    for idx in sorted(downloaded_files_indices, reverse=True):
        del failed_failes[idx]

    return failed_failes


def filter_unpublished_files(  # pylint: disable=too-many-arguments
    apikey_headers: dict,
    url_catalog: str,
    user: str,
    mission: str,
    files_stac: list,
    logger,
):
    """Check for unpublished files in the catalog.

    Args:
        apikey_headers (dict): The apikey used for request (may be empty)
        url_catalog (str): The URL of the catalog.
        user (str): The user identifier.
        mission (str): The mission identifier.
        files_stac (list): List of files to be checked for publication.
        logger: The logger object for logging.

    Returns:
        list: List of files that are not yet published in the catalog.
    """

    ids = []
    for fs in files_stac:
        ids.append(str(fs["id"]))
    catalog_endpoint = url_catalog.rstrip("/") + "/catalog/search"
    request_params = {"collection": f"{mission}_aux", "ids": ",".join(ids), "filter": f"owner_id='{user}'"}
    try:
        response = requests.get(
            catalog_endpoint,
            params=request_params,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **apikey_headers,
        )
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Request exception caught: %s", e)
        # try to ingest everything anyway
        return

    # try to ingest everything anyway
    if response.status_code != 200:
        return
    try:
        eval_response = response.json()
    except requests.exceptions.JSONDecodeError:
        # content is empty, try to ingest everything anyway
        return

    # no file in the catalog
    if eval_response["features"] is None:
        return

    # logger.debug(f"Files found in the catalog ({len(eval_response['features'])}): {eval_response['features']} ")
    for feature in eval_response["features"]:
        for fs in files_stac:
            if feature["id"] == fs["id"]:
                files_stac.remove(fs)
                break


def get_station_files_list(apikey_headers: dict, endpoint: str, start_date: datetime, stop_date: datetime, logger):
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
    try:
        response = requests.get(endpoint + "/search", params=payload, timeout=SEARCH_ENDPOINT_TIMEOUT, **apikey_headers)
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception(f"Could not get the response from the station search endpoint: {e}")
        raise RuntimeError("Could not get the response from the station search endpoint") from e

    files = []
    try:
        if response.ok:
            for file_info in response.json()["features"]:
                files.append(file_info)
        else:
            logger.error(f"Error: {response.status_code} : {response.json()}")
    except KeyError as e:
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
    if station == ADGS:
        return url.rstrip("/") + "/adgs/aux"
    if station in CADIP:
        return url.rstrip("/") + f"/cadip/{station}/cadu"
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
        url_catalog,
        station,
        mission,
        tmp_download_path,
        s3_path,
        apikey,
        max_workers,
        start_datetime,
        stop_datetime,
    ):
        """
        Initialize the PrefectFlowConfig object with provided parameters.
        """
        super().__init__(user, url, url_catalog, station, mission, tmp_download_path, s3_path, apikey)
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
        # get the endpoint
        endpoint = create_endpoint(config.url, config.station)
        # create the apikey_headers
        apikey_headers = create_apikey_headers(config.apikey)

        # get the list with files from the search endpoint
        files_stac = get_station_files_list(
            apikey_headers,
            endpoint,
            config.start_datetime,
            config.stop_datetime,
            logger,
        )
        # check if the list with files returned from the station is not empty
        if len(files_stac) == 0:
            logger.warning(
                f"The station {config.station} did not return any \
element for time interval {config.start_datetime} - {config.stop_datetime}",
            )
            return True
        # filter those that are already existing
        filter_unpublished_files(
            apikey_headers,
            config.url_catalog,
            config.user,
            config.mission,
            files_stac,
            logger,
        )

        # distribute the filenames evenly in a number of lists equal with
        # the minimum between number of runners and files to be downloaded
        try:
            tasks_files_stac = [
                x.tolist() for x in [*np.array_split(files_stac, min(config.max_workers, len(files_stac)))]
            ]
        except ValueError:
            logger.warning("No task will be started, the requested number of tasks is 0 !")
            tasks_files_stac = []
        logger.info("List with files to be downloaded (after filtering against the catalog)")
        for f in files_stac:
            logger.info("      %s", f["id"])

        for files_stac in tasks_files_stac:
            ingest_files.submit(
                PrefectTaskConfig(
                    config.user,
                    config.url,
                    config.url_catalog,
                    config.station,
                    config.mission,
                    config.tmp_download_path,
                    config.s3_path,
                    config.apikey,
                    files_stac,
                ),
            )

    except (RuntimeError, TypeError) as e:
        logger.error("Exception caught: %s", e)
        return False

    return True
