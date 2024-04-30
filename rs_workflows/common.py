"""Common workflows for general usage"""

import time
from datetime import datetime
from typing import Any

import numpy as np
import requests
from prefect import exceptions, flow, get_run_logger, task
from prefect_dask.task_runners import DaskTaskRunner

from rs_client.rs_client import (
    ADGS,
    CADIP,
    AuxipClient,
    CadipClient,
    EDownloadStatus,
    RsClient,
)
from rs_workflows.utils.logging import Logging

DOWNLOAD_FILE_TIMEOUT = 180  # in seconds
SET_PREFECT_LOGGING_LEVEL = "DEBUG"
ENDPOINT_TIMEOUT = 10  # in seconds
SEARCH_ENDPOINT_TIMEOUT = 60  # in seconds
CATALOG_REQUEST_TIMEOUT = 20  # in seconds


def get_prefect_logger(general_logger_name):
    """Get theprefect logger.
    It returns the prefect logger. If this can't be taken due to the missing
    prefect context (i.e. the flow/task is run as single function, from tests for example),
    the general logger is returned

    Args:
        general_logger_name (str): The name of the general logger in case the prefect logger can't be returned.

    Returns:
        logging.Logger: A prefect logger instance or a general logger instance.
    """
    try:
        logger = get_run_logger()
        logger.setLevel(SET_PREFECT_LOGGING_LEVEL)
    except exceptions.MissingContextError:
        logger = Logging.default(general_logger_name)
        logger.warning("Could not get the prefect logger due to missing context. Using the general one")
    return logger


@task
def update_stac_catalog(  # pylint: disable=too-many-arguments
    apikey_headers: dict,
    url: str,
    user: str,
    collection_name: str,
    stac_file_info: dict,
    obs: str,
    logger,
):
    """Update the STAC catalog with file information.

    Args:
        apikey_headers (dict): The apikey used for request (may be empty)
        url (str): The URL of the catalog.
        user (str): The user identifier.
        collection_name (str): The name of the collection to be used.
        stac_file_info (dict): Information in stac format about the downloaded file.
        obs (str): The S3 bucket location where the file has been saved.
        logger: The logger object for logging.

    Returns:
        bool: True if the file information is successfully updated in the catalog, False otherwise.
    """
    # add collection name
    stac_file_info["collection"] = collection_name
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

    catalog_endpoint = url.rstrip("/") + f"/catalog/collections/{user}:{collection_name}/items/"
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


class PrefectCommonConfig:  # pylint: disable=too-few-public-methods, too-many-instance-attributes,
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
def staging_files(config: PrefectTaskConfig):
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

    logger = get_prefect_logger("task_dwn")
    # list with failed files
    failed_failes = config.task_files_stac.copy()
    try:
        rs_client = get_rs_client(
            config.apikey,
            config.url,
            config.url_catalog,
            config.user,
            config.station,
            config.mission,
            logger,
        )
    except RuntimeError as e:
        logger.exception(f"Could not get the RsClient object. Reason: {e}")
        return failed_failes

    # create the apikey_headers
    apikey_headers = RsClient.create_apikey_headers(config.apikey)

    downloaded_files_indices = []

    # Call the download endpoint for each requested file
    for i, file_stac in enumerate(config.task_files_stac):
        # update the filename to be ingested
        try:
            rs_client.staging_file(file_stac["id"], config.s3_path, config.tmp_download_path, ENDPOINT_TIMEOUT)
        except RuntimeError as e:
            # TODO: Continue? Stop ?
            logger.exception(f"Could not stage file %s. Exception: {e}")
            continue

        # monitor the status of the file until it is completely downloaded before initiating the next download request
        status = rs_client.check_status(file_stac["id"], ENDPOINT_TIMEOUT)
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
            status = rs_client.check_status(file_stac["id"], ENDPOINT_TIMEOUT)
        if status == EDownloadStatus.DONE:
            logger.info("File %s has been properly downloaded...", file_stac["id"])
            # TODO: either move the code from filter_unpublished_files to RsClient
            # or use the new PgstacClient ?
            if update_stac_catalog.fn(
                apikey_headers,
                config.url_catalog,
                config.user,
                create_collection_name(config.mission, config.station),
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


@task
def filter_unpublished_files(  # pylint: disable=too-many-arguments
    apikey_headers: dict,
    url_catalog: str,
    user: str,
    collection_name: str,
    files_stac: list,
    logger: Any,
) -> list:
    """Check for unpublished files in the catalog.

    Args:
        apikey_headers (dict): The apikey used for request (may be empty)
        url_catalog (str): The URL of the catalog.
        user (str): The user identifier.
        collection_name (str): The name of the collection to be used.
        files_stac (list): List of files (dcitionary for each) to be checked for publication.
        logger: The logger object for logging.

    Returns:
        list: List of files that are not yet published in the catalog.
    """

    ids = []
    # TODO: Should this list be checked for duplicated items?
    for fs in files_stac:
        ids.append(str(fs["id"]))
    catalog_endpoint = url_catalog.rstrip("/") + "/catalog/search"
    request_params = {"collection": collection_name, "ids": ",".join(ids), "filter": f"owner_id='{user}'"}
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
        return files_stac

    # try to ingest everything anyway
    if response.status_code != 200:
        logger.error(f"Quering the catalog endpoint returned status {response.status_code}")
        return files_stac

    try:
        eval_response = response.json()
    except requests.exceptions.JSONDecodeError:
        # content is empty, try to ingest everything anyway
        return files_stac

    # no file in the catalog
    if eval_response["features"] is None:
        return files_stac

    for feature in eval_response["features"]:
        for fs in files_stac:
            if feature["id"] == fs["id"]:
                files_stac.remove(fs)
                break
    return files_stac


def create_endpoint(url, station):
    """Create a rs-server endpoint URL based on the provided base URL and station type.

    This function constructs and returns a specific endpoint URL based on the provided
    base URL and the type of station. For the time being, the supported station types are "ADGS" and
    "CADIP", "INS", "MPS", "MTI", "NSG", "SGS".

    For other values, a RuntimeError is raised.

    Args:
        url (str): The base URL to which the station-specific path will be appended.
        station (str): The type of station for which the endpoint is being created. Supported
            values are "ADGS" and "CADIP", "INS", "MPS", "MTI", "NSG", "SGS".

    Returns:
        str: The constructed endpoint URL.

    Raises:
        RuntimeError: If the provided station type is not supported.

    Notes:
        - This function constructs a station-specific endpoint URL by appending a path
          based on the station type to the provided base URL.
        - For "ADGS" stations, the endpoint path is "/adgs/aux/".
        - For "CADIP" stations, the endpoint path is "/cadip/{station}/cadu/".
        - If an unsupported station type is provided, a RuntimeError is raised.

    """
    if station == ADGS:
        return url.rstrip("/") + "/adgs/aux"
    if station in CADIP:
        return url.rstrip("/") + f"/cadip/{station}/cadu"
    raise RuntimeError("Unknown station !")


def get_rs_client(apikey, url, url_catalog, user, station, mission, logger):
    """Get the RsClient"""
    if station == ADGS:
        return AuxipClient(apikey, url, url_catalog, user, mission, logger)
    return CadipClient(apikey, url, url_catalog, user, station, mission, logger)


def create_collection_name(mission, station):
    """Create the name of the catalog collection

    This function constructs and returns a specific name for the catalog collection .
    For ADGS station type should be "mission_name"_aux
    For CADIP stations type should be "mission_name"_chunk

    For other values, a RuntimeError is raised.

    Args:
        mission (str): The name of the mission.
        station (str): The type of station . Supported
            values are "ADGS" and "CADIP", "INS", "MPS", "MTI", "NSG", "SGS".

    Returns:
        str: The name of the collection

    Raises:
        RuntimeError: If the provided station type is not supported.

    """
    if station == ADGS:
        return f"{mission}_aux"
    if station in CADIP:
        return f"{mission}_chunk"
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
        limit=None,
    ):
        """
        Initialize the PrefectFlowConfig object with provided parameters.
        """
        super().__init__(user, url, url_catalog, station, mission, tmp_download_path, s3_path, apikey)
        self.max_workers: int = max_workers
        self.start_datetime: datetime = start_datetime
        self.stop_datetime: datetime = stop_datetime
        self.limit = limit


@flow(task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 15, "threads_per_worker": 1}))
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
    logger = get_prefect_logger("flow_dwn")
    logger.info(f"The download flow is starting. Received workers:{config.max_workers}")
    try:
        # get the endpoint
        # endpoint = create_endpoint(config.url, config.station)
        try:
            rs_client = get_rs_client(
                config.apikey,
                config.url,
                config.url_catalog,
                config.user,
                config.station,
                config.mission,
                logger,
            )
        except RuntimeError as e:
            logger.exception(f"Could not get the RsClient object. Reason: {e}")
            return False
        # create the apikey_headers
        # apikey_headers = create_apikey_headers(config.apikey)

        # get the list with files from the search endpoint
        try:
            files_stac = rs_client.get_station_files_list(
                config.start_datetime,
                config.stop_datetime,
                SEARCH_ENDPOINT_TIMEOUT,
                config.limit,
            )
        except RuntimeError as e:
            logger.exception(f"Unable to get the list with files for staging: {e}")
            return False

        # check if the list with files returned from the station is not empty
        if len(files_stac) == 0:
            logger.warning(
                f"The station {config.station} did not return any \
element for time interval {config.start_datetime} - {config.stop_datetime}",
            )
            return True
        # create the collection name

        # filter those that are already existing
        # TODO: either move the code from filter_unpublished_files to RsClient
        # or use the new PgstacClient ?
        files_stac = filter_unpublished_files(  # type: ignore
            RsClient.create_apikey_headers(config.apikey),
            config.url_catalog,
            config.user,
            create_collection_name(config.mission, config.station),
            files_stac,
            logger,
            wait_for=[files_stac],
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
            staging_files.submit(
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
