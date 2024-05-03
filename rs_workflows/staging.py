"""Common workflows for general usage"""

import time
from datetime import datetime
from typing import Any

import numpy as np
import requests
from prefect import exceptions, flow, get_run_logger, task
from prefect_dask.task_runners import DaskTaskRunner

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_client.rs_client import RsClient
from rs_common.config import EDownloadStatus
from rs_common.logging import Logging

DOWNLOAD_FILE_TIMEOUT = 180  # in seconds
SET_PREFECT_LOGGING_LEVEL = "DEBUG"
ENDPOINT_TIMEOUT = 10  # in seconds
SEARCH_ENDPOINT_TIMEOUT = 60  # in seconds
CATALOG_REQUEST_TIMEOUT = 20  # in seconds


def get_prefect_logger(general_logger_name):
    """Get the prefect logger.
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
    rs_client: RsClient,
    collection_name: str,
    stac_file_info: dict,
    obs: str,
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

    catalog_endpoint = (
        rs_client.hostname_for("catalog") + f"/catalog/collections/{rs_client.owner_id}:{collection_name}/items/"
    )
    try:
        response = requests.post(
            catalog_endpoint,
            json=stac_file_info,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **rs_client.apikey_headers,
        )
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        rs_client.logger.exception("Request exception caught: %s", e)
        return False
    try:
        if not response.ok:
            rs_client.logger.error(f"The catalog update endpoint: {response.json()}")
    except requests.exceptions.JSONDecodeError:
        rs_client.logger.exception(f"Could not get the json body from response: {response}")
    return response.status_code == 200


class PrefectCommonConfig:  # pylint: disable=too-few-public-methods, too-many-instance-attributes,
    """Common configuration to Prefect tasks and flows.
    Base class for configuration to prefect tasks and flows that ingest files from different stations (cadip, adgs...)

    Attributes:
        rs_client (AuxipClient | CadipClient): RsClient instance
        mission (str): The mission identifier.
        tmp_download_path (str): The temporary download path.
        s3_path (str): The S3 path for storing downloaded files.
    """

    def __init__(
        self,
        rs_client: AuxipClient | CadipClient,
        mission,
        tmp_download_path,
        s3_path,
    ):
        self.rs_client: AuxipClient | CadipClient = rs_client
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
        max_retries (int): Maximum number of retries for the task.
    """

    def __init__(
        self,
        rs_client: AuxipClient | CadipClient,
        mission,
        tmp_download_path,
        s3_path,
        task_files_stac,
        max_retries: int = 3,
    ):
        """
        Initialize the PrefectTaskConfig object with provided parameters.
        """

        super().__init__(rs_client, mission, tmp_download_path, s3_path)
        self.task_files_stac: list[dict] = task_files_stac
        self.max_retries: int = max_retries


@task
def staging(config: PrefectTaskConfig):
    """Prefect task function to stage (=download/ingest) files.

    This prefect task function access the RS-Server endpoints that start the download of files and
    check the status for the actions

    Args:
        config (PrefectTaskConfig): Configuration object containing details about the files to be downloaded.

    Raises:
        None: This function does not raise any exceptions.

    Returns:
        failed_files: A list of files which could not be downloaded and / or uploaded to the s3.
    """

    logger = get_prefect_logger("task_dwn")
    rs_client = config.rs_client
    # list with failed files
    failed_files = config.task_files_stac.copy()

    downloaded_files_indices = []

    # Call the download endpoint for each requested file
    for i, file_stac in enumerate(config.task_files_stac):
        # update the filename to be ingested
        try:
            rs_client.staging(file_stac["id"], config.s3_path, config.tmp_download_path, ENDPOINT_TIMEOUT)
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
                rs_client,
                create_collection_name(rs_client, config.mission),
                file_stac,
                config.s3_path,
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
        del failed_files[idx]

    return failed_files


@task
def filter_unpublished_files(
    rs_client: RsClient,
    collection_name: str,
    files_stac: list,
) -> list:
    """Check for unpublished files in the catalog.

    Args:
        rs_client (RsClient): RsClient instance.
        collection_name (str): The name of the collection to be used.
        files_stac (list): List of files (dcitionary for each) to be checked for publication.

    Returns:
        list: List of files that are not yet published in the catalog.
    """

    ids = []
    # TODO: Should this list be checked for duplicated items?
    for fs in files_stac:
        ids.append(str(fs["id"]))
    catalog_endpoint = rs_client.hostname_for("catalog") + "/catalog/search"
    request_params = {"collection": collection_name, "ids": ",".join(ids), "filter": f"owner_id='{rs_client.owner_id}'"}
    try:
        response = requests.get(
            catalog_endpoint,
            params=request_params,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **rs_client.apikey_headers,
        )
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        rs_client.logger.exception("Request exception caught: %s", e)
        # try to ingest everything anyway
        return files_stac

    # try to ingest everything anyway
    if response.status_code != 200:
        rs_client.logger.error(f"Quering the catalog endpoint returned status {response.status_code}")
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


def create_collection_name(rs_client, mission):
    """Create the name of the catalog collection

    This function constructs and returns a specific name for the catalog collection .
    For ADGS station type should be "mission_name"_aux
    For CADIP stations type should be "mission_name"_chunk

    For other values, a RuntimeError is raised.

    Args:
        rs_client (AuxipClient | CadipClient | type): RsClient instance or type.
        mission (str): The name of the mission.

    Returns:
        str: The name of the collection

    Raises:
        RuntimeError: If the provided station type is not supported.

    """
    if (rs_client == AuxipClient) or isinstance(rs_client, AuxipClient):
        return f"{mission}_aux"
    if (rs_client == CadipClient) or isinstance(rs_client, CadipClient):
        return f"{mission}_chunk"
    raise RuntimeError("Unknown station !")


class PrefectFlowConfig(PrefectCommonConfig):  # pylint: disable=too-few-public-methods
    """Configuration class for Prefect flow.

    This class inherits the PrefectCommonConfig and represents the configuration for a
    Prefect flow

    Attributes:
        max_workers (int): The maximum number of workers for the Prefect flow.
        start_datetime (datetime): The start datetime of the files that the station should return
        stop_datetime (datetime): The stop datetime of the files that the station should return
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        rs_client: AuxipClient | CadipClient,
        mission,
        tmp_download_path,
        s3_path,
        max_workers,
        start_datetime,
        stop_datetime,
        limit=None,
    ):
        """
        Initialize the PrefectFlowConfig object with provided parameters.
        """
        super().__init__(rs_client, mission, tmp_download_path, s3_path)
        self.max_workers: int = max_workers
        self.start_datetime: datetime = start_datetime
        self.stop_datetime: datetime = stop_datetime
        self.limit = limit


@flow(task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 15, "threads_per_worker": 1}))
def staging_flow(config: PrefectFlowConfig):
    """Prefect flow for staging (=download/ingest) files from a station.

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
    logger.info(f"The staging flow is starting. Received workers:{config.max_workers}")
    try:
        rs_client = config.rs_client

        # get the list with files from the search endpoint
        try:
            files_stac = rs_client.search_stations(
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
                f"The station {rs_client.station_name()} did not return any \
element for time interval {config.start_datetime} - {config.stop_datetime}",
            )
            return True
        # create the collection name

        # filter those that are already existing
        # TODO: either move the code from filter_unpublished_files to RsClient
        # or use the new PgstacClient ?
        files_stac = filter_unpublished_files(  # type: ignore
            rs_client,
            create_collection_name(rs_client, config.mission),
            files_stac,
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
            staging.submit(
                PrefectTaskConfig(
                    config.rs_client,
                    config.mission,
                    config.tmp_download_path,
                    config.s3_path,
                    files_stac,
                ),
            )

    except (RuntimeError, TypeError) as e:
        logger.error("Exception caught: %s", e)
        return False

    return True