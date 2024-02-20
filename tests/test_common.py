"""Unit tests for common (cadip/adgs) prefect flow"""
import json
import os.path as osp
from datetime import datetime
from pathlib import Path

# import yaml
import pytest
import responses

from rs_workflows.common import (
    EDownloadStatus,
    PrefectFlowConfig,
    PrefectTaskConfig,
    check_status,
    create_endpoint,
    download_flow,
    get_station_files_list,
    ingest_files,
    get_general_logger,
)

RESOURCES = Path(osp.realpath(osp.dirname(__file__))) / "resources"

endpoints = {
    "CADIP": {
        "search": "/cadip/CADIP/cadu/search",
        "download": "/cadip/CADIP/cadu",
        "status": "/cadip/CADIP/cadu/status",
    },
    "ADGS": {"search": "/adgs/aux/search", "download": "/adgs/aux", "status": "/adgs/aux/status"},
}


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "filename, station",
    [
        ("CADU_PRODUCT_TEST.tst", "CADIP"),
        ("ADGS_PRODUCT_TEST.tst", "ADGS"),
    ],
)
def test_valid_check_status(filename, station):
    """Unit test for the check_status function.

    This test validates the behavior of the check_status function under a valid scenario
    for different station types (CADIP, ADGS....).

    Args:
        filename (str): The name of the file for which the downloading status is to be checked.
        station (str): The station type.

    Raises:
        AssertionError: If any of the assertions fail during the test.

    Returns:
        None: This test does not return any value.
    """
    endpoint = "http://127.0.0.1:5000" + endpoints[station]["status"]
    json_response = {"name": filename, "status": EDownloadStatus.NOT_STARTED}

    responses.add(
        responses.GET,
        endpoint + f"?name={filename}",
        json=json_response,
        status=200,
    )
    logger = get_general_logger("tests")
    assert check_status(endpoint, filename, logger) == EDownloadStatus.NOT_STARTED

    json_response["status"] = EDownloadStatus.IN_PROGRESS
    responses.add(
        responses.GET,
        endpoint,
        json=json_response,
        status=200,
    )
    assert check_status(endpoint, filename, logger) == EDownloadStatus.IN_PROGRESS

    json_response["status"] = EDownloadStatus.DONE
    responses.add(
        responses.GET,
        endpoint,
        json=json_response,
        status=200,
    )
    assert check_status(endpoint, filename, logger) == EDownloadStatus.DONE


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "filename, station",
    [
        ("CADU_PRODUCT_TEST.tst", "CADIP"),
        ("ADGS_PRODUCT_TEST.tst", "ADGS"),
    ],
)
def test_invalid_check_status(filename, station):
    """Unit test for the check_status function in case of invalid response.

    This test validates the behavior of the check_status function when receiving
    an invalid response from the endpoint, resulting in a FAILED status.

    Args:
        filename (str): for which the downloading status is to be checked.
        station (str): The station type.

    Raises:
        AssertionError: If the check_status function does not return EDownloadStatus.FAILED.

    Returns:
        None: This test does not return any value.
    """
    endpoint = "http://127.0.0.1:5000" + endpoints[station]["status"]
    json_response = {"detail": "Not Found"}
    responses.add(
        responses.GET,
        endpoint + f"?name={filename}",
        json=json_response,
        status=404,
    )
    logger = get_general_logger("tests")
    assert check_status(endpoint, filename, logger) == EDownloadStatus.FAILED


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        "CADIP",
        "ADGS",
    ],
)
def test_ok_ingest_files(station):
    """Unit test for the ingest_files function in case of successful files ingestion.

    This test validates the behavior of the ingest_files function when successfully ingesting files
    from the station, resulting in an empty list of returned failed files.

    Args:
        station (str): The station type for which files are being ingested.

    Raises:
        AssertionError: If the number of returned files is not zero. Otherwise, it means that
        ingestion failed for some of the files

    Returns:
        None: This test does not return any value.
    """

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())
    local_path_for_dwn = f"/tmp/{station}"
    obs = "s3://test/tmp"

    for i in range(0, len(files_stac[station]["features"])):
        # mock the status endpoint
        fn = files_stac[station]["features"][i]["id"]
        endpoint = "http://127.0.0.1:5000" + endpoints[station]["status"] + f"?name={fn}"
        json_response = {"name": fn, "status": EDownloadStatus.DONE}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )
        # mock the download endpoint

        endpoint = (
            "http://127.0.0.1:5000"
            + endpoints[station]["download"]
            + f"?name={fn}&"
            + f"local={local_path_for_dwn}&"
            + f"obs={obs}"
        )
        json_response = {"started": "true"}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )

    task_config = PrefectTaskConfig(
        "testUser",
        "http://127.0.0.1:5000",
        station,
        "s1",
        local_path_for_dwn,
        obs,
        files_stac[station]["features"],
        0,
        1,
    )
    ret_files = ingest_files.fn(task_config)
    assert len(ret_files) == 0


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        "CADIP",
        "ADGS",
    ],
)
def test_nok_ingest_files(station):
    """Unit test for the ingest_files function in case of failed file ingestion.

    This test validates the behavior of the ingest_files function when file ingestion
    fails for some files, resulting in a non-empty list of returned files.

    Args:
        station (str): The station type for which files are being ingested.

    Raises:
        AssertionError: If the number of returned files is not as expected.

    Returns:
        None: This test does not return any value.
    """

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())
    local_path_for_dwn = f"/tmp/{station}"
    obs = "s3://test/tmp"

    # mock the status endpoint
    for i in range(0, len(files_stac[station]["features"])):
        fn = files_stac[station]["features"][i]["id"]
        endpoint = (
            "http://127.0.0.1:5000"
            + endpoints[station]["download"]
            + f"?name={fn}&"
            + f"local={local_path_for_dwn}&"
            + f"obs={obs}"
        )
        json_response = {"started": "false"}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=503,
        )
    task_config = PrefectTaskConfig(
        "testUser",
        "http://127.0.0.1:5000",
        station,
        "s1",
        local_path_for_dwn,
        obs,
        files_stac[station]["features"],
        0,
        1,
    )
    ret_files = ingest_files.fn(task_config)
    assert len(ret_files) == 2


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        "CADIP",
        "ADGS",
    ],
)
def test_get_station_files_list(station):
    """Unit test for the get_station_files_list function.

    This test validates the behavior of the get_station_files_list function when fetching
    the list of files from the search endpoint of station.

    Args:
        station (str): The station type for which files are being fetched.

    Raises:
        AssertionError: If the length of the returned files list is not as expected.

    Returns:
        None: This test does not return any value.
    """

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())

    # mock the search endpoint
    endpoint = "http://127.0.0.1:5000" + endpoints[station]["search"]

    json_response = files_stac[station]
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z",
        json=json_response,
        status=200,
    )

    search_response = get_station_files_list(
        endpoint.rstrip("/search"),
        datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
    )

    assert len(search_response) == 2


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        "CADIP",
        "ADGS",
    ],
)
def test_err_ret_get_station_files_list(station):
    """Unit test for the get_station_files_list function in case of error response.

    This test validates the behavior of the get_station_files_list function in erroneous situations:
    - when receiving an error response from the search endpoint of station, with status 400
    - when receiving a bad format answer, even if the status is ok.

    Args:
        station (str): The station type for which files are being fetched.

    Raises:
        AssertionError:
        - If the length of the returned files list is not 0.
        - If a RuntimeError is not raised in case of the bad format answer

    Returns:
        None: This test does not return any value.
    """

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())

    # mock the search endpoint
    endpoint = "http://127.0.0.1:5000" + endpoints[station]["search"]

    # register a mock with an error answer
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z",
        json={"detail": "Operational error"},
        status=400,
    )

    search_response = get_station_files_list(
        endpoint.rstrip("/search"),
        datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    assert len(search_response) == 0

    # register a mock with a bad format answer
    json_response = files_stac[station]
    # change the features key to something else
    json_response["unk_features"] = json_response.pop("features")
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z",
        json={"detail": "Operational error"},
        status=200,
    )

    with pytest.raises(RuntimeError) as runtime_exception:
        search_response = get_station_files_list(
            endpoint.rstrip("/search"),
            datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
            datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
        )
    assert "Wrong format of search endpoint answer" in str(runtime_exception.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        "CADIP",
        "ADGS",
    ],
)
def test_wrong_url_get_station_files_list(station):
    """Unit test for the get_station_files_list function in case of wrong endpoint URL.

    This test validates the behavior of the get_station_files_list function when providing
    a wrong endpoint URL, which should raise a RuntimeError.

    Args:
        station (str): The station type for which files are being fetched.

    Raises:
        AssertionError: If the RuntimeError is not raised as expected.

    Returns:
        None: This test does not return any value.
    """

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())

    # mock the search endpoint
    endpoint = "http://127.0.0.1:5000" + endpoints[station]["search"]

    json_response = files_stac[station]
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z",
        json=json_response,
        status=200,
    )

    # use a wrong endpoint
    with pytest.raises(RuntimeError) as runtime_exception:
        get_station_files_list(
            "http://127.0.0.1:5000/search",
            datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
            datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
        )
    assert "Could not connect to the search endpoint" in str(runtime_exception.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    ["CADIP", "ADGS", "UNKNOWN"],
)
def test_create_endpoint(station):
    """Unit test for the create_endpoint function.

    This test validates the behavior of the create_endpoint function when creating
    an endpoint URL for different station types, including an unknown one.

    Args:
        station (str): The station type for which the endpoint URL is being created.

    Raises:
        AssertionError: If the expected result does not match the actual result.
        If a RuntimeError exception is not raised in case of an Unknown station

    Returns:
        None: This test does not return any value.
    """

    if station == "UNKNOWN":
        with pytest.raises(RuntimeError) as runtime_exception:
            create_endpoint("http://127.0.0.1:5000", station)
        assert "Unknown station !" in str(runtime_exception.value)
    else:
        assert (
            create_endpoint("http://127.0.0.1:5000", station)
            == "http://127.0.0.1:5000" + endpoints[station]["download"]
        )


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        "CADIP",
        "ADGS",
    ],
)
def test_download_flow(station):
    """Unit test for the download_flow function.

    This test validates the behavior of the download_flow function prefect flow when ingests
    files from the station.

    Args:
        station (str): The station type for which files are being ingested.

    Raises:
        AssertionError: If the return value of download_flow is not True.

    Returns:
        None: This test does not return any value.
    """

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())
    local_path_for_dwn = f"/tmp/{station}"
    obs = "s3://test/tmp"

    # mock the search endpoint
    endpoint = "http://127.0.0.1:5000" + endpoints[station]["search"]

    json_response = files_stac[station]
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z",
        json=json_response,
        status=200,
    )

    for i in range(0, len(files_stac[station]["features"])):
        # mock the status endpoint
        fn = files_stac[station]["features"][i]["id"]
        endpoint = "http://127.0.0.1:5000" + endpoints[station]["status"] + f"?name={fn}"
        json_response = {"name": fn, "status": EDownloadStatus.DONE}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )
        # mock the download endpoint
        endpoint = (
            "http://127.0.0.1:5000"
            + endpoints[station]["download"]
            + f"?name={fn}&"
            + f"local={local_path_for_dwn}&"
            + f"obs={obs}"
        )
        json_response = {"started": "true"}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )

    flow_config = PrefectFlowConfig(
        "testUser",
        "http://127.0.0.1:5000",
        station,
        "s1",
        local_path_for_dwn,
        obs,
        0,
        datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    assert download_flow(flow_config) is True
