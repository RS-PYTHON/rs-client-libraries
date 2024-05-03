"""Unit tests for staging (cadip/adgs) prefect flow"""
import json
import os.path as osp
import urllib
from datetime import datetime
from pathlib import Path

# import yaml
import pytest
import responses

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_common.config import ECadipStation, EDownloadStatus, EPlatform
from rs_common.logging import Logging
from rs_workflows.staging import (
    PrefectFlowConfig,
    PrefectTaskConfig,
    create_collection_name,
    filter_unpublished_files,
    staging,
    staging_flow,
    update_stac_catalog,
)

RESOURCES = Path(osp.realpath(osp.dirname(__file__))) / "resources"
MISSION_NAME = "s1"

ADGS = "ADGS"
CADIP = "CADIP"

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
        ("CADU_PRODUCT_TEST.tst", CADIP),
        ("ADGS_PRODUCT_TEST.tst", ADGS),
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
    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"
    timeout = 3  # seconds

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "test_user", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "test_user", ECadipStation.CADIP, [EPlatform.S1A], logger)
    endpoint = href + endpoints[station]["status"]
    json_response = {"name": filename, "status": EDownloadStatus.NOT_STARTED}

    responses.add(
        responses.GET,
        endpoint + f"?name={filename}",
        json=json_response,
        status=200,
    )

    assert rs_client.check_status(filename, timeout) == EDownloadStatus.NOT_STARTED

    json_response["status"] = EDownloadStatus.IN_PROGRESS
    responses.add(
        responses.GET,
        endpoint,
        json=json_response,
        status=200,
    )
    assert rs_client.check_status(filename, timeout) == EDownloadStatus.IN_PROGRESS

    json_response["status"] = EDownloadStatus.DONE
    responses.add(
        responses.GET,
        endpoint,
        json=json_response,
        status=200,
    )
    assert rs_client.check_status(filename, timeout) == EDownloadStatus.DONE


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "filename, station",
    [
        ("CADU_PRODUCT_TEST.tst", CADIP),
        ("ADGS_PRODUCT_TEST.tst", ADGS),
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
    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"
    timeout = 3  # seconds

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "test_user", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "test_user", ECadipStation.CADIP, [EPlatform.S1A], logger)
    endpoint = href + endpoints[station]["status"]

    json_response = {"detail": "Not Found"}
    responses.add(
        responses.GET,
        endpoint + f"?name={filename}",
        json=json_response,
        status=404,
    )

    assert rs_client.check_status(filename, timeout) == EDownloadStatus.FAILED


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "response_is_valid, station",
    [
        (True, "ADGS"),
        (False, "ADGS"),
        (True, "CADIP"),
        (False, "CADIP"),
    ],
)
def test_update_stac_catalog(response_is_valid, station):
    """Test the update_stac_catalog function.

    It uses responses library to mock HTTP responses and
    parametrize to test different scenarios with varying response validity and station.
    Args:
        response_is_valid (bool): Flag indicating whether the response should be valid.
        station (str): The station for which to test the function.

    Raises:
        AssertionError: If the response from update_stac_catalog does not match the expected validity.

    Returns:
        None
    """

    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "testUser", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "testUser", ECadipStation.CADIP, [EPlatform.S1A], logger)

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())

    # set the response status
    response_status = 200 if response_is_valid else 400
    # mock the publish to catalog endpoint
    collection_name = create_collection_name(rs_client, MISSION_NAME)
    responses.add(
        responses.POST,
        f"{href}/catalog/collections/testUser:{collection_name}/items/",
        status=response_status,
    )

    for file_s in files_stac[station]["features"]:
        resp = update_stac_catalog.fn(rs_client, collection_name, file_s, "s3://tmp_bucket/tmp")
        assert resp == response_is_valid


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station, mock_files_in_catalog",
    [
        ("ADGS", {"numberReturned": 0, "features": []}),
        (
            "ADGS",
            {
                "numberReturned": 1,
                "features": [
                    {
                        "id": "S1__AUX_WND_V20190117T120000_G20190117T063216.SAFE.zip",
                    },
                ],
            },
        ),
        (
            "ADGS",
            {
                "numberReturned": 2,
                "features": [
                    {
                        "id": "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ",
                    },
                    {
                        "id": "S1__AUX_WND_V20190117T120000_G20190117T063216.SAFE.zip",
                    },
                ],
            },
        ),
        ("CADIP", {"numberReturned": 0, "features": []}),
        (
            "CADIP",
            {
                "numberReturned": 1,
                "features": [
                    {
                        "id": "DCS_04_S1A_20231126151600051390_ch1_DSDB_00026.raw",
                    },
                ],
            },
        ),
        (
            "CADIP",
            {
                "numberReturned": 2,
                "features": [
                    {
                        "id": "DCS_04_S1A_20231126151600051390_ch2_DSDB_00001.raw",
                    },
                    {
                        "id": "DCS_04_S1A_20231126151600051390_ch1_DSDB_00026.raw",
                    },
                ],
            },
        ),
    ],
)
def test_filter_unpublished_files(station, mock_files_in_catalog):
    """Test the filter_unpublished_files function.

    Args:
        station (str): The station for which to test the function.
        mock_files_in_catalog (dict): Mocked response containing files from the catalog.

    Raises:
        AssertionError: If the length of filtered files_stac does not match the expected length after filtering.
        AssertionError: If any of the file IDs from the mock response is found in the filtered files_stac.

    Returns:
        None
    """

    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "testUser", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "testUser", ECadipStation.CADIP, [EPlatform.S1A], logger)

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())[station]["features"]

    initial_len = len(files_stac)

    # get ids from the expected response
    file_ids = []
    for fs in files_stac:
        file_ids.append(fs["id"])

    collection_name = create_collection_name(rs_client, MISSION_NAME)

    request_params = {"collection": collection_name, "ids": ",".join(file_ids), "filter": "owner_id='testUser'"}

    # mock the publish to catalog endpoint
    endpoint = f"{href}/catalog/search?" + urllib.parse.urlencode(request_params)

    responses.add(
        responses.GET,
        endpoint,
        json=mock_files_in_catalog,
        status=200,
    )
    logger = Logging.default(__name__)

    files_stac = filter_unpublished_files.fn(
        rs_client,
        collection_name,
        files_stac,
    )

    logger.debug(f"AFTER filtering ! FS = {files_stac} || ex = {mock_files_in_catalog}")

    assert len(files_stac) == initial_len - mock_files_in_catalog["numberReturned"]
    file_ids = []
    for fs in files_stac:
        file_ids.append(fs["id"])

    for fn in mock_files_in_catalog["features"]:
        assert fn["id"] not in file_ids


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        ADGS,
    ],
)
def test_ok_staging(station):
    """Unit test for the staging function in case of successful files ingestion.

    This test validates the behavior of the staging function when successfully ingesting files
    from the station, resulting in an empty list of returned failed files.

    Args:
        station (str): The station type for which files are being ingested.

    Raises:
        AssertionError: If the number of returned files is not zero. Otherwise, it means that
        ingestion failed for some of the files

    Returns:
        None: This test does not return any value.
    """

    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())
    local_path_for_dwn = f"./temporary/{station}"
    obs = "s3://test/tmp"

    for i in range(0, len(files_stac[station]["features"])):
        # mock the status endpoint
        fn = files_stac[station]["features"][i]["id"]
        endpoint = href + endpoints[station]["status"] + f"?name={fn}"
        json_response = {"name": fn, "status": EDownloadStatus.DONE}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )
        # mock the download endpoint

        endpoint = (
            href + endpoints[station]["download"] + f"?name={fn}&" + f"local={local_path_for_dwn}&" + f"obs={obs}"
        )
        json_response = {"started": "true"}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "testUser", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "testUser", ECadipStation.CADIP, [EPlatform.S1A], logger)

    # mock the publish to catalog endpoint
    collection_name = create_collection_name(rs_client, MISSION_NAME)
    endpoint = f"{href}/catalog/collections/testUser:{collection_name}/items/"
    responses.add(
        responses.POST,
        endpoint,
        status=200,
    )
    task_config = PrefectTaskConfig(
        rs_client,
        MISSION_NAME,
        local_path_for_dwn,
        obs,
        files_stac[station]["features"],
        1,
    )
    ret_files = staging.fn(task_config)
    assert len(ret_files) == 0


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        ADGS,
    ],
)
def test_nok_staging(station):
    """Unit test for the staging function in case of failed file ingestion.

    This test validates the behavior of the staging function when file ingestion
    fails for some files, resulting in a non-empty list of returned files.

    Args:
        station (str): The station type for which files are being ingested.

    Raises:
        AssertionError: If the number of returned files is not as expected.

    Returns:
        None: This test does not return any value.
    """

    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())
    local_path_for_dwn = f"./temporary/{station}"
    obs = "s3://test/tmp"

    # mock the status endpoint
    for i in range(0, len(files_stac[station]["features"])):
        fn = files_stac[station]["features"][i]["id"]
        endpoint = (
            href + endpoints[station]["download"] + f"?name={fn}&" + f"local={local_path_for_dwn}&" + f"obs={obs}"
        )
        json_response = {"started": "false"}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=503,
        )

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "testUser", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "testUser", ECadipStation.CADIP, [EPlatform.S1A], logger)

    # mock the publish to catalog endpoint
    collection_name = create_collection_name(rs_client, MISSION_NAME)
    endpoint = f"{href}/catalog/collections/testUser:{collection_name}/items/"
    responses.add(
        responses.POST,
        endpoint,
        status=200,
    )
    task_config = PrefectTaskConfig(
        rs_client,
        MISSION_NAME,
        local_path_for_dwn,
        obs,
        files_stac[station]["features"],
        1,
    )
    ret_files = staging.fn(task_config)
    assert len(ret_files) == 2


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        ADGS,
    ],
)
def test_search_stations(station):
    """Unit test for the search_stations function.

    This test validates the behavior of the search_stations function when fetching
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

    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"
    timeout = 3  # seconds

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "test_user", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "test_user", ECadipStation.CADIP, [EPlatform.S1A], logger)

    # mock the search endpoint
    endpoint = href + endpoints[station]["search"]

    json_response = files_stac[station]
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z",
        json=json_response,
        status=200,
    )

    search_response = rs_client.search_stations(
        datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
        timeout,
    )
    assert len(search_response) == 2
    del files_stac[station]["features"][1]
    json_response = files_stac[station]
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z&limit=1",
        json=json_response,
        status=200,
    )
    search_response = rs_client.search_stations(
        datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
        timeout,
        1,
    )
    assert len(search_response) == 1


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        ADGS,
    ],
)
def test_err_ret_search_stations(station):
    """Unit test for the search_stations function in case of error response.

    This test validates the behavior of the search_stations function in erroneous situations:
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

    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"
    timeout = 3  # seconds

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "test_user", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "test_user", ECadipStation.CADIP, [EPlatform.S1A], logger)

    # mock the search endpoint
    endpoint = href + endpoints[station]["search"]

    # register a mock with an error answer
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z&limit=2",
        json={"detail": "Operational error"},
        status=400,
    )
    logger = Logging.default(__name__)
    search_response = rs_client.search_stations(
        datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
        timeout,
        2,
    )
    assert len(search_response) == 0

    # register a mock with a bad format answer
    json_response = files_stac[station]
    # change the features key to something else
    json_response["unk_features"] = json_response.pop("features")
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z&limit=2",
        json={"detail": "Operational error"},
        status=200,
    )

    with pytest.raises(RuntimeError) as runtime_exception:
        search_response = rs_client.search_stations(
            datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
            datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
            timeout,
            2,
        )
    assert "Wrong format of search endpoint answer" in str(runtime_exception.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        ADGS,
    ],
)
def test_wrong_url_search_stations(station):
    """Unit test for the search_stations function in case of wrong endpoint URL.

    This test validates the behavior of the search_stations function when providing
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
    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"
    bad_href = "http://127.0.0.1:6000"
    timeout = 3  # seconds

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(bad_href, None, "testUser", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(bad_href, None, "testUser", ECadipStation.CADIP, [EPlatform.S1A], logger)

    # mock the search endpoint
    endpoint = href + endpoints[station]["search"]

    json_response = files_stac[station]
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z&limit=2",
        json=json_response,
        status=200,
    )

    # use a wrong endpoint
    with pytest.raises(RuntimeError) as runtime_exception:
        rs_client.search_stations(
            datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
            datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
            timeout,
            2,
        )
    assert "Could not get the response from the station search endpoint" in str(runtime_exception.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    ["CADIP", "ADGS", "UNKNOWN"],
)
def test_create_collection_name(station):
    """Unit test for the create_collection_name function.

    This test validates the behavior of the create_collection_name function when creating
    an the name of the collection to be used based on the mission name (currently, only s1) and
    on the station name

    Args:
        station (str): The station type for which the collection name is created

    Raises:
        AssertionError: If the expected result does not match the actual result.
        If a RuntimeError exception is not raised in case of an Unknown station

    Returns:
        None: This test does not return any value.
    """

    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "testUser", [EPlatform.S1A], logger)
    elif station == CADIP:
        rs_client = CadipClient(href, None, "testUser", ECadipStation.CADIP, [EPlatform.S1A], logger)
    else:
        rs_client = None

    if station == "UNKNOWN":
        with pytest.raises(RuntimeError) as runtime_exception:
            create_collection_name(rs_client, MISSION_NAME)
        assert "Unknown station !" in str(runtime_exception.value)
    else:
        assert (
            create_collection_name(rs_client, MISSION_NAME) == MISSION_NAME + "_aux" if station == "ADGS" else "_chunk"
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
def test_staging_flow(station):
    """Unit test for the staging_flow function.

    This test validates the behavior of the staging_flow function prefect flow when ingests
    files from the station.

    Args:
        station (str): The station type for which files are being ingested.

    Raises:
        AssertionError: If the return value of staging_flow is not True.

    Returns:
        None: This test does not return any value.
    """

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())
    local_path_for_dwn = f"./temporary/{station}"
    obs = "s3://test/tmp"
    href = "http://127.0.0.1:5000"
    logger = Logging.default(__name__)

    # mock the search endpoint
    endpoint = href + endpoints[station]["search"]

    json_response = files_stac[station]
    responses.add(
        responses.GET,
        endpoint + "?datetime=2014-01-01T00:00:00Z/2024-02-02T23:59:59Z",
        json=json_response,
        status=200,
    )

    # mock the catalog search endpoint
    endpoint = f"{href}/catalog/search?"
    # get filenames
    file_ids = []
    for fs in files_stac[station]["features"]:
        file_ids.append(fs["id"])
    # set the collection name
    collection_name = "s1_aux" if station == "ADGS" else "s1_chunk"
    request_params = {"collection": collection_name, "ids": ",".join(file_ids), "filter": "owner_id='testUser'"}
    endpoint = endpoint + urllib.parse.urlencode(request_params)
    responses.add(
        responses.GET,
        endpoint,
        status=200,
    )

    for fn in file_ids:
        # mock the status endpoint
        # fn = files_stac[station]["features"][i]["id"]
        endpoint = href + endpoints[station]["status"] + f"?name={fn}"
        json_response = {"name": fn, "status": EDownloadStatus.DONE}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )
        # mock the download endpoint
        endpoint = (
            href + endpoints[station]["download"] + f"?name={fn}&" + f"local={local_path_for_dwn}&" + f"obs={obs}"
        )
        json_response = {"started": "true"}
        responses.add(
            responses.GET,
            endpoint,
            json=json_response,
            status=200,
        )

    rs_client: AuxipClient | CadipClient | None = None
    if station == ADGS:
        rs_client = AuxipClient(href, None, "testUser", [EPlatform.S1A], logger)
    else:
        rs_client = CadipClient(href, None, "testUser", ECadipStation.CADIP, [EPlatform.S1A], logger)

    flow_config = PrefectFlowConfig(
        rs_client,
        MISSION_NAME,
        local_path_for_dwn,
        obs,
        0,
        datetime.strptime("2014-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime("2024-02-02T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    assert staging_flow(flow_config) is True