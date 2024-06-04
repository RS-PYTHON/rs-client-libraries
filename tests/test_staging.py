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

"""Unit tests for staging (cadip/adgs) prefect flow"""
import json
import os.path as osp
from datetime import datetime
from pathlib import Path

# import yaml
import pytest
import responses

from rs_client.auxip_client import AuxipClient
from rs_client.cadip_client import CadipClient
from rs_client.rs_client import RsClient
from rs_common.config import DATETIME_FORMAT, ECadipStation, EDownloadStatus
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
from tests import common

RESOURCES = Path(osp.realpath(osp.dirname(__file__))) / "resources"
MISSION_NAME = "s1"
API_KEY = "dummy-api-key"

AUXIP = "AUXIP"
CADIP = "CADIP"

endpoints = {
    "CADIP": {
        "search": "/cadip/CADIP/cadu/search",
        "download": "/cadip/CADIP/cadu",
        "status": "/cadip/CADIP/cadu/status",
    },
    "AUXIP": {"search": "/adgs/aux/search", "download": "/adgs/aux", "status": "/adgs/aux/status"},
}


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "filename, station",
    [
        ("CADU_PRODUCT_TEST.tst", CADIP),
        ("ADGS_PRODUCT_TEST.tst", AUXIP),
    ],
)
def test_valid_staging_status(filename, station):
    """Unit test for the staging_status function.

    This test validates the behavior of the staging_status function under a valid scenario
    for different station types (CADIP, AUXIP....).

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
    if station == AUXIP:
        rs_client = AuxipClient(href, API_KEY, "test_user", logger)
    else:
        rs_client = CadipClient(href, API_KEY, "test_user", ECadipStation.CADIP, logger)
    endpoint = href + endpoints[station]["status"]
    json_response = {"name": filename, "status": EDownloadStatus.NOT_STARTED}

    responses.add(
        responses.GET,
        endpoint + f"?name={filename}",
        json=json_response,
        status=200,
    )

    assert rs_client.staging_status(filename, timeout=timeout) == EDownloadStatus.NOT_STARTED

    json_response["status"] = EDownloadStatus.IN_PROGRESS
    responses.add(
        responses.GET,
        endpoint,
        json=json_response,
        status=200,
    )
    assert rs_client.staging_status(filename, timeout=timeout) == EDownloadStatus.IN_PROGRESS

    json_response["status"] = EDownloadStatus.DONE
    responses.add(
        responses.GET,
        endpoint,
        json=json_response,
        status=200,
    )
    assert rs_client.staging_status(filename, timeout=timeout) == EDownloadStatus.DONE


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "filename, station",
    [
        ("CADU_PRODUCT_TEST.tst", CADIP),
        ("ADGS_PRODUCT_TEST.tst", AUXIP),
    ],
)
def test_invalid_staging_status(filename, station):
    """Unit test for the staging_status function in case of invalid response.

    This test validates the behavior of the staging_status function when receiving
    an invalid response from the endpoint, resulting in a FAILED status.

    Args:
        filename (str): for which the downloading status is to be checked.
        station (str): The station type.

    Raises:
        AssertionError: If the staging_status function does not return EDownloadStatus.FAILED.

    Returns:
        None: This test does not return any value.
    """
    logger = Logging.default(__name__)
    href = "http://127.0.0.1:5000"
    timeout = 3  # seconds

    rs_client: AuxipClient | CadipClient | None = None
    if station == AUXIP:
        rs_client = AuxipClient(href, API_KEY, "test_user", logger)
    else:
        rs_client = CadipClient(href, API_KEY, "test_user", ECadipStation.CADIP, logger)
    endpoint = href + endpoints[station]["status"]

    json_response = {"detail": "Not Found"}
    responses.add(
        responses.GET,
        endpoint + f"?name={filename}",
        json=json_response,
        status=404,
    )

    assert rs_client.staging_status(filename, timeout=timeout) == EDownloadStatus.FAILED


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "response_is_valid, station",
    [
        (True, "AUXIP"),
        (False, "AUXIP"),
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

    json_landing_page = common.json_landing_page(href, "toto:S1_L1")
    responses.get(url=href + "/catalog/", json=json_landing_page, status=200)
    responses.get(url=href + "/catalog/catalogs/testUser", json=json_landing_page, status=200)

    json_single_collection = {
        "id": "s1_aux",
        "type": "Collection",
        "links": [
            {
                "rel": "items",
                "type": "application/geo+json",
                "href": f"{href}/catalog/collections/testUser:s1_aux/items",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{href}/catalog/catalogs/testUser",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": f"{href}/catalog/catalogs/testUser",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": f"{href}/catalog/collections/testUser:s1_aux",
            },
            {
                "rel": "items",
                "href": "http://localhost:8082/catalog/collections/testUser:s1_aux/items/",
                "type": "application/geo+json",
            },
            {
                "rel": "license",
                "href": "https://creativecommons.org/licenses/publicdomain/",
                "title": "public domain",
            },
        ],
        "owner": "testUser",
        "extent": {
            "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
            "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
        },
        "license": "public-domain",
        "description": "Some description",
        "stac_version": "1.0.0",
    }
    responses.get(url=href + "/catalog/collections/testUser:s1_aux", json=json_single_collection, status=200)
    responses.get(url=href + "/catalog/collections/testUser:s1_chunk", json=json_single_collection, status=200)

    rs_client = RsClient(href, API_KEY, "testUser", logger).get_stac_client()

    files_stac_path = RESOURCES / "files_stac.json"
    with open(files_stac_path, encoding="utf-8") as files_stac_f:
        files_stac = json.loads(files_stac_f.read())

    # set the response status
    response_status = 200 if response_is_valid else 400
    # mock the publish to catalog endpoint
    collection_name = create_collection_name(MISSION_NAME, station)
    responses.add(
        responses.POST,
        f"{href}/catalog/collections/testUser:{collection_name}/items",
        status=response_status,
    )

    for file_s in files_stac[station]["features"]:
        resp = update_stac_catalog.fn(rs_client, collection_name, file_s, "s3://tmp_bucket/tmp")
        assert resp == response_is_valid


@pytest.mark.unit
@pytest.mark.parametrize(
    "station, mock_files_in_catalog",
    [
        ("AUXIP", {"numberReturned": 0, "features": []}),
        (
            "AUXIP",
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
            "AUXIP",
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
    href = "http://mocked_stac_catalog_url"

    with responses.RequestsMock() as resp:
        json_landing_page = common.json_landing_page(href, "toto:S1_L1")
        resp.get(url=href + "/catalog/", json=json_landing_page, status=200)

        rs_client = RsClient(href, API_KEY, "testUser", logger).get_stac_client()

        files_stac_path = RESOURCES / "files_stac.json"
        with open(files_stac_path, encoding="utf-8") as files_stac_f:
            files_stac = json.loads(files_stac_f.read())[station]["features"]

        initial_len = len(files_stac)

        # get ids from the expected response
        file_ids = []
        for fs in files_stac:
            file_ids.append(fs["id"])

        collection_name = create_collection_name(MISSION_NAME, station)

        resp.post(url=href + "/catalog/search", json=mock_files_in_catalog, status=200)
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
        AUXIP,
    ],
)
def test_ok_staging(station):  # pylint: disable=too-many-locals
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

    json_landing_page = common.json_landing_page(href, "testUser:s1_aux")
    responses.add(responses.GET, url=href + "/catalog/", json=json_landing_page, status=200)
    responses.add(responses.GET, url=href + "/catalog/catalogs/testUser", json=json_landing_page, status=200)

    json_single_collection = {
        "id": "s1_aux",
        "type": "Collection",
        "links": [
            {
                "rel": "items",
                "type": "application/geo+json",
                "href": f"{href}/catalog/collections/testUser:s1_aux/items",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{href}/catalog/catalogs/testUser",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": f"{href}/catalog/catalogs/testUser",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": f"{href}/catalog/collections/testUser:s1_aux",
            },
            {
                "rel": "items",
                "href": "http://localhost:8082/catalog/collections/testUser:s1_aux/items/",
                "type": "application/geo+json",
            },
            {
                "rel": "license",
                "href": "https://creativecommons.org/licenses/publicdomain/",
                "title": "public domain",
            },
        ],
        "owner": "testUser",
        "extent": {
            "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
            "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
        },
        "license": "public-domain",
        "description": "Some description",
        "stac_version": "1.0.0",
    }
    responses.get(url=href + "/catalog/collections/testUser:s1_aux", json=json_single_collection, status=200)
    responses.get(url=href + "/catalog/collections/testUser:s1_chunk", json=json_single_collection, status=200)

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
    if station == AUXIP:
        rs_client = AuxipClient(href, API_KEY, "testUser", logger)
    else:
        rs_client = CadipClient(href, API_KEY, "testUser", ECadipStation.CADIP, logger)

    # mock the publish to catalog endpoint
    collection_name = create_collection_name(MISSION_NAME, rs_client.station_name)
    json_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ",
        "properties": {
            "datetime": "2024-06-03T15:37:20.511121Z",
            "start_datetime": "2019-02-17T09:00:00.000000Z",
            "end_datetime": "2019-01-17T15:00:00.000000Z",
            "adgs:id": "c2136f16-b482-11ee-a8fe-fa163e7968e5",
        },
        "geometry": {"type": "Polygon", "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]]},
        "links": [
            {
                "rel": "root",
                "href": "http://127.0.0.1:5000/catalog/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
            {
                "rel": "parent",
                "href": "http://127.0.0.1:5000/catalog/collections/testUser:s1_aux",
                "type": "application/json",
            },
            {
                "rel": "self",
                "href": (
                    "http://127.0.0.1:5000/catalog/collections/testUser:s1_aux/items/"
                    "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ"
                ),
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://127.0.0.1:5000/catalog/collections/testUser:s1_aux",
                "type": "application/json",
            },
        ],
        "assets": {
            "file": {
                "href": "s3://test/tmp/S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ",
            },
        },
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "stac_extensions": [],
        "collection": "s1_aux",
    }
    endpoint = f"{href}/catalog/collections/testUser:{collection_name}/items"
    responses.add(
        responses.POST,
        url=endpoint,
        json=json_item,
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
        AUXIP,
    ],
)
def test_nok_staging(station):  # pylint: disable=too-many-locals
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
    if station == AUXIP:
        rs_client = AuxipClient(href, API_KEY, "testUser", logger)
    else:
        rs_client = CadipClient(href, API_KEY, "testUser", ECadipStation.CADIP, logger)

    # mock the publish to catalog endpoint
    collection_name = create_collection_name(MISSION_NAME, rs_client.station_name)
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
        AUXIP,
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
    if station == AUXIP:
        rs_client = AuxipClient(href, API_KEY, "test_user", logger)
    else:
        rs_client = CadipClient(href, API_KEY, "test_user", ECadipStation.CADIP, logger)

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
        datetime.strptime("2014-01-01T00:00:00Z", DATETIME_FORMAT),
        datetime.strptime("2024-02-02T23:59:59Z", DATETIME_FORMAT),
        timeout=timeout,
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
        datetime.strptime("2014-01-01T00:00:00Z", DATETIME_FORMAT),
        datetime.strptime("2024-02-02T23:59:59Z", DATETIME_FORMAT),
        1,
        timeout=timeout,
    )
    assert len(search_response) == 1


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        AUXIP,
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
    if station == AUXIP:
        rs_client = AuxipClient(href, API_KEY, "test_user", logger)
    else:
        rs_client = CadipClient(href, API_KEY, "test_user", ECadipStation.CADIP, logger)

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
        datetime.strptime("2014-01-01T00:00:00Z", DATETIME_FORMAT),
        datetime.strptime("2024-02-02T23:59:59Z", DATETIME_FORMAT),
        2,
        timeout=timeout,
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
            datetime.strptime("2014-01-01T00:00:00Z", DATETIME_FORMAT),
            datetime.strptime("2024-02-02T23:59:59Z", DATETIME_FORMAT),
            2,
            timeout=timeout,
        )
    assert "Wrong format of search endpoint answer" in str(runtime_exception.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        CADIP,
        AUXIP,
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
    if station == AUXIP:
        rs_client = AuxipClient(bad_href, API_KEY, "testUser", logger)
    else:
        rs_client = CadipClient(bad_href, API_KEY, "testUser", ECadipStation.CADIP, logger)

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
            datetime.strptime("2014-01-01T00:00:00Z", DATETIME_FORMAT),
            datetime.strptime("2024-02-02T23:59:59Z", DATETIME_FORMAT),
            2,
            timeout=timeout,
        )
    assert "Could not get the response from the station search endpoint" in str(runtime_exception.value)


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    ["CADIP", "AUXIP", "UNKNOWN"],
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

    if station == "UNKNOWN":
        with pytest.raises(RuntimeError) as runtime_exception:
            create_collection_name(MISSION_NAME, station)
        assert "Unknown station" in str(runtime_exception.value)
    else:
        assert (
            create_collection_name(MISSION_NAME, station) == MISSION_NAME + "_aux" if station == "AUXIP" else "_chunk"
        )


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "station",
    [
        "CADIP",
        "AUXIP",
    ],
)
def test_staging_flow(station):  # pylint: disable=too-many-locals
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

    json_landing_page = common.json_landing_page(href, "toto:S1_L1")
    responses.get(url=href + "/catalog/", json=json_landing_page, status=200)

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
    collection_name = "s1_aux" if station == "AUXIP" else "s1_chunk"
    request_params = {"collection": collection_name, "ids": ",".join(file_ids), "filter": "owner_id='testUser'"}
    responses.add(responses.POST, endpoint, status=200, json=request_params)

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
    if station == AUXIP:
        rs_client = AuxipClient(href, API_KEY, "testUser", logger)
    else:
        rs_client = CadipClient(href, API_KEY, "testUser", ECadipStation.CADIP, logger)

    flow_config = PrefectFlowConfig(
        rs_client,
        MISSION_NAME,
        local_path_for_dwn,
        obs,
        0,
        datetime.strptime("2014-01-01T00:00:00Z", DATETIME_FORMAT),
        datetime.strptime("2024-02-02T23:59:59Z", DATETIME_FORMAT),
    )
    assert staging_flow(flow_config) is True
