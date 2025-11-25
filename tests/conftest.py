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

"""
https://docs.pytest.org/en/6.2.x/fixture.html#conftest-py-sharing-fixtures-across-multiple-files

The conftest.py file serves as a means of providing fixtures for an entire directory.
Fixtures defined in a conftest.py can be used by any test in that package without needing to import them
(pytest will automatically discover them).
"""
import logging

import pytest
import responses
from prefect.testing.utilities import prefect_test_harness

from rs_client.rs_client import RsClient
from rs_client.stac.stac_base import StacBase
from rs_common.config import EPlatform
from rs_common.utils import env_bool
from rs_workflows import init_pi_db_flow
from tests import common

# Use dummy values
RSPY_UAC_CHECK_URL = "https://www.rspy-uac-manager.com"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
PLATFORMS = [EPlatform.S1A, EPlatform.S2A]


HTTP_OK = 200
HTTP_ERROR = 500
OWNER = "toto"
COLLECTION_ID = "S1_L1"

MOCKED_URL = "https://mocked_stac_catalog_url"
COLLECTION_RESPONSE = {
    "id": COLLECTION_ID,
    "type": "Collection",
    "links": [
        {
            "rel": "items",
            "type": "application/geo+json",
            "href": f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items",
        },
        {
            "rel": "parent",
            "type": "application/json",
            "href": f"{MOCKED_URL}/catalog/catalogs/{OWNER}",
        },
        {
            "rel": "root",
            "type": "application/json",
            "href": f"{MOCKED_URL}/catalog/catalogs/{OWNER}",
        },
        {
            "rel": "self",
            "type": "application/json",
            "href": f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}",
        },
        {
            "rel": "items",
            "href": f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items/",
            "type": "application/geo+json",
        },
        {
            "rel": "license",
            "href": "https://creativecommons.org/licenses/publicdomain/",
            "title": "public domain",
        },
    ],
    "owner": OWNER,
    "extent": {
        "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
        "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
    },
    "license": "public-domain",
    "description": "Some description",
    "stac_version": "1.0.0",
}

ITEM_RESPONSE = {
    "id": "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.EOF",
    "bbox": [-180.0, -90.0, 180.0, 90.0],
    "type": "Feature",
    "links": [
        {
            "rel": "collection",
            "type": "application/json",
            "href": f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}",
        },
        {
            "rel": "parent",
            "type": "application/json",
            "href": f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}",
        },
        {"rel": "root", "type": "application/json", "href": f"{MOCKED_URL}/catalog/"},
        {
            "rel": "self",
            "type": "application/geo+json",
            "href": f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items/"
            "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.EOF",
        },
    ],
    "assets": {},
    "geometry": None,
    "collection": "S1_L1",
    "properties": {
        "owner": OWNER,
        "created": "2024-05-27T09:44:09.509000Z",
        "expires": "2025-03-28T13:07:32.278399Z",
        "updated": "2025-02-26T13:07:32.278399Z",
        "auxip:id": "7158d45f-4a44-4141-a3c9-d82cc8f4c2a0",
        "datetime": "2024-05-27T09:44:09.509000Z",
        "platform": "sentinel-1a",
        "published": "2025-02-26T13:07:32.278394Z",
        "end_datetime": "2024-05-27T09:44:19.509000Z",
        "product:type": "AUX_PP2",
        "constellation": "sentinel-1",
        "start_datetime": "2024-05-27T09:44:09.509000Z",
        "processing:datetime": "2024-05-27T00:00:00.000Z",
        "processing:facility": "FOS",
    },
    "stac_version": "1.1.0",
    "stac_extensions": [
        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
    ],
}


@pytest.fixture(scope="session", autouse=True)
def before_and_after(session_mocker):
    """This function is called before and after all the pytests have started/ended."""

    ####################
    # Before all tests #
    ####################

    # Avoid errors:
    # Transient error StatusCode.UNAVAILABLE encountered while exporting metrics to localhost:4317, retrying in 1s
    session_mocker.patch(  # pylint: disable=protected-access
        "opentelemetry.exporter.otlp.proto.grpc.exporter.OTLPExporterMixin",
    )._export.return_value = True

    yield

    ###################
    # After all tests #
    ###################


@pytest.fixture(scope="function", autouse=True)
def clear_caches():
    """Clear caches at the end of each test"""
    yield
    StacBase.get_collection.cache_clear()  # pylint:disable=no-member


@pytest.fixture(name="mock_prefect", scope="session")
def __mock_prefect():
    """
    Init a mockup prefect server, see: https://docs.prefect.io/v3/how-to-guides/workflows/test-workflows
    """
    # NOTE: this takes long, so for local testing you can comment it,
    # and replace with "docker compose up" from rs-demo and set this env var to "1"
    if env_bool("SKIP_PREFECT_TEST_HARNESS", False):
        yield
    else:
        with prefect_test_harness():
            yield


@pytest.fixture
def mocked_stac_catalog_delete_item():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}")
        resp.get(url=MOCKED_URL + "/catalog/", json=json_landing_page, status=HTTP_OK)

        json_status = {"status": HTTP_OK}
        resp.add(
            "DELETE",
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items/item_0",
            json=json_status,
            status=HTTP_OK,
        )

        yield MOCKED_URL


@pytest.fixture
def mocked_stac_catalog_add_update_item():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}", conforms_to=False)
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)

        json_status = {"status": HTTP_OK}
        resp.add(
            "POST",
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items",
            json=json_status,
            status=HTTP_OK,
        )
        resp.add(
            "PUT",
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items/item_0",
            json=json_status,
            status=HTTP_OK,
        )

        yield MOCKED_URL


@pytest.fixture
def mocked_stac_catalog_delete_collection():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}")
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)

        json_status = {"status": HTTP_OK}
        resp.add(
            "DELETE",
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}",
            json=json_status,
            status=HTTP_OK,
        )

        yield MOCKED_URL


@pytest.fixture
def mocked_stac_catalog_add_update_collection():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}")
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)
        resp.add("POST", url=f"{MOCKED_URL}/catalog/collections", json={"status": HTTP_OK}, status=HTTP_OK)
        resp.add("PUT", url=f"{MOCKED_URL}/catalog/collections/OWNERID:S2_L2", json={"status": HTTP_OK}, status=HTTP_OK)

        yield MOCKED_URL


@pytest.fixture
def mocked_stac_catalog_add_collection_error():
    """Mock error response from a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}")
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)
        resp.add("POST", url=f"{MOCKED_URL}/catalog/collections", json={"status": HTTP_ERROR}, status=HTTP_ERROR)

        yield MOCKED_URL


@pytest.fixture
def mocked_stac_catalog_get_collection():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp:
        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}", conforms_to=True)
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)
        resp.get(
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}",
            json=COLLECTION_RESPONSE,
            status=HTTP_OK,
        )
        resp.get(
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items?collections={COLLECTION_ID}",
            json=COLLECTION_RESPONSE,
            status=HTTP_OK,
        )
        resp.get(url=f"{MOCKED_URL}/catalog/collections", json=COLLECTION_RESPONSE, status=HTTP_OK)

        yield MOCKED_URL


@pytest.fixture
def mocked_stac_catalog_search_inside_collection():
    """Mock responses to a STAC catalog search request."""
    with responses.RequestsMock() as resp:
        url = "http://mocked_stac_catalog_url"
        json_landing_page = common.json_landing_page(url, f"{OWNER}:{COLLECTION_ID}", conforms_to=True)
        resp.get(url=url + "/catalog/", json=json_landing_page, status=HTTP_OK)
        json_search = {
            "type": "FeatureCollection",
            "context": {"limit": 10, "returned": 2},
            "features": [
                {
                    "id": "DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw",
                    "bbox": [-180, -90, 180, 90],
                    "type": "Feature",
                    "links": [
                        {
                            "rel": "collection",
                            "type": "application/json",
                            "href": ("https://dev-rspy.esa-copernicus.eu/catalog/collections/" "toto:S1_L1"),
                        },
                        {
                            "rel": "parent",
                            "type": "application/json",
                            "href": ("https://dev-rspy.esa-copernicus.eu/catalog/collections/" "toto:S1_L1"),
                        },
                        {
                            "rel": "root",
                            "type": "application/json",
                            "href": "https://dev-rspy.esa-copernicus.eu/catalog/catalogs/toto",
                        },
                        {
                            "rel": "self",
                            "type": "application/geo+json",
                            "href": (
                                "https://dev-rspy.esa-copernicus.eu/catalog/collections/"
                                "toto:S1_L1/items/"
                                "DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw"
                            ),
                        },
                    ],
                    "assets": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
                    },
                    "collection": "S1_L1",
                    "properties": {
                        "gsd": 0.12345,
                        "owner": "toto",
                        "width": 2500,
                        "height": 2500,
                        "expires": "2024-08-08T07:12:45.662521Z",
                        "updated": "2024-07-09T07:12:45.662521Z",
                        "datetime": "2024-07-09T07:12:45.459911Z",
                        "proj:epsg": 3857,
                        "published": "2024-07-09T07:12:45.662515Z",
                        "orientation": "nadir",
                    },
                    "stac_version": "1.0.0",
                    "stac_extensions": ["https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json"],
                },
                {
                    "id": "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ",
                    "bbox": [-180, -90, 180, 90],
                    "type": "Feature",
                    "links": [
                        {
                            "rel": "collection",
                            "type": "application/json",
                            "href": ("https://dev-rspy.esa-copernicus.eu/catalog/collections/" "toto:S1_L1"),
                        },
                        {
                            "rel": "parent",
                            "type": "application/json",
                            "href": ("https://dev-rspy.esa-copernicus.eu/catalog/collections/" "toto:S1_L1"),
                        },
                        {
                            "rel": "root",
                            "type": "application/json",
                            "href": "https://dev-rspy.esa-copernicus.eu/catalog/catalogs/toto",
                        },
                        {
                            "rel": "self",
                            "type": "application/geo+json",
                            "href": (
                                "https://dev-rspy.esa-copernicus.eu/catalog/collections/"
                                "toto:S1_L1/items/"
                                "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ"
                            ),
                        },
                    ],
                    "assets": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
                    },
                    "collection": "S1_L1",
                    "properties": {
                        "gsd": 0.12345,
                        "owner": "toto",
                        "width": 2500,
                        "height": 2500,
                        "expires": "2024-08-08T07:12:39.570544Z",
                        "updated": "2024-07-09T07:12:39.570544Z",
                        "datetime": "2024-07-09T07:12:39.081716Z",
                        "proj:epsg": 3857,
                        "published": "2024-07-09T07:12:39.570534Z",
                        "orientation": "nadir",
                    },
                    "stac_version": "1.0.0",
                    "stac_extensions": ["https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json"],
                },
            ],
            "links": [
                {
                    "rel": "collection",
                    "type": "application/json",
                    "href": "https://dev-rspy.esa-copernicus.eu/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": "https://dev-rspy.esa-copernicus.eu/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": "https://dev-rspy.esa-copernicus.eu/catalog/catalogs/toto",
                },
                {
                    "rel": "self",
                    "type": "application/geo+json",
                    "href": ("https://dev-rspy.esa-copernicus.eu/catalog/collections/toto:S1_L1/items"),
                },
            ],
        }

        resp.post(url=url + "/catalog/search", json=json_search, status=HTTP_OK)
        yield url


@pytest.fixture(name="mocked_stac_catalog_url")
def mocked_stac_catalog_url_():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp:
        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}")
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)
        resp.get(url=f"{MOCKED_URL}/auxip/", json=json_landing_page, status=HTTP_OK)
        resp.get(url=f"{MOCKED_URL}/prip/", json=json_landing_page, status=HTTP_OK)
        resp.get(url=f"{MOCKED_URL}/cadip/", json=json_landing_page, status=HTTP_OK)

        yield MOCKED_URL


@pytest.fixture(name="set_db_env_var")
def set_db_env_var_fixture(monkeypatch):
    """Fixture to set different environment variables

    This fixture sets a variety of environment variables

    Args:
        monkeypatch: Pytest utility for temporarily modifying environment variables.
    """
    envvars = {
        "RSPY_HOST_CATALOG": "https://dummy-catalog/catalog/",
        "RSPY_HOST_CADIP": "https://dummy-cadip/cadip/",
        "RSPY_HOST_AUXIP": "https://dummy-auxip/auxip/",
        "RSPY_HOST_PRIP": "https://dummy-prip/prip/",
        "RSPY_HOST_STAGING": "https://dummy-staging/staging/",
    }
    for key, val in envvars.items():
        monkeypatch.setenv(key, val)
    yield  # restore the environment


@pytest.fixture(name="generic_rs_client")
def generic_rs_client_(mocked_stac_catalog_url, monkeypatch):
    """Return a generic RsClient instance for testing."""
    monkeypatch.setenv("RSPY_OAUTH2_COOKIE", "RSPY_OAUTH2_COOKIE")
    yield RsClient(mocked_stac_catalog_url, RS_SERVER_API_KEY, OWNER)  # will be used to test the StacClient


@pytest.fixture(name="auxip_client")
def auxip_client_(generic_rs_client):
    """Return a generic AuxipClient instance for testing."""
    yield generic_rs_client.get_auxip_client()


@pytest.fixture(name="cadip_client")
def cadip_client_(generic_rs_client):
    """Return a generic CadipClient instance for testing."""
    yield generic_rs_client.get_cadip_client()


@pytest.fixture(name="prip_client")
def prip_client_(generic_rs_client):
    """Return a generic PripClient instance for testing."""
    yield generic_rs_client.get_prip_client()


@pytest.fixture(name="stac_client")
def stac_client_(generic_rs_client):
    """Return a generic StacClient instance for testing."""
    yield generic_rs_client.get_catalog_client()


@pytest.fixture
def mocked_stac_catalog_invalid_get_item():
    """Fixture that mock invalid response of catalog from /collections/{collection-id}/items/{item-id}"""
    with responses.RequestsMock(assert_all_requests_are_fired=True) as resp:
        data = {
            "code": "NotFoundError",
            "description": "Item invalid in Collection ovidiu_my_test_collection does not exist.",
        }
        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}")
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)
        resp.get(
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}",
            json=COLLECTION_RESPONSE,
            status=HTTP_OK,
        )
        resp.get(
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items/invalid_item",
            json=data,
            status=404,
        )

        yield MOCKED_URL


@pytest.fixture
def mocked_stac_catalog_get_item():
    """Fixture that mock valid response of catalog from /collections/{collection-id}/items/{item-id}"""
    item_id = "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.EOF"
    with responses.RequestsMock(assert_all_requests_are_fired=True) as resp:
        # Mocked URL
        json_landing_page = common.json_landing_page(MOCKED_URL, f"{OWNER}:{COLLECTION_ID}")
        resp.get(url=f"{MOCKED_URL}/catalog/", json=json_landing_page, status=HTTP_OK)
        resp.get(
            url=f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}",
            json=COLLECTION_RESPONSE,
            status=HTTP_OK,
        )
        url = f"{MOCKED_URL}/catalog/collections/{OWNER}:{COLLECTION_ID}/items/{item_id}"
        resp.get(
            url=url,
            json=ITEM_RESPONSE,
            status=200,
        )

        yield MOCKED_URL


@pytest.fixture(autouse=True, scope="function")
def patch_prefect_logger(monkeypatch):
    """
    Patch Prefect get_run_logger to avoid MissingContextError in tests.
    Replaces Prefect’s logger with a standard Python logger.
    """
    monkeypatch.setattr(init_pi_db_flow, "get_run_logger", lambda: logging.getLogger("test"))
