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

import pytest
import responses

from rs_client.rs_client import RsClient
from rs_common.config import EPlatform
from tests import common

# Use dummy values
RSPY_UAC_CHECK_URL = "http://www.rspy-uac-manager.com"
RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = "OWNER_ID"
CADIP_STATION = "CADIP"
ADGS_STATION = "ADGS"
PLATFORMS = [EPlatform.S1A, EPlatform.S2A]


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


@pytest.fixture
def mocked_stac_catalog_delete_item():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1")
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        json_status = {"status": "200"}
        resp.add("DELETE", url=url + "/catalog/collections/toto:S1_L1/items/item_0", json=json_status, status=200)

        yield url


@pytest.fixture
def mocked_stac_catalog_add_item():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1", conforms_to=False)
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        json_single_collection = {
            "id": "S1_L1",
            "type": "Collection",
            "links": [
                {
                    "rel": "items",
                    "type": "application/geo+json",
                    "href": f"{url}/catalog/collections/toto:S1_L1/items",
                },
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{url}/catalog/catalogs/toto",
                },
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": f"{url}/catalog/catalogs/toto",
                },
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "items",
                    "href": "http://localhost:8082/catalog/collections/toto:S1_L1/items/",
                    "type": "application/geo+json",
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/publicdomain/",
                    "title": "public domain",
                },
            ],
            "owner": "toto",
            "extent": {
                "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
                "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
            },
            "license": "public-domain",
            "description": "Some description",
            "stac_version": "1.0.0",
        }
        resp.get(url=url + "/catalog/collections/toto:S1_L1", json=json_single_collection, status=200)

        json_status = {"status": "200"}
        resp.add("POST", url=url + "/catalog/collections/toto:S1_L1/items", json=json_status, status=200)

        yield url


@pytest.fixture
def mocked_stac_catalog_delete_collection():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1")
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        json_status = {"status": "200"}
        resp.add("DELETE", url=url + "/catalog/collections/toto:S1_L1", json=json_status, status=200)

        yield url


@pytest.fixture
def mocked_stac_catalog_add_collection():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1")
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        json_status = {"status": "200"}
        resp.add("POST", url=url + "/catalog/collections", json=json_status, status=200)

        yield url


@pytest.fixture
def mocked_stac_catalog_get_collection():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1", conforms_to=True)
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        json_single_collection = {
            "id": "S1_L1",
            "type": "Collection",
            "links": [
                {
                    "rel": "items",
                    "type": "application/geo+json",
                    "href": f"{url}/catalog/collections/toto:S1_L1/items",
                },
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{url}/catalog/catalogs/toto",
                },
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": f"{url}/catalog/catalogs/toto",
                },
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "items",
                    "href": "http://localhost:8082/catalog/collections/toto:S1_L1/items/",
                    "type": "application/geo+json",
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/publicdomain/",
                    "title": "public domain",
                },
            ],
            "owner": "toto",
            "extent": {
                "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
                "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
            },
            "license": "public-domain",
            "description": "Some description",
            "stac_version": "1.0.0",
        }
        resp.get(url=url + "/catalog/collections/toto:S1_L1", json=json_single_collection, status=200)
        resp.get(
            url=url + "/catalog/collections/toto:S1_L1/items?collections=S1_L1",
            json=json_single_collection,
            status=200,
        )
        resp.get(url=url + "/catalog/collections", json=json_single_collection, status=200)

        yield url


@pytest.fixture
def mocked_stac_catalog_search_inside_collection():
    """Mock responses to a STAC catalog search request."""
    with responses.RequestsMock() as resp:
        url = "http://mocked_stac_catalog_url"
        json_landing_page = common.json_landing_page(url, "toto:S1_L1", conforms_to=True)
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)
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
                    "assets": {
                        "DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw": {
                            "href": (
                                "https://dev-rspy.esa-copernicus.eu/catalog/collections/"
                                "toto:S1_L1/items/"
                                "DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw/download/file"
                            ),
                            "alternate": {
                                "s3": {
                                    "href": (
                                        "s3://rs-cluster-catalog/toto/CADIP/"
                                        "DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw"
                                    ),
                                },
                            },
                        },
                    },
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
                    "assets": {
                        "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ": {
                            "href": (
                                "https://dev-rspy.esa-copernicus.eu/catalog/collections/"
                                "toto:S1_L1/items/"
                                "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ/"
                                "download/file"
                            ),
                            "alternate": {
                                "s3": {
                                    "href": (
                                        "s3://rs-cluster-catalog/toto/AUXIP/"
                                        "S2__OPER_AUX_ECMWFD_PDMC_20190216T120000_V20190217T090000_20190217T210000.TGZ"
                                    ),
                                },
                            },
                        },
                    },
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

        resp.post(url=url + "/catalog/search", json=json_search, status=200)
        yield url


@pytest.fixture(name="mocked_stac_catalog_url")
def mocked_stac_catalog_url_():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp:
        # Mocked URL
        url = "https://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1")
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)
        resp.get(url=url + "/auxip/", json=json_landing_page, status=200)
        resp.get(url=url + "/cadip/", json=json_landing_page, status=200)

        yield url


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
        "RSPY_HOST_AUXIP": "https://dummy-audxip/auxip/",
        "RSPY_HOST_STAGING": "https://dummy-staging/staging/",
    }
    for key, val in envvars.items():
        monkeypatch.setenv(key, val)
    yield  # restore the environment


@pytest.fixture(name="generic_rs_client")
def generic_rs_client_(mocked_stac_catalog_url, monkeypatch):
    """Return a generic RsClient instance for testing."""
    monkeypatch.setenv("RSPY_OAUTH2_COOKIE", "RSPY_OAUTH2_COOKIE")
    yield RsClient(mocked_stac_catalog_url, RS_SERVER_API_KEY, OWNER_ID)  # will be used to test the StacClient


@pytest.fixture(name="auxip_client")
def auxip_client_(generic_rs_client):
    """Return a generic AuxipClient instance for testing."""
    yield generic_rs_client.get_auxip_client(ADGS_STATION)


@pytest.fixture(name="cadip_client")
def cadip_client_(generic_rs_client):
    """Return a generic CadipClient instance for testing."""
    yield generic_rs_client.get_cadip_client(CADIP_STATION)


@pytest.fixture(name="stac_client")
def stac_client_(generic_rs_client):
    """Return a generic StacClient instance for testing."""
    yield generic_rs_client.get_catalog_client()
