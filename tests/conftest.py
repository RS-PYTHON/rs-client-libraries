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

from tests import common


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
def mocked_stac_catalog_bad_endpoint():
    """Mock a response to a bad endpoint"""
    with responses.RequestsMock() as resp:
        url = "http://bad_endpoint"

        json_response = None

        resp.get(url=url + "/catalog/", json=json_response, status=None)
        yield url


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

        yield url


@pytest.fixture
def mocked_stac_catalog_search_adgs():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1")
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        json_search = {
            "type": "FeatureCollection",
            "context": {"limit": 10, "returned": 3},
            "features": [
                {
                    "id": "ADGS1.EOF",
                    "type": "Feature",
                    "links": [
                        {
                            "rel": "collection",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_aux",
                        },
                        {
                            "rel": "parent",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_aux",
                        },
                        {"rel": "root", "type": "application/json", "href": "http://127.0.0.1:8003/"},
                        {
                            "rel": "self",
                            "type": "application/geo+json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_aux/items/ADGS1.EOF",
                        },
                    ],
                    "assets": {
                        "file": {
                            "href": (
                                "https://rs-server/catalog/TestUser/collections/s1_aux/items/ADGS1.EOF/" "download/file"
                            ),
                            "alternate": {"s3": {"href": "s3://rs-cluster-catalog/stations/ADGS/ADGS1.EOF"}},
                            "file:size": 2029769,
                        },
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[180, -90], [180, 90], [-180, 90], [-180, -90], [180, -90]]],
                    },
                    "collection": "TestUser_s1_aux",
                    "properties": {
                        "owner": "TestUser",
                        "adgs:id": "09858a20-6996-4a07-9b79-e07741783f75",
                        "created": "2021-07-16T00:00:00.000Z",
                        "datetime": "2021-07-16T00:00:00.000Z",
                        "end_datetime": "2021-07-16T00:00:00.000Z",
                        "start_datetime": "2021-07-16T00:00:00.000Z",
                    },
                    "stac_version": "1.0.0",
                    "stac_extensions": [
                        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
                        "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
                    ],
                },
                {
                    "id": "ADGS2.EOF",
                    "type": "Feature",
                    "links": [
                        {
                            "rel": "collection",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_aux",
                        },
                        {
                            "rel": "parent",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_aux",
                        },
                        {"rel": "root", "type": "application/json", "href": "http://127.0.0.1:8003/"},
                        {
                            "rel": "self",
                            "type": "application/geo+json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_aux/items/ADGS2.EOF",
                        },
                    ],
                    "assets": {
                        "file": {
                            "href": (
                                "https://rs-server/catalog/TestUser/collections/s1_aux/items/ADGS2.EOF/" "download/file"
                            ),
                            "alternate": {"s3": {"href": "s3://rs-cluster-catalog/stations/ADGS/ADGS2.EOF"}},
                            "file:size": 1922340,
                        },
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[180, -90], [180, 90], [-180, 90], [-180, -90], [180, -90]]],
                    },
                    "collection": "TestUser_s1_aux",
                    "properties": {
                        "owner": "TestUser",
                        "adgs:id": "6e7f7f39-4060-4448-875d-b11f96681551",
                        "created": "2020-04-09T00:00:00.000Z",
                        "datetime": "2020-04-16T00:00:00.000Z",
                        "end_datetime": "2020-04-16T00:00:00.000Z",
                        "start_datetime": "2020-04-16T00:00:00.000Z",
                    },
                    "stac_version": "1.0.0",
                    "stac_extensions": [
                        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
                        "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
                    ],
                },
            ],
        }
        resp.post(url=url + "/catalog/search", json=json_search, status=200)
        yield url


@pytest.fixture
def mocked_stac_catalog_search_cadip():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1")
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        json_search = {
            "type": "FeatureCollection",
            "context": {"limit": 1000, "returned": 2},
            "features": [
                {
                    "id": "CADU1.raw",
                    "type": "Feature",
                    "links": [
                        {
                            "rel": "collection",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_chunk",
                        },
                        {
                            "rel": "parent",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_chunk",
                        },
                        {"rel": "root", "type": "application/json", "href": "http://127.0.0.1:8003/"},
                        {
                            "rel": "self",
                            "type": "application/geo+json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_chunk/items/CADU1.raw",
                        },
                    ],
                    "assets": {
                        "file": {
                            "href": (
                                "https://rs-server/catalog/TestUser/collections/s1_chunk/items/CADU1.raw/"
                                "download/file"
                            ),
                            "alternate": {"s3": {"href": "s3://rs-cluster-catalog/stations/CADIP/CADU1.raw"}},
                            "file:size": 58,
                        },
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[180, -90], [180, 90], [-180, 90], [-180, -90], [180, -90]]],
                    },
                    "collection": "TestUser_s1_chunk",
                    "properties": {
                        "owner": "TestUser",
                        "created": "2020-01-05T18:52:56.165Z",
                        "cadip:id": "d52278c7-75e5-4cb5-b8e4-0a75b83e9a2d",
                        "datetime": "1970-01-01T12:00:00.000Z",
                        "end_datetime": "1970-01-01T12:00:00.000Z",
                        "cadip:channel": 1,
                        "start_datetime": "1970-01-01T12:00:00.000Z",
                        "cadip:retransfer": "False",
                        "cadip:session_id": "S1A_20200105072204051312",
                        "cadip:final_block": "False",
                        "eviction_datetime": "2020-01-05T18:52:56.165Z",
                        "cadip:block_number": 1,
                    },
                    "stac_version": "1.0.0",
                    "stac_extensions": [
                        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
                        "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
                    ],
                },
                {
                    "id": "CADU2.raw",
                    "type": "Feature",
                    "links": [
                        {
                            "rel": "collection",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_chunk",
                        },
                        {
                            "rel": "parent",
                            "type": "application/json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_chunk",
                        },
                        {"rel": "root", "type": "application/json", "href": "http://127.0.0.1:8003/"},
                        {
                            "rel": "self",
                            "type": "application/geo+json",
                            "href": "http://127.0.0.1:8003/collections/TestUser_s1_chunk/items/CADU2.raw",
                        },
                    ],
                    "assets": {
                        "file": {
                            "href": (
                                "https://rs-server/catalog/TestUser/collections/s1_chunk/items/CADU2.raw/"
                                "download/file"
                            ),
                            "alternate": {"s3": {"href": "s3://rs-cluster-catalog/stations/CADIP/CADU2.raw"}},
                            "file:size": 63,
                        },
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[180, -90], [180, 90], [-180, 90], [-180, -90], [180, -90]]],
                    },
                    "collection": "TestUser_s1_chunk",
                    "properties": {
                        "owner": "TestUser",
                        "created": "2020-01-05T18:52:53.165Z",
                        "cadip:id": "0c6602db-9df4-48b5-b6e8-1d79b8a2da5a",
                        "datetime": "1970-01-01T12:00:00.000Z",
                        "end_datetime": "1970-01-01T12:00:00.000Z",
                        "cadip:channel": 1,
                        "start_datetime": "1970-01-01T12:00:00.000Z",
                        "cadip:retransfer": "False",
                        "cadip:session_id": "S1A_20200105072204051312",
                        "cadip:final_block": "False",
                        "eviction_datetime": "2020-01-05T18:52:53.165Z",
                        "cadip:block_number": 1,
                    },
                    "stac_version": "1.0.0",
                    "stac_extensions": [
                        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
                        "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
                    ],
                },
            ],
        }
        resp.post(url=url + "/catalog/search", json=json_search, status=200)
        yield url


@pytest.fixture
def mocked_stac_catalog_url():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = common.json_landing_page(url, "toto:S1_L1")
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        yield url
