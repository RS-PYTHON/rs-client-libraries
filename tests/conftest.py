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
        json_landing_page = {
            "type": "Catalog",
            "id": "stac-fastapi",
            "title": "stac-fastapi",
            "description": "stac-fastapi",
            "stac_version": "1.0.0",
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "root", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "data", "type": "application/json", "href": f"{url}/catalog/collections"},
                {
                    "rel": "conformance",
                    "type": "application/json",
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": f"{url}/conformance",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "GET",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "POST",
                },
                {
                    "rel": "child",
                    "type": "application/json",
                    "title": "toto_S1_L1",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": f"{url}/api",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": f"{url}/api.html",
                },
            ],
            "stac_extensions": [],
        }
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
        json_landing_page = {
            "type": "Catalog",
            "id": "stac-fastapi",
            "title": "stac-fastapi",
            "description": "stac-fastapi",
            "stac_version": "1.0.0",
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "root", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "data", "type": "application/json", "href": f"{url}/catalog/collections"},
                {
                    "rel": "conformance",
                    "type": "application/json",
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": f"{url}/conformance",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "GET",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "POST",
                },
                {
                    "rel": "child",
                    "type": "application/json",
                    "title": "toto_S1_L1",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": f"{url}/api",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": f"{url}/api.html",
                },
            ],
            "stac_extensions": [],
        }
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
        json_landing_page = {
            "type": "Catalog",
            "id": "stac-fastapi",
            "title": "stac-fastapi",
            "description": "stac-fastapi",
            "stac_version": "1.0.0",
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "root", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "data", "type": "application/json", "href": f"{url}/catalog/collections"},
                {
                    "rel": "conformance",
                    "type": "application/json",
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": f"{url}/conformance",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "GET",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "POST",
                },
                {
                    "rel": "child",
                    "type": "application/json",
                    "title": "toto_S1_L1",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": f"{url}/api",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": f"{url}/api.html",
                },
            ],
            "stac_extensions": [],
        }
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
        json_landing_page = {
            "type": "Catalog",
            "id": "stac-fastapi",
            "title": "stac-fastapi",
            "description": "stac-fastapi",
            "stac_version": "1.0.0",
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "root", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "data", "type": "application/json", "href": f"{url}/catalog/collections"},
                {
                    "rel": "conformance",
                    "type": "application/json",
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": f"{url}/conformance",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "GET",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "POST",
                },
                {
                    "rel": "child",
                    "type": "application/json",
                    "title": "toto_S1_L1",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": f"{url}/api",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": f"{url}/api.html",
                },
            ],
            "stac_extensions": [],
        }
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
        json_landing_page = {
            "type": "Catalog",
            "id": "stac-fastapi",
            "title": "stac-fastapi",
            "description": "stac-fastapi",
            "stac_version": "1.0.0",
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "root", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "data", "type": "application/json", "href": f"{url}/catalog/collections"},
                {
                    "rel": "conformance",
                    "type": "application/json",
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": f"{url}/conformance",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "GET",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "POST",
                },
                {
                    "rel": "child",
                    "type": "application/json",
                    "title": "toto_S1_L1",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": f"{url}/api",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": f"{url}/api.html",
                },
            ],
            "stac_extensions": [],
        }
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
def mocked_stac_catalog_url():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json_landing_page = {
            "type": "Catalog",
            "id": "stac-fastapi",
            "title": "stac-fastapi",
            "description": "stac-fastapi",
            "stac_version": "1.0.0",
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "root", "type": "application/json", "href": f"{url}/catalog/"},
                {"rel": "data", "type": "application/json", "href": f"{url}/catalog/collections"},
                {
                    "rel": "conformance",
                    "type": "application/json",
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": f"{url}/conformance",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "GET",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/catalog/search",
                    "method": "POST",
                },
                {
                    "rel": "child",
                    "type": "application/json",
                    "title": "toto_S1_L1",
                    "href": f"{url}/catalog/collections/toto:S1_L1",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": f"{url}/api",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": f"{url}/api.html",
                },
            ],
            "stac_extensions": [],
        }
        resp.get(url=url + "/catalog/", json=json_landing_page, status=200)

        yield url
