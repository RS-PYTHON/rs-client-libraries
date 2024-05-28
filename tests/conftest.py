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
def mocked_stac_catalog_url():
    """Mock responses to a STAC catalog server made with the "requests" library. Return the mocked server URL."""
    with responses.RequestsMock() as resp:
        # Mocked URL
        url = "http://mocked_stac_catalog_url"

        # This is the returned content when calling a real STAC catalog service with:
        # requests.get("http://real_stac_catalog_url/catalog/catalogs/<owner>").json()
        json = {
            "type": "Catalog",
            "id": "stac-fastapi",
            "title": "stac-fastapi",
            "description": "stac-fastapi",
            "stac_version": "1.0.0",
            "conformsTo": [
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
                "https://api.stacspec.org/v1.0.0-rc.2/item-search#context",
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0/collections",
                "https://api.stacspec.org/v1.0.0/ogcapi-features",
                "https://api.stacspec.org/v1.0.0/item-search#query",
                "https://api.stacspec.org/v1.0.0/item-search#sort",
                "https://api.stacspec.org/v1.0.0/item-search#fields",
                "https://api.stacspec.org/v1.0.0/item-search",
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
                "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
                "https://api.stacspec.org/v1.0.0-rc.3/ogcapi-features/extensions/transaction",
                "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/features-filter",
                "https://api.stacspec.org/v1.0.0-rc.2/item-search#filter",
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
                "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
                "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
                "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
            ],
            "links": [
                {"rel": "self", "type": "application/json", "href": url},
                {"rel": "root", "type": "application/json", "href": url},
                {"rel": "data", "type": "application/json", "href": f"{url}/collections"},
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
                    "href": f"{url}/search",
                    "method": "GET",
                },
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": f"{url}/search",
                    "method": "POST",
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
        resp.get(url=url + "/catalog/", json=json, status=200)
        yield url
