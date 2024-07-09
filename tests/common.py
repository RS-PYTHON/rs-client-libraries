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

"""Common code for the pytests."""


def json_landing_page(href: str, user_collection: str, conforms_to=True):
    """Return the catalog landing page."""

    conforms_to_dict = (
        {
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0-rc.3/ogcapi-features/extensions/transaction",
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
                "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/features-filter",
                "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
                "https://api.stacspec.org/v1.0.0/item-search#sort",
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
                "https://api.stacspec.org/v1.0.0/ogcapi-features",
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
                "https://api.stacspec.org/v1.0.0-rc.2/item-search#context",
                "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
                "https://api.stacspec.org/v1.0.0/collections",
                "https://api.stacspec.org/v1.0.0/item-search",
                "https://api.stacspec.org/v1.0.0/item-search#query",
                "https://api.stacspec.org/v1.0.0/item-search#fields",
                "https://api.stacspec.org/v1.0.0/core",
                "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
                "https://api.stacspec.org/v1.0.0-rc.2/item-search#filter",
                "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
            ],
        }
        if conforms_to
        else {}
    )

    return {
        "type": "Catalog",
        "id": "stac-fastapi",
        "title": "stac-fastapi",
        "description": "stac-fastapi",
        "stac_version": "1.0.0",
        **conforms_to_dict,
        "links": [
            {"rel": "self", "type": "application/json", "href": f"{href}/catalog/"},
            {"rel": "root", "type": "application/json", "href": f"{href}/catalog/"},
            {"rel": "data", "type": "application/json", "href": f"{href}/catalog/collections"},
            {
                "rel": "conformance",
                "type": "application/json",
                "title": "STAC/WFS3 conformance classes implemented by this server",
                "href": f"{href}/conformance",
            },
            {
                "rel": "search",
                "type": "application/geo+json",
                "title": "STAC search",
                "href": f"{href}/catalog/search",
                "method": "GET",
            },
            {
                "rel": "search",
                "type": "application/geo+json",
                "title": "STAC search",
                "href": f"{href}/catalog/search",
                "method": "POST",
            },
            {
                "rel": "child",
                "type": "application/json",
                "title": "{user_collection}",
                "href": f"{href}/catalog/collections/{user_collection}",
            },
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI service description",
                "href": f"{href}/api",
            },
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI service documentation",
                "href": f"{href}/api.html",
            },
        ],
        "stac_extensions": [],
    }
