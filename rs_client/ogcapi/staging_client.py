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

"""Launch staging with rs-client-libraries"""

import json
import math
import os
import os.path as osp
from collections import defaultdict
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

# openapi_core libraries used for endpoints validation
from openapi_core import OpenAPI  # Spec, validate_request, validate_response
from stac_pydantic.api import Item, ItemCollection

from rs_client.ogcapi.ogcapi_client import OgcApiClient, OgcValidationException
from rs_common.utils import get_href_service

PATH_TO_YAML_OPENAPI = osp.realpath(
    osp.join(
        osp.dirname(__file__),
        "../../config",
        "staging_templates",
        "yaml",
        "staging_openapi_schema.yaml",
    ),
)
RESOURCE = "staging"


class StagingClient(OgcApiClient):
    """Implement the OGC API client for the staging."""

    # Init the OpenAPI instance from config file
    openapi = OpenAPI.from_file_path(PATH_TO_YAML_OPENAPI)

    @property
    def href_service(self) -> str:
        """
        Return the RS-Server staging URL hostname.
        This URL can be overwritten using the RSPY_HOST_STAGING env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_STAGING")

    def run_staging(  # pylint: disable=too-many-locals
        self,
        stac_input: dict[Any, Any] | str,
        out_coll_name: str,
    ) -> dict[str, dict]:
        """Method to start the staging process from rs-client - Call the endpoint /processes/staging/execution

        Args:
            stac_input (dict | str): it can be:
                - A Python dictionary corresponding to a Feature or a FeatureCollection (that can be for example
                  the output of a search for Cadip or Auxip sessions)
                - A json string corresponding to a Feature or a FeatureCollection
                - A string corresponding to a path to a json file containing a Feature or a FeatureCollection
                - A single link that returns a STAC itemCollection: this link should be an url to search a
                  itemCollection, for example:
                  http://localhost:8002/cadip/search?ids=S1A_20231120061537234567&collections=cadip_sentinel1
        out_coll_name (str): name of the output collection
        Return:
            dict(hostname, job_id (int, str)): Returns the status codes of the staging requests (one per URL hostname) +
            the identifiers
        """
        stac_input_dict = {}

        # ----- Case 1: we only load a link (that refers to a STAC itemCollection) in the staging request body
        if isinstance(stac_input, str):

            # Check that it's an url
            parsed = urlparse(stac_input)
            if parsed.scheme and parsed.netloc and parsed.hostname:
                staging_body = {"inputs": {"collection": out_coll_name, "items": {"href": stac_input}}}
                return {parsed.hostname: super()._run_process(RESOURCE, staging_body)}

        # ----- Case 2: we directly load a STAC ItemCollection in the staging request body

        # If stac_input is a file, load this file to a dictionary
        if isinstance(stac_input, str):
            # If the input is a valid path to a json_file, load this file
            if os.path.exists(os.path.dirname(stac_input)) and stac_input.endswith(".json"):
                # Read the yaml or json file
                with open(stac_input, encoding="utf-8") as opened:
                    stac_file_to_dict = json.loads(opened.read())
                    stac_input_dict = stac_file_to_dict
            # If the input string is not a path, try to convert the content of the string to a json dictionary
            else:
                try:
                    stac_input_dict = json.loads(stac_input)
                except json.JSONDecodeError as e:
                    raise OgcValidationException(
                        f"""Invalid input format: {stac_input} - Input data must be either:
                            - A Python dictionary corresponding to a Feature or a FeatureCollection
                            (that can be for example the output of a search for Cadip or Auxip sessions)
                            - A json string corresponding to a Feature or a FeatureCollection
                            - A string corresponding to a path to a json file containing a Feature or a
                            FeatureCollection
                            - A single link that returns a STAC ItemCollection: this link should be an
                            url to search an ItemCollection
                        """.strip(),
                    ) from e
        else:
            stac_input_dict = stac_input

        if "type" not in stac_input_dict:
            raise KeyError("Key 'type' is missing from the staging input data")

        # Validate input data using Pydantic
        if stac_input_dict["type"] == "Feature":
            stac_items: Sequence[Item] = [Item(**stac_input_dict)]
        else:
            stac_items = ItemCollection(**stac_input_dict).features

        # Order the items by station url hostname
        hostname_items: dict[str, list[Item]] = defaultdict(list)
        for item in stac_items:
            assets = list(item.assets.values())
            if assets:
                hostname = urlparse(assets[0].href).hostname or ""
                hostname_items[hostname].append(item)

        # Trigger staging for each domain items
        hostname_triggers: dict[str, dict] = {}
        for hostname, items in hostname_items.items():
            stac_item_collection = ItemCollection(type="FeatureCollection", features=items)
            staging_body = {
                "inputs": {
                    "collection": out_coll_name,
                    "items": {"value": stac_item_collection.model_dump(mode="json")},
                },
            }
            hostname_triggers[hostname] = super()._run_process(RESOURCE, staging_body)

        return hostname_triggers

    def wait_for_jobs(
        self,
        all_job_status: dict[str, dict],
        logger=None,
        timeout: int | float = math.inf,
        poll_interval: int = 2,
    ):
        """
        Wait for job to finish.

        Args:
            job_status: Returned by `run_staging`
            logger: To show advancement in logger
            timeout: Job completion timeout in seconds
            poll_interval: When to check again for job completion in seconds

        Raises:
            RuntimeError in case of error
        """
        # Call parent method for each hostname
        for hostname, job_status in all_job_status.items():
            super().wait_for_job(
                job_status,
                logger,
                f"Staging from {hostname!r}",
                timeout,
                poll_interval,
            )
