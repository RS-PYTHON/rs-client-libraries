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

"""Use "DPR as a service" implemented by rs-dpr-service"""

import ast
import os.path as osp

from openapi_core import OpenAPI  # Spec, validate_request, validate_response

from rs_client.ogcapi.ogcapi_client import OgcApiClient
from rs_common.utils import get_href_service

PATH_TO_YAML_OPENAPI = osp.realpath(
    osp.join(
        osp.dirname(__file__),
        "../../config",
        "staging_templates",
        "yaml",
        "dpr_openapi_schema.yaml",
    ),
)


class DprClient(OgcApiClient):
    """Implement the OGC API client for 'DPR as a service'."""

    # Init the OpenAPI instance from config file
    openapi = OpenAPI.from_file_path(PATH_TO_YAML_OPENAPI)

    @property
    def endpoint_prefix(self) -> str:
        """Return the endpoints prefix, if any."""
        return "dpr/"

    @property
    def href_service(self) -> str:
        """
        Return the rs-dpr-service URL hostname.
        This URL can be overwritten using the RSPY_HOST_DPR_SERVICE env variable (used e.g. for local mode).
        Otherwise it should just be the RS-Server URL.
        """
        return get_href_service(self.rs_server_href, "RSPY_HOST_DPR_SERVICE")

    def wait_for_job(self, *args, **kwargs) -> list[dict]:  # type: ignore
        """
        Wait for job to finish.

        Returns:
            EOPF results
        """
        # Call parent method and parse results
        job_status = super().wait_for_job(*args, **kwargs)
        return ast.literal_eval(job_status["message"])

    #
    # These endpoints are not implemented by the service

    def get_processes(self) -> dict:
        raise NotImplementedError

    def get_jobs(self) -> dict:
        raise NotImplementedError

    def delete_job(self, _: str) -> dict:
        raise NotImplementedError

    def get_job_results(self, _: str) -> dict:
        raise NotImplementedError
