#!/bin/bash
# Copyright 2023-2026 Airbus, CS Group
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
Call Prefect REST API from the command line of a Github CI/CD workflow or action.

See: https://docs.prefect.io/v3/api-ref/rest-api

NOTE:
The PREFECT_CONFIG_DEV and PREFECT_CONFIG_OPS variables are defined in
https://github.com/RS-PYTHON/rs-client-libraries/settings/variables/actions
It should have yaml content as:
prefect_api_url: https://<prefect-public-domain>/api

The PREFECT_CREDENTIALS_DEV and PREFECT_CREDENTIALS_OPS secrets are defined in
https://github.com/RS-PYTHON/rs-client-libraries/settings/secrets/actions
It should have yaml content as:
keycloak_token_url: ***
client_id: ***
client_secret: ***
"""

import yaml


def read_access_token(config: str, credentials: str) -> str:
    """
    Read access token returned by KeyCloak.

    Args:
        config: Prefect configuration as a yaml str
        credentials: Prefect credentials as a yaml str

    Returns: access token as a string.
    """
    # Read yaml content.
    # WARNING: don't print the credentials value because they could appear in clear in the ci/cd logs!
    config_dict = yaml.safe_load(config)
    credentials_dict = yaml.safe_load(credentials)
