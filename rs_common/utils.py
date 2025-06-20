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

"""This module is used to share common functions between apis"""

import os
from dataclasses import dataclass


@dataclass
class AuthInfo:
    """User authentication information in Keycloak."""

    # User login (preferred username)
    user_login: str

    # IAM roles
    iam_roles: list[str]

    # Configuration associated to the API key (not implemented for now)
    apikey_config: dict


def read_response_error(response):
    """Read and return an HTTP response error detail."""

    # Try to read the response detail or error
    try:
        json = response.json()
        detail = json.get("detail") or json.get("description") or json["error"]

    # If this fail, get the full response content
    except Exception:  # pylint: disable=broad-exception-caught
        detail = response.content.decode("utf-8", errors="ignore")

    return detail


def get_href_service(rs_server_href, env_var) -> str:
    """Get specific href link."""
    if from_env := os.getenv(env_var, None):
        return from_env.rstrip("/")
    if not rs_server_href:
        raise RuntimeError("RS-Server URL is undefined")
    return rs_server_href.rstrip("/")


def env_bool(var: str, default: bool) -> bool:
    """
    Return True if an environemnt variable is set to 1, true or yes (case insensitive).
    Return False if set to 0, false or no (case insensitive).
    Return the default value if not set or set to a different value.
    """
    val = os.getenv(var, str(default)).lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
    return default
