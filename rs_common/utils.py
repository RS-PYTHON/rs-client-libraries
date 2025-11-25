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
from datetime import datetime


@dataclass
class AuthInfo:
    """User authentication information in Keycloak."""

    # User login (preferred username)
    user_login: str

    # IAM roles
    iam_roles: list[str]

    # Configuration associated to the API key (not implemented for now)
    apikey_config: dict


def read_response_error(response) -> str:
    """Read and return an HTTP response error detail."""

    # Try to read the response detail or error
    try:
        json = response.json()
        if isinstance(json, str):
            return json
        return json.get("detail") or json.get("description") or json["error"]

    # If this fail, get the full response content
    except Exception:  # pylint: disable=broad-exception-caught
        return response.content.decode("utf-8", errors="ignore")


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


def strftime_millis(date: datetime):
    """Format datetime with milliseconds precision"""
    return date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def create_valcover_filter(
    start_datetime: datetime | str,
    end_datetime: datetime | str,
    product_type: str,
) -> dict:
    """Creates a ValCover filter from the input values to be used in flows

    Args:
        start_datetime: Start datetime for the time interval used to filter the files
        end_datetime: End datetime for the time interval used to filter the files
        product_type: Auxiliary file type wanted

    Returns:
        dict: ValCover filter
    """
    # Convert datetime inputs to str
    if isinstance(start_datetime, datetime):
        start_datetime = strftime_millis(start_datetime)
    if isinstance(end_datetime, datetime):
        end_datetime = strftime_millis(end_datetime)

    # CQL2 filter: we use a filter combining a ValCover filter and a product type filter
    return {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "product:type"}, product_type]},
            {
                "op": "t_contains",
                "args": [
                    {"interval": [{"property": "start_datetime"}, {"property": "end_datetime"}]},
                    {"interval": [start_datetime, end_datetime]},
                ],
            },
        ],
    }
