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

"""This module is used to share common functions between apis"""

import logging
import os
import shutil
import tarfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from prefect import get_run_logger
from prefect.exceptions import MissingContextError


@dataclass
class AuthInfo:
    """User authentication information in Keycloak."""

    # User login (preferred username)
    user_login: str

    # IAM roles
    iam_roles: list[str]

    # Oauth2 attributes and/or custom `config` associated to the API key
    attributes: dict[str, Any]


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


# True if the 'RSPY_LOCAL_MODE' environemnt variable is set to 1, true or yes (case insensitive).
# By default: if not set or set to a different value, return False.
LOCAL_MODE: bool = env_bool("RSPY_LOCAL_MODE", default=False)

# Cluster mode is the opposite of local mode
CLUSTER_MODE: bool = not LOCAL_MODE


def _get_logger():
    """Return the Prefect run logger when available, otherwise a module logger."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def _is_safe_extract_path(extract_to: Path, member_name: str) -> bool:
    """Return whether an archive member stays within the target directory."""
    member_path = Path(member_name.replace("\\", "/"))
    if member_path.is_absolute():
        return False
    try:
        (extract_to / member_path).resolve().relative_to(extract_to.resolve())
    except ValueError:
        return False
    return True


def extract_zip(zip_path: Path, extract_to: Path):
    """Extract a ZIP archive into the target directory."""
    logger = get_run_logger()
    logger.info(f"Extracting ZIP: {zip_path} -> {extract_to}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        count = 0
        for name in zip_ref.namelist():
            if not _is_safe_extract_path(extract_to, name):
                logger.warning(f"Skipping unsafe ZIP member: {name}")
                continue
            destination = (extract_to / name.replace("\\", "/")).resolve()
            if name.endswith("/"):
                destination.mkdir(parents=True, exist_ok=True)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                with zip_ref.open(name) as src, destination.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            count += 1
        logger.info(f"ZIP contains {count} safe entries")


def extract_tar(file_path: Path, extract_to: Path) -> int:
    """Extract a TAR-compatible archive into the target directory."""
    logger = get_run_logger()
    logger.info(f"Extracting TAR archive: {file_path} -> {extract_to}")

    with tarfile.open(file_path, "r:*") as tar:
        count = 0
        for member in tar.getmembers():
            if member.issym() or member.islnk() or member.isdev():
                logger.warning(f"Skipping unsafe TAR member: {member.name}")
                continue
            if not _is_safe_extract_path(extract_to, member.name):
                logger.warning(f"Skipping unsafe TAR member: {member.name}")
                continue
            destination = (extract_to / member.name).resolve()
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
            else:
                extracted = tar.extractfile(member)
                if extracted is None:
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                with extracted, destination.open("wb") as dst:
                    shutil.copyfileobj(extracted, dst)
            count += 1
        logger.info(f"TAR archive contains {count} safe entries")
        return count


def strip_archive_suffix(name: str) -> str:
    """Return the asset name without its supported archive suffix."""
    normalized_name = name.lower()
    for suffix in (".tar.gz", ".tgz", ".zip", ".tar"):
        if normalized_name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def get_upload_prefix(asset_href: str, asset_name: str) -> str:
    """Return the S3 prefix where extracted content should be uploaded."""
    logger = _get_logger()
    parent_prefix = asset_href.rsplit("/", 1)[0]

    if asset_name.lower().endswith(".zip"):
        result = parent_prefix + "/"
        logger.info(f"get_upload_prefix returning ZIP prefix={result}")
        return result

    last_segment = parent_prefix.rsplit("/", 1)[-1]
    normalized_segment = strip_archive_suffix(last_segment)

    if normalized_segment != last_segment:
        base_prefix = parent_prefix.rsplit("/", 1)[0]
        result = base_prefix + "/" + normalized_segment + "/"
        logger.info(f"get_upload_prefix returning normalized prefix={result}")
        return result

    result = parent_prefix + "/"
    logger.info(f"get_upload_prefix returning parent prefix={result}")
    return result


def _extract_nested_archive(full_path: Path) -> bool:
    """Extract a nested TAR-compatible archive and delete it on success."""
    logger = get_run_logger()
    logger.info(f"Found nested archive: {full_path}")
    extracted_members = extract_tar(full_path, full_path.parent)
    if not extracted_members:
        logger.warning("Skipping removal of nested archive because no members were extracted: " f"{full_path}")
        return False
    full_path.unlink()
    return True


def recursive_extract(folder: Path) -> int:
    """Extract nested TAR-compatible archives found anywhere under ``folder``."""
    logger = get_run_logger()
    extracted_count = 0
    extracted = True

    while extracted:
        extracted = False

        for root, _, files in os.walk(folder):
            for file in files:
                full_path = Path(root) / file

                if file.lower().endswith((".tar", ".tgz", ".tar.gz")) and _extract_nested_archive(full_path):
                    extracted = True
                    extracted_count += 1

    logger.info(f"Processed {extracted_count} nested archive(s)")
    return extracted_count


def normalize_extract_dir(extract_dir: Path) -> Path:
    """
    Return the directory that should be used as the upload root.

    If the extraction produced a single top-level directory, descend into it to
    avoid creating an unnecessary extra folder level in S3.
    """
    logger = get_run_logger()
    root_items = list(extract_dir.iterdir())

    if len(root_items) != 1:
        logger.info("No normalization needed, multiple root items found")
        return extract_dir

    root_item = root_items[0]
    if not root_item.is_dir():
        logger.info("No normalization needed, single root item is not a directory")
        return extract_dir

    logger.info(f"Normalizing folder structure: entering {root_item}")
    return root_item
