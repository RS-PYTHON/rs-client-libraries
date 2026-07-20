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

"""General utilities"""

import asyncio
import os
import re
import shutil
import tempfile
from os.path import commonprefix
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task
from pystac import Item, ItemCollection

from rs_common.prefect_utils import s3_delete, s3_download_file, s3_upload_dir
from rs_common.utils import (
    extract_tar,
    extract_zip,
    get_upload_prefix,
    normalize_extract_dir,
    recursive_extract,
    strip_archive_suffix,
)
from rs_workflows.flow_utils import ARCHIVE_SUFFIXES


def search_by_name(values: list[dict[str, Any]], name: str) -> Any:
    """Return the first item whose "name" field matches the given value."""
    return next((obj for obj in values if obj.get("name") == name), None)


def get_common_and_relative_paths(paths: list[str]) -> tuple[str, list[str]]:
    """
    Returns the common folder prefix of the given paths and their escaped
    relative parts.

    The common prefix is truncated to the last `/` so it always represents
    a directory.

    Args:
        paths (list[str]): List of absolute paths or URIs.

    Returns:
        tuple[str, list[str]]:
            - Common folder ending with `/`
            - Escaped relative parts for each path
    """
    # Compute longest common prefix
    common_prefix = commonprefix(paths)
    # Ensure prefix stops on folder boundary
    common_folder = common_prefix[: common_prefix.rfind("/") + 1]
    # List relative names inside common folder
    relative_parts = [re.escape(path.removeprefix(common_folder)) for path in paths]
    return common_folder, relative_parts


# Pattern used in reading from logfile in run_processor()
# The main line:
# 2026-04-17 11:56:08 - module - DEBUG - message
# Compacted regex (2 lines) for debugging in any online website
# ^(?P<timestamp>\d{4}-\d{2}-\d{2}[^\n]*?)\s+-\s+(?P<logger>.*?)\s*(?:-\s+|:\s+)\[?
# (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\]?\s*(?:-\s+|:\s+)?(?P<message>.*)$
LOG_PATTERN = re.compile(
    r"""
    # positions itself at the start of the line
    ^
    # find timestamp
    (?P<timestamp>\d{4}-\d{2}-\d{2}[^\n]*?)
    # matches any whitespace with - in the middle
    \s+-\s+
    # find the module name (ex. eopf)
    (?P<logger>.*?)
    # find the logging level for both DEBUG and [DEBUG]
    \s*
    (?:
        -\s+
        |
        :\s+
    )

    \[?
    (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)
    \]?

    \s*
    (?:
        -\s+
        |
        :\s+
    )?
    # finds the log message
    (?P<message>.*)
    # assert position at end of the line
    $
    """,
    re.MULTILINE | re.VERBOSE,
)


def parse_logs(text):
    """
    Parse EOPF processor logs into structured log entries.
    The parser detects the beginning of a new log entry using the global LOG_PATTERN regex.

    Each yielded log entry contains:
    - timestamp
    - logger
    - level
    - message

    Text found before the first detected log entry is classified as an INFO-level message.
    """

    current = None
    # Stores lines found before the first valid log match
    preamble = []  # type: list[str]

    for line in text.splitlines():

        match = LOG_PATTERN.match(line)

        if match:
            # If text exists before the first valid log entry, return INFO log entry
            if preamble:

                yield {"timestamp": "", "logger": "system", "level": "INFO", "message": "\n".join(preamble)}

                # Reset buffer
                preamble = []

            # When a new log entry starts, the previous one is now complete
            if current:
                yield current

            current = {
                "timestamp": match.group("timestamp"),
                "logger": match.group("logger"),
                "level": match.group("level"),
                "message": match.group("message"),
            }
        # The line is either preamble text or continuation of a multiline message
        else:

            if current is None:
                preamble.append(line)

            # multiline message
            else:
                current["message"] += "\n" + line

    # The last entry still hasn't been emitted yet
    # Entries are emitted when the next entry begins
    if current:
        yield current

    # Edge case when only preabmle exists
    elif preamble:
        yield {"timestamp": "", "logger": "system", "level": "INFO", "message": "\n".join(preamble)}


def get_archived_item_indexes(item_collection) -> list[int]:
    """
    Return the indexes of staged items that still reference archive assets.

    Only items whose asset hrefs end with one of the known archive suffixes need
    the extra unzip/decompress normalization step. Returning indexes instead of
    copying items allows the caller to update the existing collection in place
    after normalization completes.
    """
    archived_indexes = []
    for idx, stac_item in enumerate(item_collection.items):
        if any(asset.href.endswith(ARCHIVE_SUFFIXES) for asset in stac_item.assets.values()):
            archived_indexes.append(idx)
    return archived_indexes


def _normalize_stac_items(items: Item | ItemCollection | dict[str, Any] | list[Item | dict[str, Any]]) -> list[Item]:
    """Convert supported STAC item inputs to pystac Items."""
    if isinstance(items, Item):
        return [items]

    if isinstance(items, ItemCollection):
        return list(items.items)

    if isinstance(items, dict):
        if items.get("type") == "FeatureCollection":
            return list(ItemCollection.from_dict(items).items)
        if items.get("type") == "Feature":
            return [Item.from_dict(items)]
        raise ValueError("Expected a STAC Feature or FeatureCollection dictionary.")

    normalized_items = []
    for item in items:
        if isinstance(item, Item):
            normalized_items.append(item)
        elif isinstance(item, dict):
            normalized_items.append(Item.from_dict(item))
        else:
            raise TypeError(f"Expected STAC Item objects or dictionaries, got {type(item)!r}.")

    return normalized_items


@task(name="download_and_extract_assets")
async def download_and_extract_assets_task(
    items: Item | ItemCollection | dict[str, Any] | list[Item | dict[str, Any]],
    extract_to: Path,
    asset: str | None = None,
):
    """
    Download and extract all assets from the given items to the destination directory.

    Args:
        items: STAC item input containing assets to download.
        extract_to: Local directory where assets should be extracted.
        asset: Optional asset name to download. When omitted, all assets are downloaded.
    """
    logger = get_run_logger()

    for item in _normalize_stac_items(items):
        for asset_name, item_asset in item.assets.items():
            if asset is not None and asset_name != asset:
                continue

            if not item_asset.href.startswith("s3://"):
                logger.warning(f"Skipping non-S3 asset: {asset_name} ({item_asset.href})")
                continue

            # create the temporary file off the event loop
            tmp_fd, tmp_name = await asyncio.to_thread(
                tempfile.mkstemp,
                suffix=Path(item_asset.href).suffix,
            )
            os.close(tmp_fd)
            tmp_path = Path(tmp_name)

            try:
                logger.info(f"Downloading asset {asset_name} from {item_asset.href}")
                await s3_download_file(item_asset.href, tmp_path)

                # extract or move to destination
                if item_asset.href.lower().endswith((".zip", ".tar", ".tgz", ".tar.gz")):
                    logger.info(f"Extracting {item_asset.href} to {extract_to}")
                    if item_asset.href.lower().endswith(".zip"):
                        extract_zip(tmp_path, extract_to)
                    else:
                        extract_tar(tmp_path, extract_to)

                    # handle nested archives (common in AUXIP)
                    recursive_extract(extract_to)
                else:
                    # not an archive, just copy/move to destination
                    dest_path = extract_to / Path(item_asset.href).name
                    logger.info(f"Copying {item_asset.href} to {dest_path}")
                    shutil.copy(tmp_path, dest_path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()


async def process_asset(asset_href: str, asset_name: str, use_extension=False) -> str:
    """
    Process an archived AUXIP asset stored in S3 and replace it with its extracted content.

    If the asset href points to a `.zip`, `.tar`, `.tgz`, or `.tar.gz` object in S3,
    the archive is downloaded to a temporary local directory and extracted. If the
    extracted content contains nested `.tar`, `.tgz`, or `.tar.gz` archives, those
    archives are also extracted in place.

    The extracted payload is then uploaded back to the same S3 parent prefix using a
    folder-like target derived from the original ZIP name. In this context,
    "normalization" means replacing the original archive object with the extracted
    directory content under its corresponding S3 prefix.

    Example:
    - input href: `s3://bucket/path/some_adfs.zip`
    - extracted content: `file.xml` and `content.tar.gz`
    - final S3 result: `s3://bucket/path/some_adfs/` containing `file.xml` and the
    extracted content from `content.tar.gz`

    The function returns the new S3 prefix pointing to the extracted content.
    """
    logger = get_run_logger()
    logger.info(f"Processing asset: {asset_href}")

    if not asset_name.lower().endswith(ARCHIVE_SUFFIXES):
        raise ValueError(f"Unsupported archive type for asset '{asset_name}'")

    is_zip = asset_name.lower().endswith(".zip")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir)  # type: ignore

        archive_local = tmp_dir / ("archive.zip" if is_zip else Path(asset_href).name)  # type: ignore
        extract_dir = tmp_dir / "extracted"  # type: ignore
        extract_dir.mkdir()

        # 1. Download
        logger.info(f"Downloading {asset_href} -> {archive_local}")
        await s3_download_file(asset_href, archive_local)

        # 2. Remove the original archive before publishing the extracted content.
        logger.info(f"Deleting original archive from S3: {asset_href}")
        s3_delete(asset_href)

        # 3. Extract the main archive first.
        if is_zip:
            extract_zip(archive_local, extract_dir)
        else:
            extract_tar(archive_local, extract_dir)

        # 4. Some AUXIP deliveries contain nested TAR/TGZ/TAR.GZ payloads.
        nested_archives = recursive_extract(extract_dir)
        logger.info(f"Nested extraction complete, processed {nested_archives} archive(s)")

        # 5. Pick the most appropriate directory root for the upload step.
        upload_dir = normalize_extract_dir(extract_dir)
        logger.info(f"Selected upload root: {upload_dir}")

        # 6. Upload the extracted payload back to the original S3 prefix.
        prefix = get_upload_prefix(asset_href, asset_name)
        if use_extension:
            prefix = prefix.rstrip("/") + f"{upload_dir.suffix}/"  # .SAFE or .SEN3 for example
        logger.info(f"Uploading to prefix: {prefix}")

        await s3_upload_dir(upload_dir, prefix)

        if prefix.rstrip("/").lower().endswith(".zarr"):
            zarr_href = prefix.rstrip("/")
            logger.info(f"Returning normalized Zarr store href: {zarr_href}")
            return zarr_href

        extracted_files = [path for path in upload_dir.rglob("*") if path.is_file()]
        if not extracted_files:
            logger.info(f"No extracted files found, returning normalized folder prefix: {prefix}")
            return prefix

        # Always expose a concrete extracted file in the normalized href.
        # When several files are produced, pick a deterministic "main" payload
        # by preferring the largest file and then the lexicographically smallest
        selected_file = min(
            extracted_files,
            key=lambda path: (-path.stat().st_size, path.relative_to(upload_dir).as_posix()),
        )
        selected_href = prefix + selected_file.relative_to(upload_dir).as_posix()
        logger.info(f"Selected extracted file for normalized href: {selected_href}")
        return selected_href


@flow(name="Asset unzip and decompress")
async def asset_unzip_decompress(stac_item: Item, use_extension: bool = False) -> Item:
    """Prefect flow used to unzip and decompress catalog store assets."""
    logger = get_run_logger()
    updated_assets = {}

    for asset_name, asset in stac_item.assets.items():
        # After normalisation (unzip / decompress) the href is changed with the new s3 path.
        # Therefore asset name should also be updated for supported archive types.
        if asset_name.lower().endswith(ARCHIVE_SUFFIXES):
            new_href = await process_asset(asset.href, asset_name, use_extension)
            asset.href = new_href
            updated_assets[strip_archive_suffix(asset_name)] = asset
        else:
            updated_assets[asset_name] = asset

    logger.info(f"Updated the following asset {updated_assets} for item {stac_item.id}")
    stac_item.assets = updated_assets
    return stac_item


@task(name="Asset unzip")
async def asset_unzip_decompress_task(*args, **kwargs) -> Item:
    """See: asset_unzip_decompress"""
    return await asset_unzip_decompress.fn(*args, **kwargs)


def build_output_lineage(task_table: dict, pipeline_name: str) -> dict[str, set[str]]:
    """
    Parse the task_table and retrieve necessary information.

    For mockup returns: {'S03MWRL0_': {'S3BCADUS', 'osf'}, 'S03OLCL0_': {'fro', 'S3BCADUS'}}
    """

    pipeline = next(p for p in task_table["pipelines"] if p["name"] == pipeline_name)

    units = {u["name"]: u for u in task_table["units"]}

    lineage: dict[str, set[str]] = {}

    for step in sorted(pipeline["steps"], key=lambda s: s["step_id"]):

        sources: set[str] = set()

        # inherit lineage from inputs
        for logical_name, source in step.get("input_products", {}).items():

            if source == "pipeline_input":
                sources.add(logical_name)

            else:
                # e.g. "single_unit_mockup.S03OLCL0_"
                output_name = source.split(".", 1)[1]
                sources.update(lineage[output_name])

        # add ADFs declared by this unit
        unit = units[step["unit_name"]]

        for adf in unit.get("input_adfs", []):
            sources.add(adf["name"])

        # every output produced by this step gets the same lineage
        for output_name in step["output_products"].keys():
            lineage[output_name] = set(sources)

    return lineage
