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
from collections.abc import Collection
from os.path import commonprefix
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task
from pystac import Item, ItemCollection

from rs_common.prefect_utils import s3_delete, s3_download_file, s3_upload_file
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
    # Build regex from relative names inside common folder
    relative_parts = [re.escape(path.removeprefix(common_folder)) for path in paths]
    return common_folder, relative_parts


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
    asset: str | Collection[str] | None = None,
):
    """
    Download and extract assets from the given items to the destination directory.

    Args:
        items: List of STAC items containing assets to download.
        extract_to: Local directory where assets should be extracted.
        asset: Optional asset name, or asset names, to download. If omitted, all assets are downloaded.
    """
    logger = get_run_logger()
    selected_assets = {asset} if isinstance(asset, str) else set(asset) if asset else None
    stac_items = _normalize_stac_items(items)

    for item in stac_items:
        for asset_name, item_asset in item.assets.items():
            if selected_assets and asset_name not in selected_assets:
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


async def upload_folder_flat(local_folder: Path, prefix: str):
    """
    Upload all files under ``local_folder`` to the S3 prefix.

    The relative path below ``local_folder`` is preserved in the destination
    key so extracted archives keep their original folder structure.
    """
    logger = get_run_logger()
    files_to_upload = sorted(path for path in local_folder.rglob("*") if path.is_file())

    logger.info(
        f"Preparing upload of {len(files_to_upload)} file(s) from {local_folder} to {prefix}",
    )

    for file_path in files_to_upload:
        relative_path = file_path.relative_to(local_folder).as_posix()
        s3_path = prefix + relative_path
        logger.info(f"Uploading {file_path} -> {s3_path}")
        await s3_upload_file(file_path, s3_path)

    logger.info(f"Finished uploading {len(files_to_upload)} file(s) to {prefix}")


async def process_asset(asset_href: str, asset_name: str) -> str:
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
        logger.info(f"Uploading to prefix: {prefix}")

        await upload_folder_flat(upload_dir, prefix)

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
async def asset_unzip_decompress(stac_item: Item) -> Item:
    """Prefect flow used to unzip and decompress catalog store assets."""
    logger = get_run_logger()
    updated_assets = {}

    for asset_name, asset in stac_item.assets.items():
        # After normalisation (unzip / decompress) the href is changed with the new s3 path.
        # Therefore asset name should also be updated for supported archive types.
        if asset_name.lower().endswith(ARCHIVE_SUFFIXES):
            new_href = await process_asset(asset.href, asset_name)
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
