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

"""Auxip flow implementation."""

import datetime
import json
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.artifacts import acreate_markdown_artifact
from pystac import Item, ItemCollection

from rs_client.stac.auxip_client import AuxipClient
from rs_client.stac.catalog_client import CatalogClient
from rs_common.prefect_utils import s3_delete, s3_download_file, s3_upload_file
from rs_common.utils import (
    create_valcover_filter,
    extract_tar,
    extract_zip,
    get_upload_prefix,
    normalize_extract_dir,
    recursive_extract,
    strip_archive_suffix,
)
from rs_workflows.flow_utils import ARCHIVE_SUFFIXES, FlowEnv, FlowEnvArgs
from rs_workflows.staging_flow import staging_task

####################################################
# Auxip unzip and decompress convenience functions #
####################################################


# this can't be in rs_common.utils because of circular imports with prefect_utils
async def upload_folder_flat(local_folder: Path, prefix: str):
    """
    Upload all files under ``local_folder`` to the same S3 prefix.

    The upload is intentionally flattened: only the filename is kept in the
    destination key, regardless of the original nested local path.
    """
    logger = get_run_logger()
    files_to_upload = [path for path in local_folder.rglob("*") if path.is_file()]

    logger.info(
        f"Preparing flat upload of {len(files_to_upload)} file(s) from {local_folder} to {prefix}",
    )

    for file_path in files_to_upload:
        s3_path = prefix + file_path.name
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
        selected_file = min(extracted_files, key=lambda path: (-path.stat().st_size, path.name))
        logger.info(f"Selected extracted file for normalized href: {prefix + selected_file.name}")
        return prefix + selected_file.name


###############
# Auxip flows #
###############


@flow(name="Auxip search")
async def search(
    env: FlowEnvArgs,
    auxip_cql2: dict,
    error_if_empty: bool = False,
) -> ItemCollection | None:
    """
    Search Auxip products.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        auxip_cql2: Auxip CQL2 filter read from the processor tasktable.
        error_if_empty: Raise a ValueError if the results are empty.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "auxip-search"):

        logger.info(f"Start Auxip search: {auxip_cql2}")
        auxip_client: AuxipClient = flow_env.rs_client.get_auxip_client()
        found = auxip_client.search(
            method="POST",
            stac_filter=auxip_cql2.get("filter"),
            max_items=auxip_cql2.get("limit"),
            sortby=auxip_cql2.get("sortby"),
        )
        if (not found) and error_if_empty:
            raise ValueError(
                f"No Auxip product found for CQL2 filter: {json.dumps(auxip_cql2, indent=2)}",
            )
        logger.info(f"Auxip search found {len(found)} result(s): {found.to_dict()}")
        return found


@flow(name="Auxip staging")
async def auxip_staging(
    env: FlowEnvArgs,
    cql2_filter: dict,
    catalog_collection_identifier: str,
    timeout_seconds: int = -1,
) -> tuple[bool, ItemCollection | None]:
    """
    Generic flow to retrieve a list of items matching the STAC CQL2 filter given, and to stage the ones
    that are not already in the catalog.

    Args:
        env (FlowEnvArgs): Prefect flow environment
        stac_query (dict): CQL2 filter to select which files to stage
        catalog_collection_identifier (str): Catalog collection identifier where CADIP sessions and AUX data are staged
        timeout_seconds (int): Timeout value for the Auxip search task.
            Optional, if no value is given the process will run until it is completed

    Returns:
        bool: Return status: False if staging failed, True otherwise
        ItemCollection: List of catalog Items staged from Auxip station
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "auxip-staging"):

        # Search Auxip products
        auxip_items: ItemCollection | None = (
            search_task.with_options(timeout_seconds=timeout_seconds if timeout_seconds >= 0 else None)
            .submit(
                flow_env.serialize(),
                auxip_cql2=cql2_filter,
                error_if_empty=False,
            )
            .result()  # type: ignore
        )

        # Stop process if search task didn't return any item
        if not auxip_items or len(auxip_items) == 0:
            logger.info("Nothing to stage: Auxip search with given filter returned empty result.")
            return True, None

        # Stage Auxip items
        staged = staging_task.submit(
            flow_env.serialize(),
            auxip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        staging_results = staged.result()

        # Check that all jobs monitored were successful. Otherwise, return status is "False"
        return_status = True
        for job_name in staging_results:
            job_result = staging_results[job_name]
            if "status" not in job_result or job_result["status"] != "successful":
                logger.info(
                    f"Staging job '{job_name}' with ID {job_result['jobID']} FAILED.\n"
                    f"Status: {job_result['status']} - Reason: {job_result['message']}",
                )
                logger.debug({job_name: job_result})
                return_status = False

        # Get staged items from catalog (to have the correct href)
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        catalog_items = ItemCollection(
            catalog_client.get_items(
                collection_id=catalog_collection_identifier,
                items_ids=[item.id for item in auxip_items],
            ),
        )

        # Create artifact if all jobs succeeded
        if return_status:
            logger.info("Staging successful, creating artifact with a list of staged items.")
            await acreate_markdown_artifact(
                markdown=f"{json.dumps(catalog_items.to_dict(), indent=2)}",
                key="auxiliary-files",
                description="Auxiliary files added to catalog.",
            )

        return return_status, catalog_items


@flow(name="On-demand Auxip staging")
async def on_demand_auxip_staging(
    env: FlowEnvArgs,
    start_datetime: datetime.datetime | str,
    end_datetime: datetime.datetime | str,
    product_type: str,
    catalog_collection_identifier: str,
) -> tuple[bool, ItemCollection | None]:
    """
    Flow to retrieve Auxip files using a ValCover filter with the given time interval defined by
    start_datetime and end_datetime, select only the type of files wanted if eopf_type is given, stage
    the files and add STAC items into the catalog.
    Informations on ValCover filter:
    https://pforge-exchange2.astrium.eads.net/confluence/display/COPRS/4.+External+data+selection+policies

    Args:
        env: Prefect flow environment
        start_datetime: Start datetime for the time interval used to filter the files
            (select a date or directly enter a timestamp, e.g. "2025-08-07T11:51:12.509000Z")
        end_datetime: End datetime for the time interval used to filter the files
            (select a date or directly enter a timestamp, e.g. "2025-08-10T14:00:00.509000Z")
        product_type: Auxiliary file type wanted
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged

    Returns:
        bool: Return status: False if staging failed, True otherwise
        ItemCollection: List of Items retrieved from the Auxip search and staged to the catalog
    """

    # CQL2 filter: we use a filter combining a ValCover filter and a product type filter
    cql2_filter = create_valcover_filter(start_datetime, end_datetime, product_type)

    return await auxip_staging.fn(
        env=env,
        cql2_filter={"filter": cql2_filter},
        catalog_collection_identifier=catalog_collection_identifier,
    )


@flow(name="Auxip unzip and decompress")
async def auxip_unzip_decompress(auxip_item: Item) -> Item:
    """Prefect flow used to unzip and decompress ADFS."""
    logger = get_run_logger()
    updated_assets = {}

    for asset_name, asset in auxip_item.assets.items():
        # After normalisation (unzip / decompress) the href is changed with the new s3 path.
        # Therefore asset name should also be updated for supported archive types.
        if asset_name.lower().endswith(ARCHIVE_SUFFIXES):
            new_href = await process_asset(asset.href, asset_name)
            asset.href = new_href
            updated_assets[strip_archive_suffix(asset_name)] = asset
        else:
            updated_assets[asset_name] = asset

    logger.info(f"Updated the following asset {updated_assets} for item {auxip_item.id}")
    auxip_item.assets = updated_assets
    return auxip_item


###########################
# Call the flows as tasks #
###########################


@task(name="Auxip search")
async def search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)


@task(name="Auxip staging")
async def auxip_staging_task(*args, **kwargs) -> tuple[bool, ItemCollection | None]:
    """See: auxip_staging"""
    return await auxip_staging.fn(*args, **kwargs)


@task(name="Auxip unzip and decompress")
async def auxip_unzip_decompress_task(*args, **kwargs) -> Item:
    """See: auxip_unzip_decompress"""
    return await auxip_unzip_decompress.fn(*args, **kwargs)
