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

"""DPR flow implementation"""

import datetime
import json
import shutil
import tempfile
import time
from datetime import timedelta

# import datetime
from os import path as osp
from pathlib import Path

import anyio
from prefect import get_run_logger, task
from pystac import Asset, Item

from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_common import prefect_utils
from rs_workflows import catalog_flow
from rs_workflows.flow_utils import DprProcessedItemMetadata, FlowEnv, FlowEnvArgs
from rs_workflows.payload_template import PayloadSchema
from rs_workflows.record_performance import record_performance_indicators
from rs_workflows.utils.utils import parse_logs


def s3_list(s3_prefix: str):
    """List all S3 objects under a prefix without downloading."""
    s3_bucket, prefix = prefect_utils.get_s3_bucket(s3_prefix)
    objects = s3_bucket._get_bucket_resource().objects  # pylint: disable=protected-access

    return [f"s3://{s3_bucket.bucket_name}/{obj.key}" for obj in objects.filter(Prefix=prefix.rstrip("/") + "/")]


def extract_products_and_zattrs(files: list[str], base_path: str):
    """Extract product names and their corresponding .zattrs file paths.

    Filters a list of file paths to find .zattrs files located under product
    directories within a base path, following one of these structures:
    base_path/.zattrs
    base_path/product_name/.zattrs
    base_path/products/product_name/.zattrs

    Args:
        files: List of file paths to search through.
        base_path: The base directory path to strip from file paths.

    Returns:
        A list of tuples, where each tuple contains:
            - product_name (str): The name of the product directory.
            - file (str): The full path to the .zattrs file.

        If ``base_path/.zattrs`` exists, it is treated as the product metadata
        and nested group metadata is ignored. Otherwise, includes files that are
        directly under a product directory, with an optional ``products``
        container directory under the base path.

    Example:
        >>> files = [
        ...     "/data/products/product_a/.zattrs",
        ...     "/data/products/product_b/.zattrs",
        ...     "/data/products/product_c/subdir/file.txt"
        ... ]
        >>> extract_products_and_zattrs(files, "/data/products")
        [('product_a', '/data/products/product_a/.zattrs'),
         ('product_b', '/data/products/product_b/.zattrs')]
    """
    dirs_and_attrs = []

    normalized_base_path = base_path.rstrip("/")

    root_zattrs = f"{normalized_base_path}/.zattrs"
    if root_zattrs in files:
        product_dirname = normalized_base_path.rsplit("/", maxsplit=1)[-1]
        return [(product_dirname, root_zattrs)]

    for file in files:
        if not file.startswith(normalized_base_path + "/"):
            continue

        rest = file[len(normalized_base_path) :].lstrip("/")  # noqa: E203
        parts = rest.split("/")

        # 1: base_path/product/.zattrs
        if len(parts) == 2 and parts[1] == ".zattrs":
            product_dirname = parts[0]
            dirs_and_attrs.append((product_dirname, file))

        # 2: base_path/products/product/.zattrs
        elif len(parts) == 3 and parts[0] == "products" and parts[2] == ".zattrs":
            product_dirname = parts[1]
            dirs_and_attrs.append((product_dirname, file))

    return dirs_and_attrs


def read_zattrs_sync(path: str):
    """Read a `.zattrs` file from S3 synchronously and return the parsed JSON, in memory."""
    return json.loads(prefect_utils.s3_read_bytes(path, _sync=True))  # type: ignore


def create_stac_item(
    eopf_origin_datetime,
    eopf_feature,
    s3_data_location,
    product_name: str,
    dpr_processor: str,
) -> Item:
    """
    Create a list of STAC Items from EOPF features and processing payload metadata.

    This function builds STAC Items compliant with EOPF constraints by:
    - Injecting EOPF-specific properties into each feature
    - Attaching output product assets
    - Propagating origin datetimes from input products

    Args:
        eopf_features (list[dict]): List of GeoJSON-like feature dictionaries.
        s3_data_location (str): Base S3 path where output products are stored.
        product_name (str): Product name
        dpr_processor (str): DPR processor name

    Returns:
        list[Item]: List of constructed STAC Item objects.
    """

    def build_item(
        feature_dict: dict,
        eopf_origin_datetimes,
        product_name,
        dpr_processor: str,
        assets: dict[str, Asset],
    ) -> Item:
        """
        Build a STAC Item from a feature dictionary.

        This function mutates the feature dictionary by injecting mandatory
        EOPF and STAC properties before constructing the Item.

        Args:
            feature_dict (dict): GeoJSON-like feature dictionary.
            eopf_origin_datetimes (str | list[str]): Origin datetime(s) derived
                from input EOPF products.

        Returns:
            Item: A STAC Item populated with geometry, properties, and extensions.
        """
        if eopf_origin_datetimes:
            feature_dict["properties"]["eopf:origin_datetime"] = eopf_origin_datetimes

        # C1.2 Ensure that all EOPF items have stac_version property set to "1.1.0"
        feature_dict["properties"]["stac_version"] = "1.1.0"

        # C1.3 Add stac_extensions following the list from the PRIP ICD §3.3.4
        # TODO: According to the 821 story, we have to:
        # - do not set stac_extension SAR for Sentinel-2 products "with instrument different from SRAL"
        # - do not set stac_extension SAR for Sentinel-3 products "with instrument different from SRAL"
        # Get in line with the story once clarified !
        stac_extensions: list[str] = []
        if dpr_processor == DprProcessor.S1L0.value:
            stac_extensions = [
                # TODO: We don't include the full list for now to avoid issues with catalog ingestion
                # This is because some extensions may require specific properties that are not properly
                # set by the DPR processor at this time.
                # "https://stac-extensions.github.io/sat/v1.1.0/schema.json",
                # "https://stac-extensions.github.io/processing/v1.2.0/schema.json",
                # "https://stac-extensions.github.io/product/v1.0.0/schema.json",
                # "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                # "https://stac-extensions.github.io/eo/v2.0.0/schema.json",
                # "https://stac-extensions.github.io/grid/v1.1.0/schema.json",
                # "https://stac-extensions.github.io/view/v1.1.0/schema.json",
                # "https://stac-extensions.github.io/sar/v1.3.0/schema.json",
                # "https://cs-si.github.io/eopf-stac-extension/v1.2.0/schema.json",
                # "https://stac-extensions.github.io/timestamps/v1.1.0/schema.json",
                # "https://stac-extensions.github.io/authentication/v1.1.0/schema.json",
            ]

        return Item(
            id=product_name,
            geometry=feature_dict.get("geometry"),
            bbox=feature_dict.get("bbox"),
            datetime=datetime.datetime.fromisoformat(feature_dict["properties"]["datetime"]),
            properties=feature_dict["properties"],
            stac_extensions=stac_extensions,
            assets=assets,
        )

    def build_asset(path: str, product_name: str) -> Asset:
        """
        Build a STAC Asset representing a Zarr output product.

        Args:
            path (str): Full path or URL to the asset.
            title (str): Human-readable asset title.

        Returns:
            Asset: A STAC Asset configured for EOPF output products.
        """
        return Asset(
            href=path.replace("/.zattrs", ""),
            title=product_name,
            media_type="application/vnd+zarr",
            roles=["data", "metadata"],
            # TODO: The story RSPY-280 is implemented in the catalog to fill the auth:ref field
            # extra_fields={
            #     "auth:ref": "should be filled thanks to story RSPY-280",
            # },
        )

    item = build_item(
        eopf_feature,
        eopf_origin_datetime,
        product_name,
        dpr_processor,
        assets={product_name: build_asset(s3_data_location, product_name)},
    )
    return item


def clean_paths(paths: list[str], logger) -> None:
    """Delete directories or files listed in paths, logging outcomes.

    Args:
        paths: List of filesystem paths to remove.
        logger: Prefect logger for informational messages.
    """
    logger.info(f"Cleaning up temporary paths: {paths}")
    for path in paths:
        try:
            if not osp.exists(path):
                logger.warning(f"Autoclean: path does not exist {path}, skipping.")
                continue
            if osp.isdir(path):
                shutil.rmtree(path)
                logger.info(f"Autoclean: removed directory {path}")
            else:
                logger.warning(f"Autoclean: expected directory but found file {path}, skipping.")
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Autoclean failed for the shared path {path}: {e}")


@task(name="Update eopf assets")
def update_eopf_assets(
    env,
    input_products: list[dict],
    payload: PayloadSchema,
    dpr_processor: str,
) -> list[DprProcessedItemMetadata]:
    """Update EOPF assets by extracting metadata and creating STAC items.

    This Prefect task processes output products from a DPR (Data Processing Request)
    workflow, extracts EOPF (Earth Observation Processing Framework) metadata from
    .zattrs files, and generates STAC (SpatioTemporal Asset Catalog) items for
    each discovered product.

    Workflow:
        1. Lists all .zattrs files in the output product paths
        2. Reads and validates EOPF discovery metadata from each .zattrs file
        3. Extracts product type information
        4. Creates corresponding STAC items for catalog registration

    Args:
        env: Environment configuration object containing runtime settings.
        input_products: List of dictionaries representing input product metadata.
        payload: PayloadSchema object containing I/O configuration, including
            output_product paths to scan for .zattrs files.
        dpr_processor: str
            DPR processor name

    Returns:
        A list of DprProcessedItemMetadata objects, each containing:
            - stac_item (Item): STAC item created from EOPF metadata.
            - product_type (str): Product type extracted from the .zattrs file.
            - output_product_id (str): The ID of the original output product.

    Raises:
        RuntimeError: If any .zattrs file cannot be read or does not contain
            required EOPF discovery metadata (stac_discovery.properties).

    Notes:
        - Requires .zattrs files to follow EOPF metadata conventions
        - Each .zattrs file must contain stac_discovery.properties.product:type
        - Uses S3 storage backend (via s3_list and read_zattrs_sync functions)
    """
    logger = get_run_logger()
    logger.info("Starting EOPF asset update.")
    logger.info(f"Payload received: {payload}")
    logger.info(f"Input products: {input_products}")

    if payload.io is None:
        raise RuntimeError("Payload I/O configuration is missing.")
    # Get all .zattrs files found in the output products paths
    zattrs_list = []
    for prod in payload.io.output_products:
        path = prod.path
        # Keep track of which output product ID each .zattrs belongs to
        new_zattrs = extract_products_and_zattrs(s3_list(path), path)
        for name, loc in new_zattrs:
            zattrs_list.append((name, loc, prod.id))
        logger.info(
            f"The output product section {prod.id} has been added to the list, "
            "and all product types found at this path will be published to the catalog. "
            f"The path is {path}",
        )

    # List & extract
    logger.info(
        f"Found {len(zattrs_list)} .zattrs files from {len(payload.io.output_products)} "
        f"output product sections from payload. The list with products to be published: {zattrs_list}",
    )

    # C1.1 Add the property eopf:origin_datetime with value equal to the maximum
    # eopf:origin_datetime among all input products (excluding ADFS inputs)
    # Note: input_products != input_adfs
    # disabled for mockup
    if dpr_processor.lower() in ["mockup"]:
        eopf_origin_datetime = "2026-01-01T00:00:00Z"
    elif input_products and zattrs_list:
        eopf_origin_datetime = compute_eopf_origin_datetime(env, input_products)
    else:
        eopf_origin_datetime = None

    items_metadata = []
    for product_name, zattrs_s3_location, output_product_id in zattrs_list:
        logger.info(f"Product = {product_name} | zattrs = {zattrs_s3_location}")
        # Read metadata
        zattrs_data = read_zattrs_sync(zattrs_s3_location)
        if not zattrs_data:
            logger.error(f"Could not read .zattrs file {zattrs_s3_location}. Exiting.")
            raise RuntimeError(f"Could not read .zattrs file {zattrs_s3_location}. Exiting.")
        logger.info(f"DPR processor output {zattrs_data}")

        # Extract EOPF info
        if "stac_discovery" not in zattrs_data or "properties" not in zattrs_data["stac_discovery"]:
            logger.error(f".zattrs file {zattrs_s3_location} does not contain EOPF discovery metadata. Exiting.")
            raise RuntimeError(f".zattrs file {zattrs_s3_location} does not contain EOPF discovery metadata. Exiting.")

        product_type = zattrs_data["stac_discovery"]["properties"].get("product:type", None)
        logger.info(f"Extracted EOPF product type: {product_type}")

        eopf_item = zattrs_data["stac_discovery"]
        logger.debug(f"EOPF discovery metadata extracted: {eopf_item}")

        # Build STAC items
        stac_item = create_stac_item(eopf_origin_datetime, eopf_item, zattrs_s3_location, product_name, dpr_processor)

        items_metadata.append(
            DprProcessedItemMetadata(
                stac_item=stac_item,
                product_type=product_type,
                output_product_id=output_product_id,
            ),
        )
        logger.info(f"Added one stac item metadata to the already existing list. Length: {len(items_metadata)}.")

    logger.info(f"List with stac_items to be published: {items_metadata}.")
    return items_metadata


def compute_eopf_origin_datetime(env, input_products) -> str:
    """
    Compute the maximum ``eopf:origin_datetime`` across all input products.

    For each input product, this function retrieves the corresponding item
    from the catalog using its item ID and collection ID, extracts the
    ``eopf:origin_datetime`` property, and returns the latest (maximum)
    datetime value found.

    If an item cannot be retrieved from the catalog, the error is logged
    and processing continues with the remaining products.

    Parameters
    ----------
    env : object
        Execution environment object used to serialize and pass context
        to the catalog flow.
    input_products : Iterable[dict]
        Iterable of input product mappings. Each mapping is expected to
        contain values of the form ``(item_id, collection_id)``.

    Returns
    -------
    str
        ISO 8601 string representing the maximum ``eopf:origin_datetime``
        found among all retrieved items. If no valid items are found,
        returns the fallback value ``"2023-01-01T00:00:00Z"``.
    """
    logger = get_run_logger()
    items = []
    if not input_products:
        logger.error("No valid input products found to compute eopf:origin_datetime. Exit")
        raise RuntimeError("No valid input products found to compute eopf:origin_datetime")

    for input_product in input_products:
        item_id = input_product.item_id
        collection_name = input_product.collection_name
        try:
            future = catalog_flow.get_item.submit(
                env.serialize(),
                collection_name,
                item_id,
            )
            if not future.result():
                logger.error(
                    f"Expected valid input product item {item_id} was not found"
                    " to compute eopf:origin_datetime. Exit",
                )
                raise RuntimeError(
                    f"Expected valid input product item {item_id} was not found to compute eopf:origin_datetime",
                )
            items.append(future.result())
        except RuntimeError as rte:
            logger.exception(f"Failed to get item '{item_id}' from collection '{collection_name}'")
            raise RuntimeError("No valid items found to compute eopf:origin_datetime") from rte

    logger.info(f"Items matching input found in catalog: {len(items)}")

    dates = [
        datetime.datetime.fromisoformat(origin_dt.replace("Z", "+00:00"))
        for item in items
        if (origin_dt := item.to_dict().get("properties", {}).get("eopf:origin_datetime"))
    ]

    try:
        max_eopf_datetime = max(dates).isoformat()
    except ValueError as ve:
        logger.exception("Failed to compute maximum eopf:origin_datetime")
        raise ValueError("Maximum eopf datetime could not be computed") from ve

    logger.info(f"Maximum eopf datetime computed from all items is {max_eopf_datetime}")
    return max_eopf_datetime


@task(name="run-processor")
async def run_processor(
    env: FlowEnvArgs,
    processor: str,
    payload: PayloadSchema,
    cluster_info: ClusterInfo,
    s3_payload_run: str,
    input_products: list[dict],
) -> list[DprProcessedItemMetadata]:
    """
    Run the DPR processor.

    Args:
        env: Prefect flow environment
        processor: DPR processor name
        s3_payload_run: S3 bucket location of the output final DPR payload file.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "run-processor"):
        if payload.io is None:
            raise ValueError("Payload I/O configuration is missing.")
        # First, remove the output products that are not final products from
        # the payload to avoid triggering the catalog registration for them
        # Create a temporary list for keeping track of products to keep
        kept_products = []
        # List of paths to delete if autoclean is enabled
        paths_to_delete: list[str] = []

        # Iterate over the original products
        for prod in payload.io.output_products:
            if prod.final_product:
                kept_products.append(prod)
            else:
                logger.info(f"Output product {prod.id} is not marked as final_product, skipping catalog registration.")
            # Record autoclean path for any product with autoclean=True
            if prod.autoclean and prod.path not in paths_to_delete:
                paths_to_delete.append(prod.path)

        # Update the original output_products list with the kept products
        payload.io.output_products[:] = kept_products

        record_performance_indicators(  # type: ignore
            start_date=datetime.datetime.now(),
            status="OK",
            dpr_processing_input_stac_items=s3_payload_run,
            payload=payload,
            dpr_processor_name=processor,
        )
        # Trigger the processor run from the dpr service
        dpr_client: DprClient = flow_env.rs_client.get_dpr_client()
        start_time = time.time()
        s3_payload_dir = osp.dirname(s3_payload_run)
        s3_payload_filename = osp.basename(s3_payload_run)
        logger.info(f"Triggering DPR processor {processor!r}")
        job_status = dpr_client.run_process(
            process=processor,
            cluster_info=cluster_info,
            s3_config_dir=s3_payload_dir,
            payload_subpath=s3_payload_filename,
            s3_report_dir=s3_payload_dir,
        )
        try:
            dpr_client.wait_for_job(job_status, logger, f"{processor!r} processor")
        finally:
            logger.info(f"Processor execution time: {str(timedelta(seconds=time.time() - start_time))}")
            # Download reports folder from the s3 bucket
            with tempfile.TemporaryDirectory() as tmpdir:
                await prefect_utils.s3_download_dir(s3_payload_dir, tmpdir)

                # Display here the log from eopf processors if it exists in the reports folder.
                # We search for a log file that shares the same name as the payload file, but
                # has the suffix ".processor.log". This approach is consistent with the current implementation
                # of the rs-dpr-service, which creates a subfolder named "reports" in the same directory as
                # the payload file. The processor log filename will be built by the rs-dpr-service
                # by using the same base name as the payload file, but with the addition of the
                # ".processor.log" suffix instead of ".yaml".
                local_log_file = osp.join(
                    tmpdir,
                    "reports",
                    Path(s3_payload_filename).with_suffix(".processor.log").name,
                )
                try:
                    async with await anyio.open_file(local_log_file, encoding="utf-8") as opened:

                        s3_log_file = await opened.read()

                        # Parse each line from s3_log_file and display it asa a Prefect log level
                        for entry in parse_logs(s3_log_file):

                            level = entry["level"].strip().lower()
                            getattr(logger, level, logger.info)(entry["message"])

                except FileNotFoundError:
                    logger.info(f"No processor log file was uploaded under: {s3_payload_dir!r}")
            # After processing, clean up autoclean paths. IMPORTANT : the shared disk has to be mounted
            # in the current flow environment (prefect worker) for this to work ! So, the shared_disk has to be
            # mounted in both dask worker environment (where the processor runs) and in the prefect worker
            # environment (where this flow runs)
            clean_paths(paths_to_delete, logger)

        items_metadata = update_eopf_assets(flow_env, input_products, payload, processor)
        eopf_stac_items = [asset.stac_item for asset in items_metadata]
        eopf_types = [asset.product_type for asset in items_metadata]

        # Wait for the job to finish
        record_performance_indicators(  # type: ignore
            stop_date=datetime.datetime.now(),
            status="OK",
            stac_items=eopf_stac_items,
            payload=payload,
            dpr_processor_name=processor,
            eopf_types=eopf_types,
        )
        return items_metadata
