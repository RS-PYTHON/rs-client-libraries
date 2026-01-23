# Copyright 2025 CS Group
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
import tempfile

# import datetime
from os import path as osp
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from prefect import get_run_logger, task
from pystac import Asset, Item

from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_common import prefect_utils
from rs_workflows import catalog_flow
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.record_performance import record_performance_indicators


def s3_list(s3_prefix: str):
    """List all S3 objects under a prefix without downloading."""
    s3_bucket, prefix = prefect_utils.get_s3_bucket(s3_prefix)
    objects = s3_bucket._get_bucket_resource().objects  # pylint: disable=protected-access

    return [f"s3://{s3_bucket.bucket_name}/{obj.key}" for obj in objects.filter(Prefix=prefix.rstrip("/") + "/")]


def extract_products_and_zattrs(files: list[str], base_path: str):
    """
    Extract product names and associated .zattrs files from a list of file paths.

    This function scans a list of file paths and identifies Zarr products by
    detecting valid `.zattrs` files under the given base path. It supports both
    common Zarr layouts:
    1. base_path/<product>/.zattrs
    2. base_path/<product>/<product>/.zattrs

    Args:
        files (list[str]): List of file paths to scan.
        base_path (str): Base directory under which products are located.

    Returns:
        tuple[list[str], list[str]]:
            - A list of unique product names discovered.
            - A list of full paths to detected `.zattrs` files.
    """
    products = set()
    zattrs = []

    for f in files:
        if not f.startswith(base_path):
            continue

        rest = f[len(base_path) :].lstrip("/")  # noqa: E203
        parts = rest.split("/")

        if len(parts) < 2:
            continue

        product_name = parts[0]

        # 1: base_path/product/.zattrs
        if len(parts) == 2 and parts[1] == ".zattrs":
            products.add(product_name)
            zattrs.append(f)

        # 2: base_path/product/product/.zattrs
        elif len(parts) == 3 and parts[1] == product_name and parts[2] == ".zattrs":  # noqa: SIM114
            products.add(product_name)
            zattrs.append(f)

    return list(products), zattrs


def read_zattrs_sync(zattrs_paths: list[str]):
    """
    Download `.zattrs` files synchronously using prefect_utils.s3_download_file
    and return parsed JSON dicts in memory.
    """
    results = []
    for path in zattrs_paths:
        with tempfile.NamedTemporaryFile() as temp:
            s3_download_file_sync(path, str(temp.name), _sync=True)
            with open(temp.name, encoding="utf-8") as f:
                data = json.load(f)
        results.append({"path": path, "data": data})
    return results


def s3_download_file_sync(
    s3_path: str,
    to_path: str | Path,
    **download_kwargs: Any,
) -> str | Path:
    """
    Download a file from S3 synchronously.
    """
    s3_bucket, from_path = prefect_utils.get_s3_bucket(s3_path)
    s3_bucket.download_object_to_path(from_path, str(to_path), **download_kwargs)
    return to_path


def create_stac_items(env, input_products, eopf_features, s3_data_location):
    """
    Create a list of STAC Items from EOPF features and processing payload metadata.

    This function builds STAC Items compliant with EOPF constraints by:
    - Injecting EOPF-specific properties into each feature
    - Attaching output product assets
    - Propagating origin datetimes from input products

    Args:
        eopf_features (list[dict]): List of GeoJSON-like feature dictionaries.
        s3_data_location (str): Base S3 path where output products are stored.

    Returns:
        list[Item]: List of constructed STAC Item objects.
    """

    def build_item(feature_dict: dict, eopf_origin_datetimes) -> Item:
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
        feature_dict["properties"]["eopf:origin_datetime"] = eopf_origin_datetimes

        # C1.2 Ensure that all EOPF items have stac_version property set to "1.1.0"
        feature_dict["properties"]["stac_version"] = "1.1.0"

        # C1.3 Add stac_extensions following the list from the PRIP ICD §3.3.4
        default_stac_extensions: list[str] = [
            # "https://github.com/stac-extensions/sat",
            # "https://github.com/stac-extensions/product",
            # "https://github.com/stac-extensions/grid",
            # "https://github.com/stac-extensions/processing",
            # "https://github.com/stac-extensions/eo",
            # "https://github.com/stac-extensions/view",
            # "https://github.com/stac-extensions/timestamps",
            # "https://github.com/CS-SI/eopf-stac-extension"
        ]

        return Item(
            id=feature_dict["id"],
            geometry=feature_dict["geometry"],
            bbox=feature_dict["bbox"],
            datetime=datetime.datetime.fromisoformat(feature_dict["properties"]["datetime"]),
            properties=feature_dict["properties"],
            stac_extensions=default_stac_extensions,
        )

    def build_asset(path: str, title: str) -> Asset:
        """
        Build a STAC Asset representing a Zarr output product.

        Args:
            path (str): Full path or URL to the asset.
            title (str): Human-readable asset title.

        Returns:
            Asset: A STAC Asset configured for EOPF output products.
        """
        return Asset(
            href=path,
            title=title,
            media_type="application/vnd+zarr",
            roles=["data", "metadata"],
            extra_fields={
                "file:local_path": path,
                "auth:ref": "should be filled thanks to story RSPY-280",
            },
        )

    # C1.1 Add the property eopf:origin_datetime with value equal to the maximum
    # eopf:origin_datetime among all input products (excluding ADFS inputs)
    # Note: input_products != input_adfs
    eopf_origin_datetimes = compute_eopf_origin_datetimes(env, input_products)

    items = []
    for feature_dict in eopf_features:
        item = build_item(feature_dict, eopf_origin_datetimes)
        title = f"{item.id}.zarr"
        item.assets = {title: build_asset(s3_data_location, title)}
        items.append(item)

    return items


def zarr_root_from_s3(base_s3_uri: str, internal_s3_path: str) -> str:
    """Extract the Zarr root path from an internal S3 path."""
    parsed = urlparse(base_s3_uri)

    base_path = PurePosixPath(parsed.path.lstrip("/"))
    internal_path = PurePosixPath(internal_s3_path)
    # !
    relative = internal_path.relative_to(base_path)
    zarr_root = next(p for p in relative.parents if p.name.endswith(".zarr"))

    return f"{parsed.scheme}://{parsed.netloc}/{base_path / zarr_root}"


@task(name="Update eopf assets")
def update_eopf_assets(env, input_products: list[dict], payload: dict) -> tuple[Any, Any]:
    """
    Update EOPF-related STAC assets by discovering products in S3 and
    generating corresponding STAC items from `.zattrs` metadata.

    This task inspects the workflow output products defined in the payload,
    identifies the unique S3 base path, and discovers all product files and
    associated `.zattrs` metadata files under that path. The `.zattrs`
    metadata is read synchronously and used to extract EOPF discovery
    information and product types, which are then converted into STAC items.

    Steps performed:
    1. Extract the unique S3 base path from ``payload["I/O"]["output_products"]``.
    2. List all files under the resolved S3 path.
    3. Separate product files from `.zattrs` metadata files.
    4. Read and parse `.zattrs` metadata synchronously.
    5. Extract EOPF discovery metadata and EOPF product types.
    6. Build STAC items using the extracted metadata and input products.

    Parameters
    ----------
    env : object
        Execution environment used to provide context when creating STAC items.
    input_products : list[dict]
        List of input product mappings used to relate generated STAC items
        to their upstream products.
    payload : dict
        Workflow payload containing input/output definitions. The S3 discovery
        path is read from ``payload["I/O"]["output_products"]``.

    Returns
    -------
    tuple
        A tuple ``(stac_items, eopf_types)`` where:
        - stac_items : list
            List of STAC items generated from EOPF `.zattrs` discovery metadata.
        - eopf_types : list[str]
            List of extracted EOPF product type identifiers.
    """
    logger = get_run_logger()
    logger.info("Starting EOPF asset update.")
    logger.debug(f"Payload received: {payload}")
    logger.info("Input products: %s", input_products)
    # Determine path
    paths = {prod["path"] for prod in payload["I/O"]["output_products"]}
    path = next(iter(paths))
    logger.info(f"Using S3 path: {path}")

    # List & extract
    all_files = s3_list(path)
    logger.info(f"Found {len(all_files)} files under path.")
    products, zattrs = extract_products_and_zattrs(all_files, path)
    logger.info(f"Extracted {len(products)} product files and {len(zattrs)} .zattrs files.")

    # Read metadata
    zattrs_data = read_zattrs_sync(zattrs)
    logger.info(f"Loaded metadata from {len(zattrs_data)} .zattrs files.")
    s3_data_location = zarr_root_from_s3(path, zattrs)
    # Extract EOPF info
    eopf_types = [attrs["data"]["stac_discovery"]["properties"]["product:type"] for attrs in zattrs_data]
    logger.info(f"Extracted EOPF product types: {eopf_types}")

    eopf_items = [attrs["data"]["stac_discovery"] for attrs in zattrs_data]
    logger.debug(f"EOPF discovery metadata extracted: {eopf_items}")

    # Build STAC items
    stac_items = create_stac_items(env, input_products, eopf_items, s3_data_location)
    logger.info(f"Created {len(stac_items)} STAC items.")

    return stac_items, eopf_types


def compute_eopf_origin_datetimes(env, input_products):
    """
    Compute the maximum ``eopf:origin_datetime`` across all input CADU products.

    For each input product, this function retrieves the corresponding item
    from the catalog using its CADU ID and collection ID, extracts the
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
        contain values of the form ``(cadu_id, collection_id)``.

    Returns
    -------
    str
        ISO 8601 string representing the maximum ``eopf:origin_datetime``
        found among all retrieved items. If no valid items are found,
        returns the fallback value ``"2023-01-01T00:00:00Z"``.
    """
    logger = get_run_logger()
    cadu_items = []

    for input_product in input_products:
        for _, (cadu_id, collection_id) in input_product.items():
            try:
                future = catalog_flow.get_item.submit(
                    env.serialize(),
                    collection_id,
                    cadu_id,
                )
                cadu_items.append(future.result())
            except RuntimeError:
                logger.exception(f"Failed to get CADU item '{cadu_id}' from collection '{collection_id}'")

    logger.info(f"Items matching input found in catalog: {len(cadu_items)}")

    if not cadu_items:
        # error maybe?
        logger.warning("No valid CADU items found, returning fallback datetime")
        return "2023-01-01T00:00:00Z"

    max_eopf_datetime = max(
        datetime.datetime.fromisoformat(
            item.to_dict()["properties"]["eopf:origin_datetime"].replace("Z", "+00:00"),  # type: ignore
        )
        for item in cadu_items
    ).isoformat()

    logger.info(f"Maximum eopf datetime computed from all items is {max_eopf_datetime}")
    return max_eopf_datetime


@task(name="Run DPR processor")
async def run_processor(
    env: FlowEnvArgs,
    processor: DprProcessor,
    payload: dict,
    cluster_info: ClusterInfo,
    s3_payload_run: str,
    input_products: list[dict],
) -> list[dict]:
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
        record_performance_indicators(  # type: ignore
            start_date=datetime.datetime.now(),
            status="OK",
            dpr_processing_input_stac_items=s3_payload_run,
            payload=payload,
            dpr_processor_name=processor.value,
        )
        # Trigger the processor run from the dpr service
        dpr_client: DprClient = flow_env.rs_client.get_dpr_client()
        job_status = dpr_client.run_process(
            process=processor,
            cluster_info=cluster_info,
            s3_config_dir=osp.dirname(s3_payload_run),
            payload_subpath=osp.basename(s3_payload_run),
            s3_report_dir=osp.join(osp.dirname(s3_payload_run)),
        )
        dpr_job = dpr_client.wait_for_job(job_status, logger, f"{processor.value!r} processor")

        logger.info(f"DPR processor output {dpr_job}")
        eopf_stac_items, eopf_types = update_eopf_assets(flow_env, input_products, payload)
        # Wait for the job to finish
        record_performance_indicators(  # type: ignore
            stop_date=datetime.datetime.now(),
            status="OK",
            stac_items=eopf_stac_items,
            payload=payload,
            eopf_types=eopf_types,
        )
        return eopf_stac_items
