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

"""Utility module for the Prefect flows."""

import json
import os
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Span, SpanContext
from opentelemetry.util._decorator import _agnosticcontextmanager
from prefect import get_run_logger, task
from pystac import Asset, Item

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_client.rs_client import RsClient
from rs_common import init_opentelemetry, prefect_utils


class Priority(str, Enum):
    """
    Priority for the cluster dask to be able to prioritise task execution.
    """

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class WorkflowType(str, Enum):
    """
    Workflow type.
    """

    BENCHMARKING = "benchmarking"
    ON_DEMAND = "on-demand"
    SYSTEMATIC = "systematic"


class ProcessingMode(str, Enum):
    """
    List of mode to be applied when calling the DPR processor.
    """

    NRT = "nrt"
    NTC = "ntc"
    REPROCESSING = "reprocessing"
    SUBS = "subs"
    ALWAYS = "always"


@dataclass
class FlowEnvArgs:
    """
    Prefect flow environment arguments.

    Attributes:
        owner_id: User/owner ID (necessary to retrieve the user info: API key and OAuth2 cookie)
        from the right Prefect block. NOTE: may be useless after each user has their own prefect
        server because there will be only one block.
        calling_span (tuple): Serialized OpenTelemetry span of the calling flow, if any.
    """

    owner_id: str
    calling_span: tuple[int, int, bool] | None = None


class FlowEnv:
    """
    Prefect flow environment and reusable objects.

    Attributes:
        owner_id (str): User/owner ID
        calling_span (SpanContext | None): OpenTelemetry span of the calling flow, if any.
        this_span (SpanContext | None): Current OpenTelemetry span.
        rs_client (RsClient): RsClient instance
    """

    def __init__(self, args: FlowEnvArgs):
        """Constructor."""
        self.owner_id: str = args.owner_id
        self.calling_span: SpanContext | None = None
        self.this_span: SpanContext | None = None

        # Deserialize the calling span, if any
        if args.calling_span:
            self.calling_span = SpanContext(*args.calling_span)

        # Read prefect blocks into env vars
        prefect_utils.read_prefect_blocks(self.owner_id, _sync=True)  # type: ignore

        # Init opentelemetry traces
        init_opentelemetry.init_traces("rs.client")

        # Init the RsClient instance from the env vars
        self.rs_client = RsClient(
            rs_server_href=os.getenv("RSPY_WEBSITE"),
            rs_server_api_key=os.getenv("RSPY_APIKEY"),
            owner_id=self.owner_id,
            logger=get_run_logger(),  # type: ignore
        )

    def serialize(self) -> FlowEnvArgs:
        """Serialize this object with Pydantic."""

        # The serialized object will be used by a new opentelemetry span.
        # Its calling span will be either the current span, or the current calling span.
        new_calling_span = self.this_span or self.calling_span
        if new_calling_span:
            # Only keep the first n attributes, the other need custom serialization
            serialized_span = tuple(new_calling_span)[:3]
        else:
            serialized_span = None

        return FlowEnvArgs(owner_id=self.owner_id, calling_span=serialized_span)  # type: ignore

    @_agnosticcontextmanager
    def start_span(
        self,
        instrumenting_module_name: str,
        name: str,
    ) -> Iterator[Span]:
        """
        Context manager for creating a new main or child OpenTelemetry span and set it
        as the current span in this tracer's context.

        Args:
            instrumenting_module_name: Caller module name, just pass __name__
            name: The name of the span to be created (use a custom name)

        Yields:
            The newly-created span.
        """
        # Create new span and save it
        with init_opentelemetry.start_span(  # pylint: disable=contextmanager-generator-missing-cleanup
            instrumenting_module_name,
            name,
            self.calling_span,
        ) as span:
            self.this_span = trace.get_current_span().get_span_context()
            yield span


@dataclass
class DprProcessIn:  # pylint: disable=too-many-instance-attributes
    """
    Input parameters for the 'dpr-process' flow
    """

    env: FlowEnvArgs
    processor_name: DprProcessor
    processor_version: str
    dask_cluster_label: str
    s3_payload_file: str
    # 'pipeline' or 'unit' must be provided
    pipeline: str | None = None
    unit: str | None = None

    priority: Priority = Priority.LOW
    workflow_type: WorkflowType = WorkflowType.ON_DEMAND

    input_products: list[dict[str, tuple[str, str]]] = field(default_factory=list)
    generated_product_to_collection_identifier: list[dict[str, str | tuple[str, str]]] = field(default_factory=list)
    auxiliary_product_to_collection_identifier: dict[str, str] = field(default_factory=dict)

    processing_mode: list[ProcessingMode] = field(default_factory=list)
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    satellite: str | None = None

    def __post_init__(self) -> None:
        # Enforce the "pipeline XOR unit" rule
        has_pipeline = bool(self.pipeline)
        has_unit = bool(self.unit)
        if has_pipeline == has_unit:
            raise ValueError("Exactly one of 'pipeline' or 'unit' must be provided.")

        # if not self.input_products:
        #    raise ValueError("'input_products' must contain at least one pystac.Item.")

        if not self.generated_product_to_collection_identifier:
            raise ValueError("'generated_product_to_collection_identifier' must not be empty.")

        if not self.auxiliary_product_to_collection_identifier:
            raise ValueError("'auxiliary_product_to_collection_identifier' must not be empty.")


@dataclass
class DprProcessOut:
    """
    Output parameters for the 'dpr-process' flow
    """

    status: bool
    product_identifier: list[Item] = field(default_factory=list)


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

        rest = f[len(base_path) :].lstrip("/")
        parts = rest.split("/")

        if len(parts) < 2:
            continue

        product_name = parts[0]

        # 1: base_path/product/.zattrs
        if len(parts) == 2 and parts[1] == ".zattrs":
            products.add(product_name)
            zattrs.append(f)

        # 2: base_path/product/product/.zattrs
        elif len(parts) == 3 and parts[1] == product_name and parts[2] == ".zattrs":
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
    **download_kwargs: dict[str, Any],
) -> Path:
    """
    Download a file from S3 synchronously.
    """
    s3_bucket, from_path = prefect_utils.get_s3_bucket(s3_path)
    s3_bucket.download_object_to_path(from_path, str(to_path), **download_kwargs)
    return to_path


def create_stac_items(payload, eopf_features):
    """
    Create a list of STAC Items from EOPF features and processing payload metadata.

    This function builds STAC Items compliant with EOPF constraints by:
    - Injecting EOPF-specific properties into each feature
    - Attaching output product assets
    - Propagating origin datetimes from input products

    Args:
        payload (dict): Processing payload containing input and output product metadata.
        eopf_features (list[dict]): List of GeoJSON-like feature dictionaries.

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
        default_stac_extensions = [
            # "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
            # "https://stac-extensions.github.io/authentication/v1.1.0/schema.json",
            # "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            # "https://stac-extensions.github.io/product/v0.1.0/schema.json",
        ]

        return Item(
            id=feature_dict["id"],
            geometry=feature_dict["geometry"],
            bbox=feature_dict["bbox"],
            datetime=datetime.fromisoformat(feature_dict["properties"]["datetime"]),
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

    # Collect output product paths
    paths = {prod["path"] for prod in payload["I/O"]["output_products"]}
    path = next(iter(paths))

    # C1.1 Add the property eopf:origin_datetime with value equal to the maximum
    # eopf:origin_datetime among all input products (excluding ADFS inputs)
    # eopf_origin_datetimes = compute_eopf_origin_datetimes(payload)  # TODO
    eopf_origin_datetimes = "datetimelateraddedhere"  # TODO

    items = []
    for feature_dict in eopf_features:
        item = build_item(feature_dict, eopf_origin_datetimes)
        title = f"{item.id}.zarr"
        item.assets = {title: build_asset(f"{path}{title}", title)}
        items.append(item)

    return items


@task(name="Update eopf assets")
def update_eopf_assets(payload: dict) -> list[dict]:
    """
    Extract EOPF metadata from S3 paths found in the payload, read all `.zattrs`
    files associated with the products, and generate corresponding STAC items.

    Steps performed:
    1. Determine the unique S3 path from the output products in the payload.
    2. List all files under that path and extract product files and `.zattrs` files.
    3. Read `.zattrs` metadata synchronously.
    4. Collect EOPF item metadata and product types.
    5. Build and return STAC items together with extracted EOPF product types.

    Args:
        payload: A dictionary containing the workflow input/output structure,
                 specifically under `payload["I/O"]["output_products"]`.

    Returns:
        A tuple (stac_items, eopf_types):
            - stac_items: A list of STAC items constructed from the EOPF metadata.
            - eopf_types: A list of extracted product types (strings).
    """
    logger = get_run_logger()

    logger.info("Starting EOPF asset update.")
    logger.debug(f"Payload received: {payload}")

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

    # Extract EOPF info
    eopf_types = [attrs["data"]["stac_discovery"]["properties"]["product:type"] for attrs in zattrs_data]
    logger.info(f"Extracted EOPF product types: {eopf_types}")

    eopf_items = [attrs["data"]["stac_discovery"] for attrs in zattrs_data]
    logger.debug(f"EOPF discovery metadata extracted: {eopf_items}")

    # Build STAC items
    stac_items = create_stac_items(payload, eopf_items)
    logger.info(f"Created {len(stac_items)} STAC items.")

    return stac_items, eopf_types
