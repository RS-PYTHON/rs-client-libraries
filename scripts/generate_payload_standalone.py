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
Standalone DPR payload generator.

Does the same preparatory work as the generic processing flow (rs_workflows.on_demand_processing),
without Prefect (no Prefect server, blocks or variables) and without the rs-dpr-service:

1. read the task table from a local JSON file
2. build the list of processing units for the requested pipeline or unit
3. for each input ADFS, render the task table query, search the matching auxiliary files
   and stage them into the catalog (archives are unzipped and the catalog item updated)
4. generate the processor payload for the given input zarr products

The input products are given as zarr paths (on S3 or local). Their STAC metadata is read
from the zarr root attributes ('stac_discovery'), to resolve the task table external variables
and the ADFS queries depending on input product properties (multiplicity 'one_per_input').

Environment variables:
    RSPY_WEBSITE, RSPY_APIKEY: rs-server URL and API key (search and staging of the ADFS)
    RSPY_HOST_USER: default owner ID
    S3_ACCESSKEY, S3_SECRETKEY, S3_ENDPOINT, S3_REGION: S3 access to read the zarr inputs metadata,
        normalize staged archives, and resolve the storage configuration secrets
    RSPY_HOST_OSAM: rs-osam URL to read the output bucket configuration, if --output-bucket is not given

Example:
    python scripts/generate_payload_standalone.py \\
        --tasktable TaskTable_S3_L1_OLCI.json --pipeline ol1_eo \\
        --input S03OLCL0=s3://rs-bucket/S3A_OL_0_EFR____20250101T101010_....zarr \\
        --aux-mapping "*=s3-aux" --output-mapping S03OLCEFR=S03OLCEFR:s3-l1-olci \\
        --output-bucket rs-dev-bucket -o payload.yaml
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml
from pystac import Item, ItemCollection

from rs_client.rs_client import RsClient
from rs_common import prefect_utils
from rs_workflows.aux_flow import select_search_client_and_kwargs
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    AuxiliarySource,
    DprProcessIn,
    FlowEnvArgs,
    FlowGeneratedProduct,
    FlowInputProduct,
    LoggingLevel,
)
from rs_workflows.on_demand_processing import (
    render_aux_cql2,
    resolve_specific_input_product_stac_items,
    select_aux_collection_and_source,
)
from rs_workflows.payload_builder import build_unit_list, extract_external_modules
from rs_workflows.payload_generator import (
    CONFIG_DIR,
    build_payload,
    fetch_csv_from_endpoint,
)
from rs_workflows.payload_template import PayloadSchema
from rs_workflows.storage_configuration import StorageConfig
from rs_workflows.utils.utils import (
    asset_unzip_decompress,
    get_archived_item_indexes,
    search_by_name,
)

logger = logging.getLogger("generate_payload_standalone")

# Fake collection name used for the input products, which are not read from the catalog
INPUT_COLLECTION = "standalone-inputs"
ZARR_MEDIA_TYPE = "application/vnd+zarr"


#################
# Input products #
#################


async def read_zarr_attributes(zarr_path: str) -> dict[str, Any]:
    """
    Read the root attributes of a zarr product, located on S3 or on the local disk.
    Both zarr v3 ('zarr.json') and zarr v2 ('.zattrs') layouts are supported.
    """
    for filename, attrs_key in (("zarr.json", "attributes"), (".zattrs", None)):
        metadata_path = f"{zarr_path.rstrip('/')}/{filename}"
        try:
            if metadata_path.startswith("s3://"):
                s3_bucket, key = prefect_utils.get_s3_bucket(metadata_path)
                content = await s3_bucket.aread_path(key)
            else:
                content = Path(metadata_path).read_bytes()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.info(f"No zarr metadata found at {metadata_path}: {exc}")
            continue
        metadata = json.loads(content)
        return (metadata.get(attrs_key) or {}) if attrs_key else metadata
    logger.warning(f"⚠️ Unable to read zarr metadata from '{zarr_path}', its STAC properties will be empty")
    return {}


def build_input_item(zarr_path: str, attributes: dict[str, Any]) -> Item:
    """
    Build an in-memory STAC item for an input zarr product, from the 'stac_discovery' section
    of its root attributes. The item has a single zarr asset pointing to the given path.
    """
    stac_discovery = attributes.get("stac_discovery") or {}
    properties = dict(stac_discovery.get("properties") or {})
    if not properties.get("datetime"):
        properties["datetime"] = properties.get("start_datetime") or datetime.now(timezone.utc).isoformat()
    item_id = stac_discovery.get("id") or Path(zarr_path.rstrip("/")).name.removesuffix(".zarr")
    return Item.from_dict(
        {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": item_id,
            "geometry": stac_discovery.get("geometry"),
            "bbox": stac_discovery.get("bbox"),
            "properties": properties,
            "links": [],
            "assets": {"product": {"href": zarr_path.rstrip("/"), "type": ZARR_MEDIA_TYPE, "roles": ["data"]}},
        },
    )


class InputItemsCatalog:  # pylint: disable=too-few-public-methods
    """
    Replaces the catalog client to resolve the input products STAC items.
    The items are indexed by their zarr path, which is used as the item ID of the input products.
    """

    def __init__(self, items: dict[str, Item]):
        self.items = items

    def get_item(
        self,
        collection_id: str,  # pylint: disable=unused-argument
        item_id: str,
        owner_id: str | None = None,  # pylint: disable=unused-argument
    ) -> Item | None:
        """Return the input item built for the given zarr path."""
        return self.items.get(item_id)


def pipeline_input_names(task_table: dict[str, Any], unit_list: list[dict[str, Any]]) -> list[str]:
    """Return the names of the input products coming from outside the pipeline/unit."""
    names: list[str] = []
    for unit in unit_list:
        for product in unit.get("input_products", []):
            if product.get("origin") == "pipeline_input" and product["name"] not in names:
                names.append(product["name"])
    # Unknown names are kept so the error messages can list them
    return [name for name in names if search_by_name(task_table["io"], name) is not None] or names


def guess_input_name(zarr_path: str, candidates: list[str], task_table: dict[str, Any]) -> str:
    """
    Find the task table input product matching a zarr path, using the 'store_params.regex' of
    the task table 'io' section, or the single candidate if there is only one.
    """
    if len(candidates) == 1:
        return candidates[0]
    basename = Path(zarr_path.rstrip("/")).name
    matching = []
    for name in candidates:
        regex = (search_by_name(task_table["io"], name) or {}).get("store_params", {}).get("regex")
        if regex and (re.fullmatch(regex, zarr_path) or re.fullmatch(regex, basename)):
            matching.append(name)
    if len(matching) != 1:
        raise ValueError(
            f"Unable to determine the task table input product for '{zarr_path}' "
            f"(candidates: {candidates}, matching regex: {matching}). Use the NAME=PATH syntax.",
        )
    return matching[0]


def derive_external_variables(items: list[Item]) -> dict[str, Any]:
    """Derive default values of the task table external variables from the input products properties."""
    starts = [d for item in items if (d := item.common_metadata.start_datetime or item.datetime)]
    ends = [d for item in items if (d := item.common_metadata.end_datetime or item.datetime)]
    platforms = {item.properties.get("platform") for item in items} - {None}
    modes = {item.properties.get("sar:instrument_mode") for item in items} - {None}
    return {
        "start_datetime": min(starts) if starts else None,
        "end_datetime": max(ends) if ends else None,
        "satellite": platforms.pop() if len(platforms) == 1 else None,
        "instrument_mode": modes.pop() if len(modes) == 1 else None,
    }


################
# ADFS staging #
################


def call_with_retries(func: Callable, retries: int, retry_delay: int, description: str):
    """Call a function, retrying on exception (same behaviour as the Prefect task retries)."""
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if attempt >= retries:
                raise
            logger.warning(f"{description} failed ({exc}), retry {attempt + 1}/{retries} in {retry_delay}s")
            time.sleep(retry_delay)
    return None  # unreachable


class AdfsStager:
    """Search and stage the ADFS of the task table, like the 'process_input_adfs' task of the flow."""

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        rs_client: RsClient,
        owner_id: str,
        dpr_input: DprProcessIn,
        task_table: dict[str, Any],
        staging_retries: int = 3,
        staging_retry_delay: int = 60,
        dry_run: bool = False,
    ):
        self.env = SimpleNamespace(rs_client=rs_client, owner_id=owner_id)
        self.dpr_input = dpr_input
        self.task_table = task_table
        self.staging_retries = staging_retries
        self.staging_retry_delay = staging_retry_delay
        self.dry_run = dry_run

    def search(self, cql2: dict, source: AuxiliarySource) -> ItemCollection | None:
        """Search auxiliary files on the given STAC source (see rs_workflows.utils.stac.search)."""
        stac_client, search_kwargs = select_search_client_and_kwargs(self.env, source)  # type: ignore[arg-type]
        found = stac_client.search(
            method="POST",
            stac_filter=cql2.get("filter"),
            max_items=cql2.get("limit"),
            collections=cql2.get("collections"),
            sortby=cql2.get("sortby"),
            timestamp=cql2.get("timestamp"),
            **search_kwargs,
        )
        logger.info(f"STAC search on {source.value} found {len(found) if found else 0} result(s)")
        return found

    def stage(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        cql2: dict,
        collection: str,
        source: AuxiliarySource,
        selected_assets: list[str] | None,
    ) -> tuple[bool, ItemCollection | None]:
        """Search and stage auxiliary files (see rs_workflows.aux_flow.aux_staging)."""
        aux_items = self.search(cql2, source)
        if not aux_items:
            logger.info("Nothing to stage: AUX search with given filter returned empty result.")
            return True, None
        if source == AuxiliarySource.CATALOG:
            logger.info("AUX items found in catalog; skipping staging.")
            return True, aux_items
        if self.dry_run:
            logger.info(f"Dry run: skip staging of {[item.id for item in aux_items]} into '{collection}'")
            return True, aux_items

        rs_client = self.env.rs_client
        staging_client = rs_client.get_staging_client()
        asset_names = selected_assets or ({"product"} if source == AuxiliarySource.CDSE else None)
        job_status = staging_client.run_staging(aux_items.to_dict(), collection, asset_names)  # type: ignore
        staging_results = staging_client.wait_for_jobs(job_status, logger)

        return_status = True
        for job_name, job_result in staging_results.items():
            if job_result.get("status") != "successful":
                logger.error(
                    f"❌ Staging job '{job_name}' with ID {job_result.get('jobID')} FAILED.\n"
                    f"Status: {job_result.get('status')} - Reason: {job_result.get('message')}",
                )
                return_status = False

        # Get staged items from catalog (to have the correct href)
        catalog_items = ItemCollection(
            rs_client.get_catalog_client().get_items(
                collection_id=collection,
                items_ids=[item.id for item in aux_items],
            ),
        )
        return return_status, catalog_items

    async def normalize_archived_items(self, item_collection: ItemCollection) -> ItemCollection:
        """Unzip/decompress the staged archives and update the catalog (see _normalize_archived_aux_items)."""
        archived_indexes = get_archived_item_indexes(item_collection)
        if not archived_indexes or self.dry_run:
            return item_collection
        catalog_client = self.env.rs_client.get_catalog_client()
        for idx in archived_indexes:
            logger.info(f"Normalizing archived ADFS item '{item_collection.items[idx].id}'")
            new_item = await asset_unzip_decompress.fn(item_collection.items[idx])
            item_collection.items[idx] = new_item
            catalog_client.update_item(new_item)
        return item_collection

    async def process_input_adfs(
        self,
        input_adfs: dict[str, Any],
        specific_input_product: tuple[str | None, Item | None] = (None, None),
    ) -> tuple[str, str, tuple[bool, ItemCollection]]:
        """Try the ADFS alternatives in order and return the first staged result."""
        for alternative in input_adfs.get("alternatives", []):
            aux_cql2, parameters = render_aux_cql2(alternative, self.task_table, specific_input_product)
            product_type = parameters.get("product_type", "*")
            collection, source, selected_assets = select_aux_collection_and_source(self.dpr_input, product_type)
            logger.info(
                f"🚧 AUX request for ADFS '{input_adfs['name']}' using source '{source.value}' "
                f"and collection '{collection}':\n{json.dumps(aux_cql2, indent=2)}",
            )
            if alternative.get("timeout_seconds"):
                logger.debug("The alternative 'timeout_seconds' is not applied in standalone mode")

            # Special case for Copernicus DEM available at Earthdatahub
            if input_adfs["name"] == "DEM":
                aux_status = True
                aux_items = self.search(aux_cql2, AuxiliarySource.EARTHDATAHUB)
            else:
                aux_status, aux_items = call_with_retries(
                    lambda: self.stage(aux_cql2, collection, source, selected_assets),  # pylint: disable=W0640
                    self.staging_retries,
                    self.staging_retry_delay,
                    f"Staging of ADFS '{input_adfs['name']}'",
                )
            if not aux_items:
                continue
            for aux_item in aux_items:
                logger.info(f"Staged ADFS '{input_adfs['name']}': {aux_item.id}")
            item_collection = await self.normalize_archived_items(aux_items)
            return input_adfs["name"], input_adfs["type"], (aux_status, item_collection)

        raise RuntimeError(f"Searching for adfs input {input_adfs['name']} did not return any result")

    async def process_units(
        self,
        unit_list: list[dict[str, Any]],
        input_rs_client: Any,
    ) -> list[tuple[str, str, str]]:
        """Stage the ADFS of all the units and return the (name, type, href) tuples for the payload."""
        adfs: list[tuple[str, str, str]] = []
        for unit in unit_list:
            for input_adfs in unit["input_adfs"]:
                specific_input_name, product_stac_items = resolve_specific_input_product_stac_items(
                    input_adfs,
                    self.task_table,
                    unit,
                    self.dpr_input.input_products,
                    input_rs_client,
                )
                for stac_item in product_stac_items:
                    name, adf_type, (status, item_collection) = await self.process_input_adfs(
                        input_adfs,
                        (specific_input_name, stac_item),
                    )
                    for item in item_collection.items:
                        href = next(iter(item.assets.values())).href
                        if not status:
                            raise ValueError(f"The adf input files {href} was not correctly staged")
                        if (name, adf_type, href) not in adfs:
                            logger.info(f"ADFS '{name}' of type '{adf_type}': {href}")
                            adfs.append((name, adf_type, href))
        return adfs


###########
# Payload #
###########


def load_bucket_configuration(output_bucket: str | None) -> list[list[str]]:
    """Return the output bucket configuration, from the given bucket or the rs-osam endpoint."""
    if output_bucket:
        return [["*", "*", "*", "*", output_bucket]]
    if osam_host := os.getenv("RSPY_HOST_OSAM"):
        return fetch_csv_from_endpoint(osam_host + "/internal/configuration")
    logger.warning("⚠️ Neither --output-bucket nor RSPY_HOST_OSAM is set, the obs outputs cannot be resolved")
    return []


async def write_payload(payload: PayloadSchema, output: str | None, reveal_secrets: bool):
    """Write the payload as YAML to the given local or S3 path, or to stdout."""
    yaml_str = yaml.dump(payload.dump(reveal_secrets=reveal_secrets), default_flow_style=False, sort_keys=False)
    if not output:
        sys.stdout.write(yaml_str)
    elif output.startswith("s3://"):
        await prefect_utils.s3_upload_bytes(yaml_str.encode("utf-8"), output)
    else:
        Path(output).write_text(yaml_str, encoding="utf-8")
    if output:
        logger.info(f"✅ Payload written to {output}")


def parse_key_value(value: str, separator: str = "=") -> tuple[str | None, str]:
    """Parse an optional 'KEY=VALUE' argument. Values such as s3:// URLs are not split on '='."""
    key, sep, rest = value.partition(separator)
    if sep and key and "/" not in key:
        return key, rest
    return None, value


def parse_aux_mapping(value: str) -> AuxiliaryProductMapping:
    """Parse 'PRODUCT_TYPE=COLLECTION[:SOURCE]'."""
    product_type, rest = parse_key_value(value)
    if not product_type:
        raise argparse.ArgumentTypeError(f"Invalid AUX mapping {value!r}, expected PRODUCT_TYPE=COLLECTION[:SOURCE]")
    collection, _, source = rest.partition(":")
    return AuxiliaryProductMapping(
        product_type=product_type,
        collection_name=collection,
        source=AuxiliarySource(source) if source else AuxiliarySource.AUXIP,
    )


def parse_output_mapping(value: str) -> FlowGeneratedProduct:
    """Parse 'NAME=PRODUCT_TYPE[:COLLECTION]'."""
    name, rest = parse_key_value(value)
    if not name:
        raise argparse.ArgumentTypeError(f"Invalid output mapping {value!r}, expected NAME=PRODUCT_TYPE[:COLLECTION]")
    product_type, _, collection = rest.partition(":")
    return FlowGeneratedProduct(name=name, product_type=product_type, collection_name=collection or None)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Stage the ADFS and generate the DPR payload for a task table and a list of input zarr products, "
            "without Prefect nor the rs-dpr-service."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-t", "--tasktable", required=True, help="Task table JSON file")
    parser.add_argument(
        "-i",
        "--input",
        dest="inputs",
        action="append",
        required=True,
        metavar="[NAME=]PATH",
        help="Input zarr product path (S3 or local). NAME is the task table input product name, "
        "guessed from the task table if omitted. Can be repeated.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-p", "--pipeline", help="Task table pipeline name")
    mode.add_argument("-u", "--unit", help="Task table unit name")
    parser.add_argument(
        "--params",
        help="Optional JSON/YAML file with the same fields as the 'dpr_input' parameter of the generic "
        "processing flow (auxiliary_product_to_collection_identifier, generated_product_to_collection_identifier, "
        "satellite, ...). Command line arguments take precedence.",
    )
    parser.add_argument("-o", "--output", help="Output payload file (local or s3:// path). Default: stdout")
    parser.add_argument("--processor-name", help="Processor name. Default: deduced from the task table name")
    parser.add_argument("--processing-mode", action="append", help="Processing mode (nrt, ntc, ...). Can be repeated")
    parser.add_argument("--start-datetime", help="Default: earliest input product start datetime")
    parser.add_argument("--end-datetime", help="Default: latest input product end datetime")
    parser.add_argument("--satellite", help="Default: input products 'platform' property")
    parser.add_argument("--instrument-mode", help="Default: input products 'sar:instrument_mode' property")
    parser.add_argument("--reference-date", help="Reference date (YYYY-MM-DD)")
    parser.add_argument(
        "--aux-mapping",
        action="append",
        type=parse_aux_mapping,
        metavar="PRODUCT_TYPE=COLLECTION[:SOURCE]",
        help="Catalog collection (and search source: auxip, catalog, prip, cadip, cdse, earthdatahub) "
        "of the auxiliary product types. Use '*' for all the other types. Can be repeated.",
    )
    parser.add_argument(
        "--output-mapping",
        action="append",
        type=parse_output_mapping,
        metavar="NAME=PRODUCT_TYPE[:COLLECTION]",
        help="Product type and collection of the generated products. "
        "Default: the task table output name is used as product type. Can be repeated.",
    )
    parser.add_argument("--owner-id", default=os.getenv("RSPY_HOST_USER"), help="Owner ID")
    parser.add_argument(
        "--storage-config",
        default=str(CONFIG_DIR / "storage_configuration.json"),
        help="Storage configuration JSON file (content of the 'processing-storage-configuration' prefect variable). "
        "The ${VAR} secrets are read from the environment variables.",
    )
    parser.add_argument("--output-bucket", help="Output bucket. Default: read from rs-osam (RSPY_HOST_OSAM)")
    parser.add_argument("--temporary-folder", help="Processor temporary folder")
    parser.add_argument("--temporary-shared", action="store_true", help="Temporary folder reachable from the workers")
    parser.add_argument("--dask-task-timeout", type=int, help="Default timeout on a submitted dask task")
    parser.add_argument("--edh-api-key", default=os.getenv("EDH_API_KEY"), help="EarthDataHub API key (DEM)")
    parser.add_argument("--staging-retries", type=int, default=3)
    parser.add_argument("--staging-retry-delay", type=int, default=60)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only search the ADFS: no staging, no catalog update. The payload references the searched hrefs.",
    )
    parser.add_argument("--reveal-secrets", action="store_true", help="Write the secrets values in the payload")
    parser.add_argument("--log-level", default="INFO", choices=[level.value for level in LoggingLevel])
    return parser.parse_args(argv)


def load_params(params_file: str | None) -> dict[str, Any]:
    """Load the optional flow parameters file."""
    if not params_file:
        return {}
    params: dict[str, Any] = yaml.safe_load(Path(params_file).read_text(encoding="utf-8")) or {}
    # Accept the full flow parameters, i.e. {"dpr_input": {...}}
    return params.get("dpr_input") or params


def build_dpr_input(  # pylint: disable=too-many-arguments
    args: argparse.Namespace,
    params: dict[str, Any],
    task_table: dict[str, Any],
    input_products: list[FlowInputProduct],
    unit_list: list[dict[str, Any]],
    external_variables: dict[str, Any],
) -> DprProcessIn:
    """Build the flow input model, from the parameters file and the command line arguments."""
    values: dict[str, Any] = {key: value for key, value in params.items() if key not in ("env", "input_products")}

    # Command line arguments override the parameter file
    overrides = {
        "processor_name": args.processor_name,
        "processing_mode": args.processing_mode,
        "reference_date": args.reference_date,
        "auxiliary_product_to_collection_identifier": args.aux_mapping,
        "temporary_folder": args.temporary_folder,
        "temporary_shared": args.temporary_shared or None,
        "dask_task_timeout": args.dask_task_timeout,
        "edh_api_key": args.edh_api_key,
        **external_variables,
    }
    values.update({key: value for key, value in overrides.items() if value is not None})
    values["pipeline"], values["unit"] = args.pipeline, args.unit

    if not values.get("processor_name"):
        values["processor_name"] = task_table.get("tasktable", {}).get("name", "").replace("-", "_")
    if not values.get("auxiliary_product_to_collection_identifier"):
        raise ValueError("At least one --aux-mapping (or 'auxiliary_product_to_collection_identifier') is required")

    # Generated products: command line mappings, then the parameters file, then default values
    output_mappings = {p.name: p for p in args.output_mapping or []}
    for mapping in values.get("generated_product_to_collection_identifier", []):
        mapping = FlowGeneratedProduct.model_validate(mapping)
        output_mappings.setdefault(mapping.name, mapping)
    for unit in unit_list:
        for output_product in unit.get("output_products", []):
            if output_product["name"] not in output_mappings:
                logger.warning(f"⚠️ No output mapping for '{output_product['name']}', its name is used as product type")
                output_mappings[output_product["name"]] = FlowGeneratedProduct(
                    name=output_product["name"],
                    product_type=output_product["name"],
                )
    values["generated_product_to_collection_identifier"] = list(output_mappings.values())

    values.setdefault("processor_version", "")
    values.setdefault("dask_cluster_label", "standalone")
    values["s3_payload_file"] = args.output or ""
    values["input_products"] = input_products
    values["env"] = FlowEnvArgs(owner_id=args.owner_id, logging_level=LoggingLevel(args.log_level))
    return DprProcessIn.model_validate(values)


async def generate(args: argparse.Namespace) -> PayloadSchema:  # pylint: disable=too-many-locals
    """Stage the ADFS and generate the payload."""
    if not args.owner_id:
        raise ValueError("The owner ID is required (--owner-id or RSPY_HOST_USER)")
    task_table = json.loads(Path(args.tasktable).read_text(encoding="utf-8"))
    params = load_params(args.params)

    # Read the input products metadata
    parsed_inputs = [parse_key_value(value) for value in args.inputs]
    input_items: dict[str, Item] = {}
    for _, path in parsed_inputs:
        input_items[path] = build_input_item(path, await read_zarr_attributes(path))
        logger.info(f"Input product '{path}': {input_items[path].id}")

    # External variables: command line, then parameters file, then input products properties
    external_variables = derive_external_variables(list(input_items.values()))
    for key in external_variables:
        value = getattr(args, key) or params.get(key)
        if value and key.endswith("datetime") and isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if value:
            external_variables[key] = value
    logger.info(f"External variables: {external_variables}")

    dpr_variables = {"reference_date": args.reference_date or params.get("reference_date"), **external_variables}
    unit_list = build_unit_list(
        tasktable=task_table,
        pipeline=args.pipeline,
        unit=args.unit,
        processing_mode=args.processing_mode or params.get("processing_mode"),
        external_variables=dpr_variables,
    )
    logger.info(f"Processing units: {[unit['name'] for unit in unit_list]}")

    # Associate each input zarr to a task table input product
    candidates = pipeline_input_names(task_table, unit_list)
    input_products = [
        FlowInputProduct(
            name=name or guess_input_name(path, candidates, task_table),
            item_id=path,
            collection_name=INPUT_COLLECTION,
        )
        for name, path in parsed_inputs
    ]
    for input_product in input_products:
        logger.info(f"Input '{input_product.name}': {input_product.item_id}")

    dpr_input = build_dpr_input(args, params, task_table, input_products, unit_list, external_variables)

    # Stage the ADFS. The input products are resolved from their zarr metadata, not from the catalog.
    input_rs_client = SimpleNamespace(get_catalog_client=lambda: InputItemsCatalog(input_items))
    rs_client = RsClient(
        rs_server_href=os.getenv("RSPY_WEBSITE"),
        rs_server_api_key=os.getenv("RSPY_APIKEY"),
        owner_id=args.owner_id,
        logger=logger,
    )
    stager = AdfsStager(
        rs_client,
        args.owner_id,
        dpr_input,
        task_table,
        args.staging_retries,
        args.staging_retry_delay,
        args.dry_run,
    )
    adfs = await stager.process_units(unit_list, input_rs_client)

    # Generate the payload
    storage_data = json.loads(Path(args.storage_config).read_text(encoding="utf-8"))
    storage_configuration = StorageConfig(dict(os.environ), logger, data=storage_data)
    return build_payload(
        SimpleNamespace(owner_id=args.owner_id, rs_client=input_rs_client),  # type: ignore[arg-type]
        unit_list,
        adfs,
        dpr_input,
        storage_configuration,
        load_bucket_configuration(args.output_bucket),
        external_modules=extract_external_modules(task_table),
    )


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
        stream=sys.stderr,
        force=True,  # replace the handlers configured by Prefect at import time
    )
    try:

        async def run():
            payload = await generate(args)
            await write_payload(payload, args.output, args.reveal_secrets)

        asyncio.run(run())
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception(f"❌ Payload generation failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
