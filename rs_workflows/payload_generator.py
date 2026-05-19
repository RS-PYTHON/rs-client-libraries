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

"""This module contains functions to generate DPR payloads for RS-Server."""

import fnmatch
import os
from collections import defaultdict
from copy import deepcopy
from os.path import commonprefix
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

import requests
from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from pydantic import SecretStr
from pystac import Item

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_client.stac.catalog_client import CatalogClient
from rs_common import prefect_utils
from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnv,
)
from rs_workflows.payload_template import (
    AdfConfig,
    GeneralConfiguration,
    InputProduct,
    IOConfig,
    LoggingConfig,
    OutputProduct,
    PayloadSchema,
    StoreParams,
    WorkflowStep,
)
from rs_workflows.storage_configuration import StorageConfig
from rs_workflows.utils.utils import get_common_and_relative_paths, search_by_name

FILEPATH_ENV_VAR = "BUCKET_CONFIG_FILE_PATH"
DEFAULT_FILEPATH = "/app/conf/expiration_bucket.csv"
RSPY_CATALOG_BUCKET = "rs-dev-cluster-catalog"
DATA_EDH_DOMAIN = "data.earthdatahub.destine.eu"

CONFIG_DIR = Path(__file__).parent.parent / "config"


def build_workflow_step(unit):
    """
    Constructs a WorkflowStep instance from a unit configuration dictionary.

    This function parses the given processing unit definition, extracting input products,
    auxiliary data files (ADFs), and output products. It then returns a WorkflowStep
    object ready to be integrated into a full processing payload schema.

    Args:
        unit (dict): A dictionary defining a single workflow unit.
            Expected keys include:
                - "name" (str): The unit name.
                - "module" (str): The module path or identifier.
                - "input_products" (list[dict], optional): List of input product mappings.
                - "input_adfs" (list[dict], optional): List of auxiliary data files.
                - "output_products" (list[dict], optional): List of output product mappings.

    Returns:
        WorkflowStep: A fully initialized workflow step object.

    Raises:
        ValueError: If a required key is missing from the provided unit dictionary.
    """
    # get inputs
    inputs: dict[str, str] = {}
    for input_product in unit.get("input_products", []):
        if isinstance(input_product, dict) and "origin" in input_product and "name" in input_product:
            if "pipeline_input" in input_product["origin"]:
                inputs[input_product["name"]] = input_product["name"]
            else:
                inputs[input_product["name"]] = input_product["origin"]
    # get adfs
    adfs: dict[str, str] = {}
    for input_adf in unit.get("input_adfs", []):
        if isinstance(input_adf, dict) and "name" in input_adf:
            adfs[input_adf["name"]] = input_adf["name"]
    # get outputs
    outputs: dict[str, str] = {}
    for output_product in unit.get("output_products", []):
        if isinstance(output_product, dict) and "name" in output_product:
            left_part = output_product["regex"] if "regex" in output_product else output_product["name"]
            right_part = output_product["name"]  # ==> "*pdf" : "name"
            # if "origin" in output_product and "pipeline_output" not in output_product["origin"]:
            #     right_part = output_product["origin"]
            outputs[left_part] = right_part
    try:
        return WorkflowStep(  # type: ignore
            name=unit["name"],
            active=True,
            validate_output=False,
            module=unit["module"],
            processing_unit=unit["name"].split(".")[0] if "." in unit["name"] else unit["name"],
            inputs=inputs or None,
            adfs=adfs or None,
            outputs=outputs or None,
            parameters=unit.get("parameters", None),
        )
    except KeyError as ke:
        raise ValueError(f"Key {ke} not found in unit list") from ke


def get_first_asset_dir(item: Item) -> str | None:
    """
    Returns the directory path (local or remote) derived from the href of the first asset in a pystac Item.

    Special case:
        If the first asset is a Zarr store (media_type "application/vnd+zarr"),
        the full href is returned unchanged, since it already represents a directory-like dataset.

    Args:
        item (pystac.Item): The STAC item containing assets.

    Returns:
        str | None: The resolved directory path or URL of the first asset, or None if no assets exist.
        Examples:
            s3://dev-bucket/path/to/folder.zarr
              ->  s3://dev-bucket/path/to/folder.zarr (Zarr store, returned as-is)
            s3://dev-bucket/path/to/cadu.raw  ->  s3://dev-bucket/path/to
            /local/path/to/file.raw       ->  /local/path/to
            https://example.com/data/file.tif -> https://example.com/data
    """
    if not item.assets:
        return None

    first_asset = next(iter(item.assets.values()))
    href = first_asset.href

    if first_asset.media_type == "application/vnd+zarr":
        return href

    parsed = urlparse(href)

    # get directory part of the path
    dir_path = os.path.dirname(parsed.path)

    # rebuild full URL (keeping scheme and netloc)
    if parsed.scheme:
        return urlunparse((parsed.scheme, parsed.netloc, dir_path, "", "", ""))

    # local file
    return os.path.abspath(dir_path)


def wildcard_match(string, pattern):
    """
    Checks whether a given string matches a simple wildcard pattern.

    The wildcard character '*' is treated as a placeholder for any substring.
    For example:
    - 'abc*def' matches 'abcdef' and 'abcXYZdef'.
    - '*xyz' matches 'endxyz'.
    - '*' matches any string.

    Args:
        string (str): The string to check against the pattern.
        pattern (str): The wildcard pattern, which may include '*'.

    Returns:
        bool: True if the string matches the pattern, False otherwise.
    """
    return fnmatch.fnmatch(string, pattern or "*")


def fetch_csv_from_endpoint(endpoint: str) -> list[list[str]]:
    """
    Fetches a CSV file from rs-osam endpoint and returns it
    as a list of rows (each row is a list of strings).

    Raises:
        RuntimeError: If the endpoint cannot be reached
        or response cannot be parsed as CSV.
    """
    try:
        response = requests.get(endpoint, timeout=10)
        response.raise_for_status()
        data = response.json()  # already list[list[str]]
    except Exception as exc:
        raise RuntimeError(
            f"Failed to fetch storage configuration from rs-osam endpoint '{endpoint}': {exc}",
        ) from exc

    if not isinstance(data, list):
        raise RuntimeError(
            f"Invalid configuration format returned by rs-osam endpoint: expected list[list[str]], got {type(data)}",
        )

    for row in data:
        if not isinstance(row, list) or not all(isinstance(x, str) for x in row) or len(row) != 5:
            raise RuntimeError(
                "Invalid configuration format: expected list[list[str]] containing only strings",
            )

    return data


def find_s3_output_bucket(
    config_rows: list[list[str]],
    owner_id: str,
    output_collection: str,
    product_type: str,
) -> str:
    """
    Determines the appropriate S3 output bucket based on owner, collection, and product type.
    It is based on story 854

    The matching logic prioritizes:
        1. Exact owner and collection match.
        2. Fallback match for owner only.
        3. Global fallback bucket (all first 3 columns are '*').

    Args:
        config_rows (list[list[str]]): Parsed configuration rows from the configmap file.
        owner_id (str): Owner identifier of the processing job.
        output_collection (str): Collection name associated with the output.
        product_type (str): Product type identifier (e.g., 'S3OLC', 'S3MWR').

    Returns:
        str: The resolved S3 bucket name (from the fifth column of the configmap).

    Raises:
        RuntimeError: If no matching bucket is found in the configuration.
    """
    fallback_bucket = None
    fallback_bucket_owner_only = None
    logger = get_run_logger()

    for row in config_rows:
        # the expiration_delay (the fourth field) is not used
        logger.debug(f"Configuration bucket: Checking row {row}")
        owner_pat, coll_pat, prod_type_pat, _, bucket = row

        # Basic compatibility check using wildcard_match
        if (
            wildcard_match(owner_id, owner_pat)
            and wildcard_match(output_collection, coll_pat)
            and wildcard_match(product_type, prod_type_pat)
        ):
            # Rank 1: Exact owner and collection
            if owner_pat == owner_id and coll_pat == output_collection:
                logger.info(f"Configuration bucket: Return bucket (exact match): {bucket}")
                return bucket

            # Rank 2: owner_id match (coll_pat must be '*')
            if owner_pat == owner_id and coll_pat == "*":
                if fallback_bucket_owner_only is None:
                    fallback_bucket_owner_only = bucket
                    logger.info(f"Configuration bucket: owner only fallback bucket: {bucket}")

            # Rank 3: Global fallback (*, *, *)
            if owner_pat == "*" and coll_pat == "*" and prod_type_pat == "*":
                if fallback_bucket is None:
                    fallback_bucket = bucket
                    logger.info(
                        "Configuration bucket: global fallback bucket (all first 3 columns are '*'): "
                        f"{fallback_bucket}",
                    )
                else:
                    logger.warning(
                        "Multiple default configurations were found in the configuration map "
                        "(rs-catalog-staging-configmap), while only one is expected. The first "
                        f"one found ({fallback_bucket}) will be used, but please review your configmap to prevent "
                        "unexpected behaviors. Only a single entry should have the first three columns set to '*'",
                    )
    if fallback_bucket_owner_only:
        logger.info(f"Configuration bucket: Returning owner only fallback: {fallback_bucket_owner_only}")
        return fallback_bucket_owner_only

    if fallback_bucket:
        logger.info(f"Configuration bucket: Returning global fallback: {fallback_bucket}")
        return fallback_bucket

    raise RuntimeError(
        f"Unable to determine the output bucket for owner = '{owner_id}', "
        f"collection = '{output_collection}', type = '{product_type}'",
    )


def resolve_stac_input_path(catalog_client: CatalogClient, collection: str, stac_item_id: str) -> tuple[Item, str]:
    """
    Retrieves the S3 path of the first asset from a STAC item within a collection.

    Args:
        catalog_client (CatalogClient): Client instance used to query the STAC catalog.
        collection (str): The collection identifier in the catalog.
        stac_item_id (str): The STAC item identifier to resolve.

    Returns:
        tuple[Item, str]: The specified STAC item and the path to its first asset.

    Raises:
        RuntimeError: If the STAC item is missing or contains no assets.
    """
    stac_item = catalog_client.get_item(collection, stac_item_id)
    if stac_item is None:
        raise RuntimeError(f"STAC item '{stac_item_id}' not found in collection '{collection}'.")

    stac_item_path = get_first_asset_dir(stac_item)
    if not stac_item_path:
        raise RuntimeError(f"STAC item '{stac_item_id}' in collection '{collection}' has no assets.")

    return stac_item, stac_item_path


def build_input_products(
    unit,
    dpr_process_in: DprProcessIn,
    storage_configuration: StorageConfig,
    catalog_client: CatalogClient,
) -> list[InputProduct]:
    """
    Builds the list of input product configurations for a workflow step.

    Each input product is resolved by matching the dpr process definition
    against the unit configuration and querying the STAC catalog for its asset path.

    Args:
        unit (dict): Workflow unit definition containing input product metadata.
        dpr_process_in (DprProcessIn): Input configuration for the dpr processing prefect flow.
        storage_configuration (StorageConfig): Storage configuration parameters (S3 credentials, etc.). TODO ! as
        written in the comment from story 800, point 3: About the storage_configuration.json : for the time being,
        just consider s3 configuration. No credential should be revealed. It is up to CPM to resolve secret.
        catalog_client (CatalogClient): Client for querying STAC collections and items.

    Returns:
        list[InputProduct]: A list of input product configuration objects.

    Raises:
        RuntimeError: If an expected input product or STAC item cannot be found.
    """
    inputs = []

    # Group input products by name (needed for inputs of type regex)
    grouped_products = defaultdict(list)
    for input_product in dpr_process_in.input_products:
        grouped_products[input_product.name].append(input_product)

    # Iterate over each group of products
    for product_name, products in grouped_products.items():

        mapping = search_by_name(unit.get("input_products", []), product_name)
        if not mapping:
            raise RuntimeError(f"Couldn't find any input for task table entry '{product_name}'")

        # cf story 871/Set S3 configuration in payload.yaml
        store_name = storage_configuration.get_storage_for_specific_product(product_name)
        if not store_name:
            if dpr_process_in.unit:
                store_name = storage_configuration.get_storage_for_unit_section("input_products")
            elif dpr_process_in.pipeline:
                store_name = storage_configuration.get_storage_for_pipeline_section(
                    mapping.get("origin", ""),
                ) or storage_configuration.get_storage_for_pipeline_section("other")
                # TODO: the following line is temporary and for tests only ! Force eveything to s3 !
                # Delete the following line once the things will be clarified with other store_names
                # This is due to the internal discussions, disregard any other store_name but s3
                store_name = "s3"
        if not store_name:
            raise RuntimeError(f"Couldn't find any storage configuration for input product '{product_name}'")

        kind = storage_configuration.get_storage_kind(store_name)
        store_params = deepcopy(storage_configuration.get_store_params(store_name))

        if len(products) == 1:
            # Standard case where each input product has a different name (i.e. single STAC item)
            input_product = products[0]
            _, stac_item_path = resolve_stac_input_path(
                catalog_client,
                input_product.collection_name,
                input_product.item_id,
            )

            opening_mode = None
            if kind in ("shared_disk", "local_disk"):
                disk_config = storage_configuration.get_disk_storage(store_name)
                if disk_config:
                    opening_mode = disk_config.get("opening_mode")
                store_params = None

            inputs.append(
                InputProduct(
                    id=mapping["name"],
                    path=stac_item_path,
                    # TODO: The value for this field in the tasktable (from where the unit is built) should be
                    # set to 'filename' for the s1 l0 processor, otherwise the processor fails to start.
                    # Verify in the rs-dpr-service tasktable (config/TaskTable_S1_L0_generated_by_rs_python_v1.json)
                    # that in the io section, the type field for input_products (S1ACADUS) is set to 'filename'.
                    # To be fixed in future iterations !
                    # FIXME https://cpm.pages.eopf.copernicus.eu/eopf-cpm/v3.X.Y/v3/triggering_migration.html
                    type=mapping.get("type", "file"),  # FIXME quick and dirty CPM v3 test
                    engine=mapping["engine"],  # FIXME quick and dirty CPM v3 test
                    reader_params=store_params,  # FIXME quick and dirty CPM v3 test
                    opening_mode=opening_mode,
                ),
            )
        elif isinstance(store_params, StoreParams):
            # Advanced case where several input products share the same name (i.e. several STAC items)
            # FIXME it is store_params.multiplicity for CPMv2 but multiplicity in CPMv3
            # store_params.multiplicity = str(len(products))
            multiplicity = len(products)

            # Retrieve all paths
            paths = [
                resolve_stac_input_path(
                    catalog_client,
                    product.collection_name,
                    product.item_id,
                )[1]
                for product in products
            ]
            # Compute longest common prefix
            common_folder, relative_parts = get_common_and_relative_paths(paths)
            # FIXME it is store_params.regex for CPMv2 but regex in CPMv3
            # store_params.regex = rf"({'|'.join(relative_parts)})"
            # store_params.regex = f"{commonprefix(relative_parts)}*"  # FIXME to remove after CPM issue #1080 is solved
            regex = f"{commonprefix(relative_parts)}*"  # FIXME to remove after CPM issue #1080 is solved # FIXME CPMv3

            # Add a single InputProduct of type regex listing the provided inputs in the common folder
            inputs.append(
                InputProduct(
                    id=mapping["name"],
                    path=common_folder,
                    type=mapping.get("type", "regex"),
                    engine=mapping["engine"],  # FIXME quick and dirty CPM v3 test
                    reader_params=store_params,  # FIXME quick and dirty CPM v3 test
                    regex=regex,  # FIXME quick and dirty CPM v3 test
                    multiplicity=multiplicity,  # FIXME quick and dirty CPM v3 test
                ),
            )
        else:
            raise RuntimeError(f"Couldn't find any storage configuration for input product '{product_name}'")
    return inputs


def build_output_products(
    unit,
    dpr_process_in: DprProcessIn,
    storage_configuration: StorageConfig,
    owner_id: str,
    bucket_configuration: list[list[str]],
) -> list[OutputProduct]:
    """
    Builds the list of output product configurations for a workflow step.

    Each output product is mapped to an appropriate S3 bucket, determined by
    the owner ID, collection, and product type according to the configuration file.

    Args:
        unit (dict): Workflow unit definition containing output product metadata.
        dpr_process_in (DprProcessIn): Input configuration defining generated outputs.
        storage_configuration (StorageConfig): Storage configuration parameters (S3 credentials, etc.). TODO ! as
        written in the comment from story 800, point 3: About the storage_configuration.json : for the time being,
        just consider s3 configuration. No credential should be revealed. It is up to CPM to resolve secret.
        owner_id (str): The owner ID for the workflow.
        bucket_configuration (list[list[str]]): Parsed S3 bucket configuration entries.

    Returns:
        list[OutputProduct]: A list of output product configuration objects.

    Raises:
        RuntimeError: If an output mapping or configuration rule cannot be found.
    """

    outputs = []
    processed_products = set()
    logger = get_run_logger()

    mapping_lookup = {p.name: p for p in dpr_process_in.generated_product_to_collection_identifier}

    for mapping in unit.get("output_products", []):
        product_name = mapping["name"]

        logger.info(f"Building output section for name: {product_name}")
        output_product = mapping_lookup.get(product_name)

        # Fails ONLY if required mapping is missing
        if not output_product:
            raise RuntimeError(
                f"Missing mapping in generated_product_to_collection_identifier for task table entry '{product_name}'",
            )

        product_type = output_product.product_type
        processed_products.add(product_name)
        output_collection = (
            output_product.collection_name if output_product.collection_name is not None else product_type
        )
        # When using * for product_type, the collection name becomes mandatory.
        if output_collection == "*":
            raise RuntimeError(
                "The product type in generated_product_to_collection_identifier "
                f"cannot be '*' if the collection name is not specified for product '{product_name}'",
            )

        # cf story 871/Set S3 configuration in payload.yaml
        store_name = storage_configuration.get_storage_for_specific_product(product_name)
        if not store_name:
            if dpr_process_in.unit:
                store_name = storage_configuration.get_storage_for_unit_section("output_products")
            elif dpr_process_in.pipeline:
                store_name = storage_configuration.get_storage_for_pipeline_section(
                    mapping.get("origin", ""),
                ) or storage_configuration.get_storage_for_pipeline_section("other")
        if not store_name:
            raise RuntimeError(f"Couldn't find any storage configuration for output product '{product_name}'")

        # Determine the output path based on the storage kind
        kind = storage_configuration.get_storage_kind(store_name)
        store_params = deepcopy(storage_configuration.get_store_params(store_name))
        opening_mode = mapping.get("opening_mode", "CREATE")
        autoclean = None
        engine: str = mapping["engine"]

        if kind == "obs":
            bucket_name = find_s3_output_bucket(bucket_configuration, owner_id, output_collection, product_type)
            # CPM v3 zarr engines (zarr/cpm_zarr) for the output path to end by ".zarr"
            # Otherwise we face error "Engine zarr is not able to write ..."
            output_path = os.path.join(
                "s3://",
                bucket_name,
                owner_id,
                output_collection,
                str(uuid4()) + ".zarr" if engine.endswith("zarr") else "",
            )
        elif kind in ("shared_disk", "local_disk"):
            disk_config = storage_configuration.get_disk_storage(store_name)
            if disk_config and disk_config.get("path"):
                output_path = disk_config["path"]
                opening_mode = disk_config.get("opening_mode", opening_mode)
                autoclean = disk_config.get("autoclean", False)
            else:
                raise RuntimeError(
                    f"Storage '{store_name}' of kind '{kind}' has no storage path configured "
                    f"for output product '{product_name}'",
                )
            store_params = None
        else:
            raise RuntimeError(f"Unknown storage kind '{kind}' for output product '{product_name}'")

        outputs.append(
            OutputProduct(
                id=mapping["name"],
                path=output_path,
                engine=engine,  # FIXME quick and dirty CPM v3 test
                writer_params=store_params,  # FIXME quick and dirty CPM v3 test
                # FIXME https://cpm.pages.eopf.copernicus.eu/eopf-cpm/v3.X.Y/v3/triggering_migration.html
                type=mapping.get("type", "file"),  # FIXME quick and dirty CPM v3 test
                # opening_mode=opening_mode,  # FIXME quick and dirty CPM v3 test
                final_product=mapping.get("final_product", True),
                autoclean=autoclean,
            ),
        )

    # ensure that all keys from unit["output_products"] are present into
    # dpr_process_in.generated_product_to_collection_identifier
    for unit_output in unit.get("output_products", []):
        if unit_output["name"] not in processed_products:
            raise RuntimeError(f"Couldn't find any relation for output product '{unit_output['name']}'")

    return outputs


def get_io(
    unit,
    dpr_process_in: DprProcessIn,
    flow_env: FlowEnv,
    storage_configuration: StorageConfig,
    bucket_configuration: list[list[str]],
) -> tuple[list, list]:
    """
    Builds both input and output product configurations for a given workflow step.

    This function integrates configuration data from:
      - The workflow unit definition ('unit')
      - The DPR process input ('dpr_process_in')
      - The environment and S3 configuration (via 'flow_env' and configmap)

    Args:
        unit (dict): Workflow unit definition containing I/O product configurations.
        dpr_process_in (DprProcessIn): DPR input configuration containing product mappings.
        store_params (StoreParams): S3 storage configuration and credentials. TODO ! as
        written in the comment from story 800, point 3: About the storage_configuration.json : for the time being,
        just consider s3 configuration. No credential should be revealed. It is up to CPM to resolve secret.
        flow_env (FlowEnv): Environment context holding execution metadata.

    Returns:
        tuple[list[InputProduct], list[OutputProduct]]:
            A tuple containing lists of input and output product objects ready for inclusion in a payload schema.

    Raises:
        RuntimeError: If the configuration file cannot be read or an input/output product cannot be resolved.
    """
    catalog_client = flow_env.rs_client.get_catalog_client()
    owner_id = flow_env.owner_id
    inputs = build_input_products(unit, dpr_process_in, storage_configuration, catalog_client)
    outputs = build_output_products(unit, dpr_process_in, storage_configuration, owner_id, bucket_configuration)

    return inputs, outputs


def build_adfs(
    storage_configuration: StorageConfig,
    adfs: list[tuple[str, str, str]],
    dpr_process_in: DprProcessIn,
) -> list[AdfConfig]:
    """
    Build a list of AdfConfig objects from input ADF definitions.

    ADFs are grouped by their identifier. If an identifier is associated
    with a single path, a standard AdfConfig is created. If multiple paths
    share the same identifier, a single AdfConfig is created using a common
    folder and a regex pattern to match all corresponding files.

    Args:
        storage_configuration (StorageConfig): Configuration object used to
            retrieve default storage parameters.
        adfs (list[tuple[str, str, str]]): List of (adf_id, adfs_type, path) tuples.
        dpr_process_in (DprProcessIn): DPR input process definition

    Returns:
        list[AdfConfig]: List of constructed AdfConfig objects, one per group
        of ADFs.
    """
    result = []

    # Group adfs by name (needed for adfs of type regex)
    grouped_adfs = defaultdict(list)
    for adf_id, adfs_type, path in adfs:
        grouped_adfs[adf_id].append((path, adfs_type))

    # Iterate over each group of adfs
    for adfs_id, adfs_entries in grouped_adfs.items():
        adfs_name = storage_configuration.default_adfs_storage
        store_params = deepcopy(storage_configuration.get_store_params(adfs_name))
        if len(adfs_entries) == 1:
            # Standard case where each adfs has a different id (i.e. single file)
            path, adfs_type = adfs_entries[0]
            if adfs_type == "folder":
                path = os.path.dirname(path)
            # Inject EarthDataHub API Key in path, as per documentation at
            # https://earthdatahub.destine.eu/collections/copernicus-dem/datasets/GLO-30
            if dpr_process_in.edh_api_key and path.startswith(f"https://{DATA_EDH_DOMAIN}/"):
                path = SecretStr(
                    path.replace(DATA_EDH_DOMAIN, f"edh:{dpr_process_in.edh_api_key}@api.earthdatahub.destine.eu"),
                )
            result.append(
                AdfConfig(id=adfs_id, path=path, adf_params=store_params),
            )  # FIXME quick and dirty CPM v3 test
        elif isinstance(store_params, StoreParams):
            # Advanced case where several adfs share the same id (i.e. several files)
            adfs_paths = [p for p, _ in adfs_entries]
            store_params.multiplicity = str(len(adfs_paths))
            # Compute longest common prefix
            common_folder, relative_parts = get_common_and_relative_paths(adfs_paths)
            store_params.regex = rf"({'|'.join(relative_parts)})"
            store_params.regex = f"{commonprefix(relative_parts)}*"  # FIXME to remove after CPM issue #1080 is solved
            # Add a single AdfConfig of type regex listing the provided adfs in the common folder
            result.append(
                AdfConfig(
                    id=adfs_id,
                    path=common_folder,
                    # type="regex", # Unsupported by CPM but it feels needed here for S1ARD
                    adf_params=store_params,  # FIXME quick and dirty CPM v3 test
                ),
            )
        else:
            raise RuntimeError(f"Couldn't find any storage configuration for adfs '{adfs_name}'")

    return result


def load_storage_configuration(
    secrets: dict,
    config_path: str = str(CONFIG_DIR / "storage_configuration.json"),
    logger=None,
) -> StorageConfig:
    """
    Loads storage configuration from a JSON file and constructs a StorageConfig object.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Storage configuration file not found: {config_path}")

    return StorageConfig(secrets, config_path, logger)


@task(name="Generate payload file")
def generate_payload(  # pylint: disable=unused-argument
    flow_env: FlowEnv,
    unit_list: list[dict],
    adfs: list[tuple[str, str, str]],
    dpr_process_in: DprProcessIn,
) -> PayloadSchema:
    """
    Assembles and generates a payload schema for a DPR (Data Processing Request) job.

    This Prefect task builds the payload definition dynamically based on the provided
    workflow units, auxiliary data files, and input configuration. It produces a
    PayloadSchema object compatible with RS-Server DPR jobs.

    Args:
        env (FlowEnv): Environment configuration for the Prefect flow, including
            credentials, tracing, and runtime context.
        unit_list (list[dict]): List of workflow unit definitions containing I/O
            specifications and processing parameters.
        adfs (list[tuple[str, str, str]]): List of auxiliary item
            tuples, where each tuple includes the adfs name, the adfs type and the s3 storage path.
        dpr_process_in (DprProcessIn): DPR input process definition containing
            product paths and parameters.

    Returns:
        dict: A dictionary representation of the generated PayloadSchema.

    Raises:
        ValueError: If a required key is missing in one of the unit definitions.
        Exception: For any unexpected error during payload assembly.
    """

    # TODO: should be moved to dpr_client.py and it should call dpr_client.py::update_configuration
    logger = get_run_logger()
    # Init flow environment and opentelemetry span
    # flow_env = FlowEnv(dpr_process_in.env)
    # with flow_env.start_span(__name__, "generate-payload"):
    # the values should be name of the secrets, and not the values of these secrets.
    # it's up to the processor to retrieve the values at the running time
    # The storage_configuration.json file should be mounted in /etc/storage_configuration.json
    # in cluster mode, it should be mounted as volume from a predefined (?) configmap

    logger.info(f"🚧 Starting payload generation for DPR processor '{dpr_process_in.processor_name}'")
    logger.info("Loading storage configuration template from file")
    secrets = Secret.load(
        prefect_utils.format_env_user(prefect_utils.BLOCK_NAME_ENV_USER, flow_env.owner_id),
    ).get()  # type: ignore[union-attr]
    storage_configuration = load_storage_configuration(secrets, logger=logger)
    logger.info("Loading bucket configuration from rs-osam endpoint")
    bucket_configuration = fetch_csv_from_endpoint(os.environ["RSPY_HOST_OSAM"] + "/internal/configuration")

    logger.info("Building workflow and I/O sections")
    workflow_steps = []
    io_config = IOConfig()
    for unit in unit_list:
        try:
            workflow_steps.append(build_workflow_step(unit))
            input_products, output_products = get_io(
                unit,
                dpr_process_in,
                flow_env,
                storage_configuration,
                bucket_configuration,
            )
            seen_inputs = {p.id for p in io_config.input_products}
            io_config.input_products += [p for p in input_products if p.id not in seen_inputs]
            seen_outputs = {p.id for p in io_config.output_products}
            io_config.output_products += [p for p in output_products if p.id not in seen_outputs]
        except KeyError as ke:
            raise ValueError(f"Key {ke} not found in unit list") from ke

    logger.info("Building ADFs section")
    io_config.adfs = build_adfs(storage_configuration, adfs, dpr_process_in)

    # Add the logging config for l0 and s1 / s3 configurations. These configurations
    # are hardcoded in the l0 eopf dask worker image. The path where these files are stored is given
    # by the env var PAYLOAD_CONFIG_FILES
    logging = None
    config = None
    if dpr_process_in.processor_name in (DprProcessor.S1L0, DprProcessor.S3L0):
        logging = "/opt/dask-l0/logging_config.yaml"
        match dpr_process_in.processor_name:
            case DprProcessor.S1L0:
                config = ["/opt/dask-l0/s1_default_configuration.yaml", "/opt/dask-l0/cadu_configuration.yaml"]
                # TODO: this section is temporary, should be removed when the S1 L0 processor doesn't need it anymore
                # the processor doesn't start without these parameters
                for workflow_step in workflow_steps:
                    extra_parameters = {
                        "streaming_mode": False,
                        "temporary_path": "./output_params/tmp",
                        "acquisition_report_output_path": "./output_params/s1",
                    }
                    if workflow_step.parameters:
                        workflow_step.parameters.update(extra_parameters)
                    else:
                        workflow_step.parameters = extra_parameters
            case DprProcessor.S3L0:
                config = ["/opt/dask-l0/s3_default_configuration.yaml", "/opt/dask-l0/cadu_configuration.yaml"]
                # TODO: this section is temporary, should be removed when the S3 L0 processor doesn't need it anymore
                # the processor doesn't start without these parameters
                for workflow_step in workflow_steps:
                    extra_parameters = {
                        "temporary_path": "./S3_D_TDS_1/output/tmp",
                        "acquisition_report_output_path": "./S3_D_TDS_1/output/default/Report/",
                        "ignore_output": "S03CACHE",
                    }
                    if workflow_step.parameters:
                        workflow_step.parameters.update(extra_parameters)
                    else:
                        workflow_step.parameters = extra_parameters
    elif dpr_process_in.processor_name == DprProcessor.S1ARD:
        for workflow_step in workflow_steps:
            if workflow_step.processing_unit == "Calibration":
                workflow_step.parameters = {
                    "reference_date": str(dpr_process_in.reference_date),
                }

    # Build the full payload using the schema
    # NOTE: The dask context is not built here, it will be updated by the dpr_service
    logger.info("Building the payload")
    temp_folder_s3_secret = (
        "s3" if dpr_process_in.temporary_folder and dpr_process_in.temporary_folder.startswith("s3://") else None
    )
    payload = PayloadSchema(
        # add some default params, as stated in a comment from jira (stories 800/1050)
        general_configuration=GeneralConfiguration(
            logging=LoggingConfig(level=dpr_process_in.logging_level.name),
            triggering__temporary_shared=dpr_process_in.temporary_shared,
            dask_utils__timeout=dpr_process_in.dask_task_timeout,
            temporary__folder=dpr_process_in.temporary_folder,
            temporary__folder_s3_secret=temp_folder_s3_secret,
        ),
        workflow=workflow_steps,
        io=io_config,  # type: ignore
        # The dask_context section is built in the dpr_service
        # dask_context=dask_context,
        logging=logging,
        config=config,
        secret=["secrets.json"] if temp_folder_s3_secret else None,
    )
    logger.debug(f"Generated payload: \n {payload}")
    return payload
