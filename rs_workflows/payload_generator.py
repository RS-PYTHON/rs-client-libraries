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

"""This module contains functions to generate DPR payloads for RS-Server."""
import fnmatch
import json
import os
from urllib.parse import urlparse, urlunparse

import requests
from prefect import get_run_logger, task
from pystac import Item

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_workflows.flow_utils import (  # FlowEnvArgs,
    DprProcessIn,
    FlowEnv,
)
from rs_workflows.payload_template import (  # DaskContext,
    AdfConfig,
    GeneralConfiguration,
    InputProduct,
    IOConfig,
    OutputProduct,
    PayloadSchema,
    StorageOptions,
    StoreParams,
    WorkflowStep,
)

FILEPATH_ENV_VAR = "BUCKET_CONFIG_FILE_PATH"
DEFAULT_FILEPATH = "/app/conf/expiration_bucket.csv"


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
    input_products = []
    if "input_products" in unit:
        for input_product in unit["input_products"]:
            if isinstance(input_product, dict) and "origin" in input_product and "name" in input_product:
                if "pipeline_input" in input_product["origin"]:
                    input_products.append({input_product["name"]: input_product["name"]})
                else:
                    input_products.append({input_product["name"]: input_product["origin"]})
    # get adfs
    adfs = []
    if "input_adfs" in unit:
        for input_adf in unit["input_adfs"]:
            if isinstance(input_adf, dict) and "name" in input_adf:
                adfs.append({input_adf["name"].lower(): input_adf["name"]})
    # get outputs
    output_products = []
    if "output_products" in unit:
        for output_product in unit["output_products"]:
            if isinstance(output_product, dict) and "name" in output_product:
                left_part = output_product["regex"] if "regex" in output_product else output_product["name"]
                right_part = output_product["name"]  # ==> "*pdf" : "name"
                # if "origin" in output_product and "pipeline_output" not in output_product["origin"]:
                #     right_part = output_product["origin"]
                output_products.append({left_part: right_part})
    try:
        return WorkflowStep(  # type: ignore
            name=unit["name"],
            active=True,
            validate_output=False,
            module=unit["module"],
            processing_unit=unit["name"],
            inputs=input_products if input_products else None,
            adfs=adfs if adfs else None,
            outputs=output_products,
            parameters=None,
        )
    except KeyError as ke:
        raise ValueError(f"Key {ke} not found in unit list") from ke


def get_first_asset_dir(item: Item) -> str | None:
    """
    Returns the local or remote path component from the href of the first asset in a pystac Item.

    Args:
        item (pystac.Item): The STAC item containing assets.

    Returns:
        str | None: The path component of the first asset's href, or None if no assets exist.
        Examples:
            s3://dev-bucket/path/to/cadu.raw  ->  s3://dev-bucket/path/to
            /local/path/to/file.raw       ->  /local/path/to
            https://example.com/data/file.tif -> https://example.com/data
    """
    if not item.assets:
        return None

    first_asset = next(iter(item.assets.values()))
    href = first_asset.href

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
    """ "
    Determines the appropriate S3 output bucket based on owner, collection, and product type.
    It is based on story 854

    The matching logic prioritizes:
        1. Exact owner and collection match.
        2. Otherwise, the first row matching via wildcard pattern ('*').

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
    for row in config_rows:
        # the expiration_delay (the fourth field) is not used
        owner_pat, coll_pat, prod_type_pat, _, bucket = row

        if (
            wildcard_match(owner_id, owner_pat)
            and wildcard_match(output_collection, coll_pat)
            and wildcard_match(product_type, prod_type_pat)
        ):
            # highest priority: exact match on owner and collection
            if owner_pat == owner_id and coll_pat == output_collection:
                return bucket
            if fallback_bucket is None:
                fallback_bucket = bucket

    if fallback_bucket:
        return fallback_bucket

    raise RuntimeError(
        f"Unable to determine the output bucket for owner = '{owner_id}', "
        f"collection = '{output_collection}', type = '{product_type}'",
    )


def resolve_stac_input_path(catalog_client, collection: str, stac_item_id: str) -> str:
    """
    Retrieves the S3 path of the first asset from a STAC item within a collection.

    Args:
        catalog_client (CatalogClient): Client instance used to query the STAC catalog.
        collection (str): The collection identifier in the catalog.
        stac_item_id (str): The STAC item identifier to resolve.

    Returns:
        str: The path to the first asset of the specified STAC item.

    Raises:
        RuntimeError: If the STAC item is missing or contains no assets.
    """
    stac_item = catalog_client.get_item(collection, stac_item_id)
    if stac_item is None:
        raise RuntimeError(f"STAC item '{stac_item_id}' not found in collection '{collection}'.")

    stac_item_path = get_first_asset_dir(stac_item)
    if not stac_item_path:
        raise RuntimeError(f"STAC item '{stac_item_id}' in collection '{collection}' has no assets.")

    return stac_item_path


def build_input_products(unit, dpr_process_in, store_params, catalog_client) -> list[InputProduct]:
    """
    Builds the list of input product configurations for a workflow step.

    Each input product is resolved by matching the dpr process definition
    against the unit configuration and querying the STAC catalog for its asset path.

    Args:
        unit (dict): Workflow unit definition containing input product metadata.
        dpr_process_in (DprProcessIn): Input configuration for the dpr processing prefect flow.
        store_params (StoreParams): Storage configuration parameters (S3 credentials, etc.).TODO ! as
        written in the comment from story 800, point 3: About the storage_configuration.json : for the time being,
        just consider s3 configuration. No credential should be revealed. It is up to CPM to resolve secret.
        catalog_client (CatalogClient): Client for querying STAC collections and items.

    Returns:
        list[InputProduct]: A list of input product configuration objects.

    Raises:
        RuntimeError: If an expected input product or STAC item cannot be found.
    """
    inputs = []
    for input_product in dpr_process_in.input_products:
        product_name = next(iter(input_product))
        mapping = next((inp for inp in unit.get("input_products", []) if inp["name"] == product_name), None)

        if not mapping:
            raise RuntimeError(f"Couldn't find any input for task table entry '{product_name}'")

        stac_item_identifier, collection = input_product[product_name]
        stac_item_path = resolve_stac_input_path(catalog_client, collection, stac_item_identifier)

        inputs.append(
            InputProduct(
                id=mapping["name"],
                path=stac_item_path,
                type=mapping.get("type", "filename"),
                store_type=mapping["store_type"],
                store_params=store_params,
            ),
        )
    return inputs


def build_output_products(unit, dpr_process_in, store_params, flow_env, config_rows) -> list[OutputProduct]:
    """
    Builds the list of output product configurations for a workflow step.

    Each output product is mapped to an appropriate S3 bucket, determined by
    the owner ID, collection, and product type according to the configuration file.

    Args:
        unit (dict): Workflow unit definition containing output product metadata.
        dpr_process_in (DprProcessIn): Input configuration defining generated outputs.
        store_params (StoreParams): Storage configuration parameters. TODO ! as
        written in the comment from story 800, point 3: About the storage_configuration.json : for the time being,
        just consider s3 configuration. No credential should be revealed. It is up to CPM to resolve secret.
        flow_env (FlowEnv): Flow execution context, providing environment details like owner ID.
        config_rows (list[list[str]]): Parsed S3 bucket configuration entries.

    Returns:
        list[OutputProduct]: A list of output product configuration objects.

    Raises:
        RuntimeError: If an output mapping or configuration rule cannot be found.
    """

    outputs = []
    for output_product in dpr_process_in.generated_product_to_collection_identifier:
        product_name = next(iter(output_product))
        mapping = next((outp for outp in unit.get("output_products", []) if outp["name"] == product_name), None)

        if not mapping:
            raise RuntimeError(f"Couldn't find any output for task table entry '{product_name}'")

        product_type_and_collection = output_product[product_name]
        if isinstance(product_type_and_collection, tuple):
            product_type, output_collection = product_type_and_collection
        elif isinstance(product_type_and_collection, str):
            product_type = output_collection = product_type_and_collection
        else:
            raise RuntimeError(f"Invalid output_products definition for '{product_name}'")

        bucket_name = find_s3_output_bucket(config_rows, flow_env.owner_id, output_collection, product_type)

        output_path = os.path.join("s3://", bucket_name, flow_env.owner_id, output_collection)

        outputs.append(
            OutputProduct(
                id=mapping["name"],
                path=output_path,
                store_type=mapping["store_type"],
                store_params=store_params,
                type=mapping.get("type", "filename"),
                opening_mode=mapping.get("opening_mode", "CREATE"),
            ),
        )

    return outputs


def get_io(unit, dpr_process_in: DprProcessIn, store_params: StoreParams, flow_env: FlowEnv) -> tuple[list, list]:
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

    config_rows = fetch_csv_from_endpoint(os.environ["RSPY_HOST_OSAM"] + "/storage/configuration")

    inputs = build_input_products(unit, dpr_process_in, store_params, catalog_client)
    outputs = build_output_products(unit, dpr_process_in, store_params, flow_env, config_rows)

    return inputs, outputs


def load_store_params_from_config(config_path: str = "/etc/storage_configuration.json") -> StoreParams:
    """
    Loads storage configuration from a JSON file and constructs a StoreParams object.

    Args:
        config_path (str): Path to the storage configuration JSON file.
            Defaults to '/etc/storage_configuration.json'.

    Returns:
        StoreParams: The StoreParams object built from the configuration file.

    Raises:
        FileNotFoundError: If the JSON file does not exist.
        ValueError: If the JSON structure is invalid or missing required fields.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Storage configuration file not found: {config_path}")

    with open(config_path, encoding="utf-8") as f:
        storage_config = json.load(f)

    store_options = []

    for storage_entry in storage_config.get("storage", []):
        name = storage_entry.get("name")
        if not name:
            continue

        # S3 configuration
        if name == "s3":
            opts = StorageOptions(
                name="s3",
                key=f"${{{storage_entry['storage_options']['key']}}}",
                secret=f"${{{storage_entry['storage_options']['secret']}}}",
                client_kwargs={
                    "endpoint_url": storage_entry["storage_options"]["endpoint_url"],
                    "region_name": storage_entry["storage_options"]["region_name"],
                },
            )
            # store_options.append(StoreOptionsWrapper(storage_options=[opts]))

            # Non-S3 storage: shared_disk or local_disk
            # TODO: How do we record these ?
            # else:
            #     opts = StorageOptions(
            #         name=name,
            #         key=None,
            #         secret=None,
            #         client_kwargs={
            #             "opening_mode": storage_entry["opening_mode"],
            #             "relative_path": storage_entry["relative_path"],
            #         },
            #     )
            # store_options.append(StoreOptionsWrapper(storage_options=[opts]))
            store_options.append(opts)

    return StoreParams(storage_options=store_options)


def build_mockup_payload(owner_id):
    """
    Builds a mock payload schema for testing or demonstration purposes.

    This function generates a simplified PayloadSchema structure used for validating
    data processing pipeline integration without invoking actual DPR (Data Processing Request)
    logic. It creates one mock workflow step, one input product, and two output products
    pointing to the specified S3 output location.

    The resulting payload emulates a minimal working configuration for a single-unit
    processor named mockup_processor, with placeholder input and output data paths.

    Args:
        s3_output_data (str): S3 path (e.g., 's3://bucket/output/path') representing
            the output location for the mock products.

    Returns:
        PayloadSchema: A fully populated payload schema containing:
            - A single workflow step (mockup_processor)
            - One mock input product (S3ACADUS)
            - Two mock output products (S3MWRL0_, S3OLCL0_)
            - A default general configuration section
            - No adfs (sets it to [])

    Notes:
        - This mock payload is typically used for testing DPR endpoints or
          integration pipelines when real input data or cluster processing
          is not required.
        - The 'dask_context' section is intentionally omitted, as it is expected
          to be injected later by the DPR service layer.
    """
    mockup_output_products = ["S03MWRL0_", "S03OLCL0_"]
    workflow_steps = [
        WorkflowStep(
            name="mockup_processor",
            active=True,
            validate=False,
            module="lm.sm.mockup_processor",
            processing_unit="single_unit",
            inputs=[{"S3ACADUS": "S3ACADUS"}],
            adfs=None,
            outputs=[{"out1": mockup_output_products[0]}, {"out2": mockup_output_products[1]}],
            parameters=None,
        ),
    ]
    input_products = [
        InputProduct(
            id="S3ACADUS",
            path="s3://mockup_input_path",
            store_type="cadu",
            store_params=None,
        ),
    ]

    output_products = [
        OutputProduct(
            id=outp,
            path=f"s3://rs-dev-cluster-temp/dpr_mockup_results/{owner_id}/TEST_FLOW_OUTPUT/",
            store_type="zarr",
            type="folder",
            store_params=None,
        )
        for outp in mockup_output_products
    ]
    io_config = IOConfig(
        input_products=input_products,
        output_products=output_products,
    )
    return PayloadSchema(
        # add some default params, as stated in a comment from jira (story 800)
        general_configuration=GeneralConfiguration(),
        workflow=workflow_steps,
        io=io_config,  # type: ignore
        # The dask_context section is updated in dpr_service
        # dask_context=dask_context,
    )


@task(name="Generate payload file")
def generate_payload(  # pylint: disable=unused-argument
    flow_env: FlowEnv,
    unit_list: list[dict],
    adfs: list[tuple[str, str]],
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
        adfs (list[tuple[str, str]]): List of auxiliary item
            tuples, where each tuple includes the eopf type and the s3 storage path.
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

    if dpr_process_in.processor_name == DprProcessor.MOCKUP:
        logger.info("Generating payload for mockup processor")
        # TODO: the ouput path can be also computed, by using the following 3 lines
        # and add output_mockup_path as param to build_mockup_payload
        # config_file_path = os.getenv(FILEPATH_ENV_VAR, DEFAULT_FILEPATH)
        # config_rows = fetch_csv_from_endpoint(config_file_path)
        # output_mockup_path=build_output_products(unit_list[0], dpr_process_in, store_params, flow_env, config_rows)
        return build_mockup_payload(flow_env.owner_id)

    logger.info("Loading StoreParams configuration")
    store_params = load_store_params_from_config()

    workflow_steps = []
    io_config = IOConfig()
    logger.info("Geting workflow and I/O sections")
    for unit in unit_list:
        try:
            workflow_steps.append(build_workflow_step(unit))
            input_products, output_products = get_io(
                unit,
                dpr_process_in,
                store_params,
                flow_env,
            )
            io_config.input_products += input_products
            io_config.output_products += output_products
        except KeyError as ke:
            raise ValueError(f"Key {ke} not found in unit list") from ke

    io_config.adfs = [AdfConfig(id=adf[0], path=adf[1], store_params=store_params) for adf in adfs]
    # Build the dask context
    # dask_context = DaskContext(
    #     address="test",
    # )
    # Build the full payload using the schema
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
            case DprProcessor.S3L0:
                config = ["/opt/dask-l0/s3_default_configuration.yaml", "/opt/dask-l0/cadu_configuration.yaml"]
    logger.info("Building the payload")
    payload = PayloadSchema(
        # add some default params, as stated in a comment from jira (story 800)
        general_configuration=GeneralConfiguration(),
        workflow=workflow_steps,
        io=io_config,  # type: ignore
        # The dask_context section is updated in dpr_service
        # dask_context=dask_context,
        logging=logging,
        config=config,
    )
    logger.info(f"Generated payload: \n {payload}")
    return payload
