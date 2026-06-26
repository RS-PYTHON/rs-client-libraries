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

"""Prefect flows and tasks for on-demand processing"""

# pylint: disable=W0101  # ignore 'unreachable code' (temporar)

import datetime
import json
import os
import re
import tempfile as std_tempfile
from copy import deepcopy
from typing import Any

import anyio
import yaml
from prefect import flow, get_run_logger, task
from prefect.artifacts import acreate_markdown_artifact
from pystac import Item, ItemCollection

from rs_client.ogcapi.dpr_client import ClusterInfo
from rs_client.rs_client import RsClient
from rs_client.stac.catalog_client import CatalogClient
from rs_common import prefect_utils
from rs_workflows import aux_flow, catalog_flow, earthdatahub_flow
from rs_workflows.dpr_flow import run_processor
from rs_workflows.flow_utils import (
    AuxiliarySource,
    DprProcessIn,
    FlowEnv,
    FlowInputProduct,
    RetryConfig,
)
from rs_workflows.payload_builder import (
    build_cql2_json,
    build_unit_list,
    extract_external_modules,
)
from rs_workflows.payload_generator import generate_payload, resolve_stac_input_path
from rs_workflows.utils.utils import get_archived_item_indexes, search_by_name

SPECIFIC_INPUT_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\}")


def build_dask_dashboard_url_message(cluster_instance: str | None) -> str:
    """Build the Dask dashboard log message from the configured public gateway endpoint."""
    public_base = os.getenv("DASK_GATEWAY_PUBLIC", "")

    if not public_base or not cluster_instance:
        return "Dask cluster dashboard URL is unavailable"

    dashboard_url = f"{public_base.rstrip('/')}/clusters/{cluster_instance}/status"
    return f"Dask cluster dashboard URL: {dashboard_url}"


def _select_aux_collection_and_source(
    dpr_input: DprProcessIn,
    product_type: str,
) -> tuple[str, AuxiliarySource, list[str] | None]:
    """
    Resolve the catalog collection identifier and STAC source for a requested AUX product type.

    The selection logic mirrors the previous inline implementation used by
    ``process_input_adfs``:
    - prefer the collection explicitly mapped to the requested ``product_type``
    - otherwise fall back to the wildcard mapping (``*``) when present
    - otherwise derive the default convention-based collection name from the
      satellite and product type

    Keeping this lookup in a dedicated helper makes the fallback chain easier to
    read and test independently from the staging flow logic.
    """
    default_aux_collection = f"{dpr_input.satellite}-aux-{product_type}"
    return next(
        (
            (p.collection_name, p.source, p.selected_assets)
            for p in dpr_input.auxiliary_product_to_collection_identifier
            if p.product_type == product_type
        ),
        next(
            (
                (p.collection_name, p.source, p.selected_assets)
                for p in dpr_input.auxiliary_product_to_collection_identifier
                if p.product_type == "*"
            ),
            (
                default_aux_collection,
                AuxiliarySource.AUXIP,
                None,
            ),
        ),
    )


async def _build_aux_request(
    alternative,
    input_adfs,
    dpr_input: DprProcessIn,
    task_table: dict[str, Any],
    specific_input_product: tuple[str | None, Item | None] = (None, None),
) -> tuple[dict, str, int, AuxiliarySource, list[str] | None]:
    """
    Build the AUX request data for a single ADFS alternative.

    For one alternative definition taken from the task table input, this helper:
    - extracts the timeout, query name and query parameters
    - renders the final STAC/CQL2 payload with placeholders resolved
    - stores the generated CQL2 filter as a Prefect artifact for observability
    - resolves the target AUX catalog collection for the requested product type

    The returned tuple contains everything needed by the staging call:
    ``(aux_cql2, collection, timeout, source, selected_assets)``.
    """
    logger = get_run_logger()
    timeout = alternative["timeout_seconds"]  # pylint: disable = unused-variable
    name = alternative["query"]["name"]
    specific_input_name, stac_item = specific_input_product
    parameters = {
        k: (
            stac_item.properties.get(m.group(2), v)
            if stac_item
            and isinstance(v, str)
            and (m := SPECIFIC_INPUT_PATTERN.match(v))
            and m.group(1) == specific_input_name
            else v
        )
        for k, v in deepcopy(alternative["query"]["parameters"]).items()
    }
    query = next(q for q in task_table["queries"] if q["name"] == name)
    aux_cql2 = build_cql2_json(query, parameters)

    md = "# AUX CQL2 filter \n\n```json\n" + json.dumps(aux_cql2, indent=2) + "\n```"
    artifact_key_name: str = "aux-cql2-filter"
    await acreate_markdown_artifact(key=artifact_key_name, markdown=md, description="AUX CQL2 filter")
    logger.info(f"📌 Artifact named '{artifact_key_name}' has been linked to this flow.")

    product_type = parameters.get("product_type", "*")
    collection, source, selected_assets = _select_aux_collection_and_source(dpr_input, product_type)
    get_run_logger().info(
        f"🚧 Prepared AUX request for input {input_adfs['name']} "
        f"using source {source} and collection {collection}:🧹 {aux_cql2}",
    )
    return aux_cql2, collection, timeout if timeout else -1, source, selected_assets


async def _normalize_archived_aux_items(item_collection: ItemCollection, dpr_input: DprProcessIn) -> ItemCollection:
    """
    Normalize archived AUX items and persist the updated metadata to the catalog.

    When staged AUX items still point to archived content, this helper:
    - finds the affected items in the collection
    - submits one normalization task per archived item
    - waits for all normalization results
    - replaces the corresponding items in the in-memory collection
    - updates the catalog so the stored STAC item reflects the new asset hrefs

    If no archived assets are present, the original collection is returned
    unchanged.
    """
    logger = get_run_logger()
    archived_indexes = get_archived_item_indexes(item_collection)

    if not archived_indexes:
        return item_collection

    tasks = []
    for idx in archived_indexes:
        aux_item = item_collection.items[idx]
        logger.info(
            "The following staged ADFS asset is archived/compressed "
            f"{aux_item.to_dict()}. Starting normalization task",
        )
        tasks.append(aux_flow.aux_unzip_decompress_task.submit(aux_item))

    results = [task.result() for task in tasks]

    try:
        flow_env = FlowEnv(dpr_input.env)
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        for idx, new_item in zip(archived_indexes, results):
            logger.debug(f"Results after processing ADFS assets: {new_item.to_dict()}")  # type: ignore
            item_collection.items[idx] = new_item
            catalog_client.update_item(new_item)  # type: ignore[arg-type]
    except Exception as err:
        raise RuntimeError(
            "Error while trying to update the item collection with the uncompressed/unzipped items. "
            "This error is likely due to a failure in the aux_unzip_decompress_task. "
            "Check previous logs for more details.",
        ) from err

    return item_collection


async def _stage_input_adfs_alternative(
    alternative,
    input_adfs: dict[str, Any],
    dpr_input: DprProcessIn,
    task_table: dict[str, Any],
    specific_input_product: tuple[str | None, Item | None] = (None, None),
    staging_retries: int = 3,
    staging_retry_delay: int = 60,
) -> tuple[str, str, tuple[bool, ItemCollection]] | None:
    """
    Stage one ADFS alternative and normalize archived outputs when needed.

    This helper encapsulates the "happy path" for a single alternative:
    1. build the final AUX request and resolve the target collection
    2. stage matching AUX items with the configured retry policy
    3. if the staged assets are still archived, normalize them and update the catalog
    4. return the same tuple shape expected by the caller

    It returns ``None`` when the staging flow succeeds technically but produces
    no item collection for the current alternative, allowing the caller to try
    the next alternative in order.
    """
    logger = get_run_logger()
    aux_cql2, collection, timeout, source, selected_assets = await _build_aux_request(
        alternative,
        input_adfs,
        dpr_input,
        task_table,
        specific_input_product,
    )
    logger.info(f"Selected ADFS collection {collection} for ADFS {input_adfs["name"]}")

    aux_status: bool
    aux_items: ItemCollection | None
    # Special case for Copernicus DEM available at Earthdatahub
    if input_adfs["name"] == "DEM":
        aux_items = earthdatahub_flow.earthdatahub_search_task.submit(
            dpr_input.env,
            aux_cql2,
        ).result()
        aux_status = True
    else:
        aux_status, aux_items = (
            aux_flow.aux_staging_task.with_options(
                retries=staging_retries,
                retry_delay_seconds=staging_retry_delay,
            )
            .submit(dpr_input.env, aux_cql2, collection, timeout, source, selected_assets)
            .result()  # type: ignore
        )

    if not aux_items:
        return None

    for aux_item in aux_items:
        logger.info(f"Staged ADFS: {aux_item}")

    item_collection = await _normalize_archived_aux_items(aux_items, dpr_input)

    logger.info(f"Finished processing input ADFS, ItemCollection size: {len(item_collection.items)}")
    logger.debug(f"Finished processing input ADFS, ItemCollection: {item_collection.to_dict()}")

    return input_adfs["name"], input_adfs["type"], (aux_status, item_collection)


@task(name="Process input ADFS")
async def process_input_adfs(
    input_adfs: dict[str, Any],
    dpr_input: DprProcessIn,
    task_table: dict[str, Any],
    specific_input_product: tuple[str | None, Item | None] = (None, None),
    staging_retries: int = 3,
    staging_retry_delay: int = 60,
) -> tuple[str, str, tuple[bool, ItemCollection]]:
    """
    Stage the ADFS inputs described in the task table for one processing unit input.

    The task iterates through the ordered alternatives defined for a given ADFS
    input and stops at the first alternative that produces staged items.

    For each alternative, the task:
    - builds the final AUX CQL2 request from the task table definition
    - resolves the target AUX collection identifier
    - runs AUX staging with retries
    - normalizes archived outputs when the staged assets still point to
      compressed archives
    - updates the catalog entries after normalization so downstream payload
      generation sees the final asset hrefs

    Returns:
        tuple[str, str, tuple[bool, ItemCollection]]:
            The input ADFS name and type together with the original staging status/item
            collection tuple shape expected by downstream code.

    Raises:
        RuntimeError:
            If no alternative returns staged data, or if the task table content
            cannot be read as expected.
    """
    logger = get_run_logger()
    logger.info(f"🚧 Starting processing input ADFS for {input_adfs}")
    try:
        # For each "alternative" ( get it following the "order" )
        for alternative in input_adfs.get("alternatives", []):
            result = await _stage_input_adfs_alternative(
                alternative,
                input_adfs,
                dpr_input,
                task_table,
                specific_input_product,
                staging_retries,
                staging_retry_delay,
            )
            if result is not None:
                return result

        raise RuntimeError(f"Searching for adfs input {input_adfs['name']} did not return any result")

    except KeyError as kerr:
        raise RuntimeError(
            f"Unable to read / process tasktable and build cql2-json for: {json.dumps(input_adfs)}",
        ) from kerr


def _resolve_specific_input_product_stac_items(
    input_adfs: dict[str, Any],
    task_table: dict[str, Any],
    unit: dict[str, Any],
    provided_input_products: list[FlowInputProduct],
    rs_client: RsClient,
) -> tuple[str, list[Item]] | tuple[None, list[None]]:
    input_adfs_io = search_by_name(task_table["io"], input_adfs["name"])
    if input_adfs_io.get("multiplicity", None) == "one_per_input":
        logger = get_run_logger()
        input_product_names: set[str] = {product["name"] for product in unit["input_products"]}
        referenced_input_product_names = {
            match.group(1)
            for alt in input_adfs_io.get("alternatives", [])
            for value in alt.get("query", {}).get("parameters", {}).values()
            if isinstance(value, str)
            for match in [re.fullmatch(r"\{([^\.}]+)\.[^}]+\}", value)]
            if match and match.group(1) in input_product_names
        }
        if len(referenced_input_product_names) != 1:
            raise ValueError(
                f"ADFS multiplicy 'one_per_input' shall refer to a single input product, "
                f"found '{referenced_input_product_names}'",
            )
        referenced_input_product_name = next(iter(referenced_input_product_names))
        logger.info(f"ADFS multiplicity 'one_per_input' refers to input '{referenced_input_product_name}'")
        input_product_io: dict[str, Any] = search_by_name(task_table["io"], referenced_input_product_name)
        input_product_regex: str = input_product_io.get("store_params", {}).get("regex", None)
        if not input_product_regex:
            logger.warning(
                f"⚠️ Input product '{referenced_input_product_name}' should define a regex "
                "to perform reliable discrimination",
            )
        result = []
        catalog_client: CatalogClient = rs_client.get_catalog_client()
        for input_product in provided_input_products:
            stac_item, first_asset_path = resolve_stac_input_path(
                catalog_client,
                input_product.collection_name,
                input_product.item_id,
            )
            if not first_asset_path:
                continue
            # Check that first asset path matches input product regex
            if input_product_regex:
                logger.info(f"Checking asset path '{first_asset_path}' against regex '{input_product_regex}'")
                if not re.fullmatch(input_product_regex, first_asset_path):
                    logger.debug(f"Ignore path {first_asset_path} as it doesn't match regex '{input_product_regex}'")
                    continue
            else:
                logger.warning(f"Adding asset '{first_asset_path}' without regex validation")
            result.append(stac_item)
        if not result:
            logger.error(f"❌ Found no STAC item with assets matching the regex '{input_product_regex}'")
        return referenced_input_product_name, result
    return None, [None]


@flow(name="process-generic")
async def dpr_processing(
    dpr_input: DprProcessIn,
    retry_config: RetryConfig = RetryConfig(),  # type: ignore
):
    """
    Prefect flow for dpr-process.

    Args:
        dpr_input: Input parameters for executing this flow
        retry_config: Staging retry config
    """
    logger = get_run_logger()
    logger.info(f"Starting the DPR processing flow with processor: {dpr_input.processor_name}")
    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(dpr_input.env)

    with flow_env.start_span(__name__, "dpr-processing"):

        # Create cluster info from JUPYTERHUB_API_TOKEN env var (only in cluster mode, read from the
        # prefect blocks) and Dask cluster label.
        cluster_info = ClusterInfo(
            jupyter_token=os.environ["JUPYTERHUB_API_TOKEN"] if prefect_utils.CLUSTER_MODE else "",
            cluster_label=dpr_input.dask_cluster_label,
            cluster_instance=dpr_input.dask_cluster_instance or "",
        )

        # read tasktable and construct list of processing units
        task_table: dict[str, Any] = flow_env.rs_client.get_dpr_client().get_process(
            dpr_input.processor_name,
            cluster_info,
        )

        # Persist the full task table as a Prefect artifact for later investigation.
        md = "# Task table\n\n```json\n" + json.dumps(task_table, indent=2) + "\n```"
        artifact_key_name: str = "dpr-task-table"
        await acreate_markdown_artifact(key=artifact_key_name, markdown=md, description="DPR task table")
        logger.info(f"📌 Artifact named '{artifact_key_name}' has been linked to this flow.")
        # Log the public Dask dashboard URL when the flow input provides the cluster instance.
        logger.info(build_dask_dashboard_url_message(cluster_info.cluster_instance))

        processing_mode = list(dpr_input.processing_mode) if dpr_input.processing_mode else None
        unit_list = build_unit_list(
            tasktable=task_table,
            pipeline=dpr_input.pipeline,
            unit=dpr_input.unit,
            processing_mode=processing_mode,
            external_variables={
                "start_datetime": dpr_input.start_datetime,
                "end_datetime": dpr_input.end_datetime,
                "reference_date": dpr_input.reference_date,
                "instrument_mode": dpr_input.instrument_mode,
                "satellite": dpr_input.satellite,
            },
        )

        tasks = []
        for unit in unit_list:
            # For each input_adfs element computed on STEP 1
            for input_adfs in unit["input_adfs"]:
                # For each specific input in case of multiplicity=one_per_input
                specific_input_name, product_stac_items = _resolve_specific_input_product_stac_items(
                    input_adfs,
                    task_table,
                    unit,
                    dpr_input.input_products,
                    flow_env.rs_client,
                )
                for specific_input_product_stac_item in product_stac_items:
                    if specific_input_product_stac_item:
                        logger.info(
                            f"Submitting {input_adfs['name']} ADFS task for input {specific_input_product_stac_item}",
                        )
                    tasks.append(
                        process_input_adfs.submit(
                            input_adfs,
                            dpr_input,
                            task_table,
                            (specific_input_name, specific_input_product_stac_item),
                            retry_config.staging_retries,
                            retry_config.staging_retry_delay,
                        ),
                    )

        try:
            aux_items: list[tuple[str, str, tuple[bool, ItemCollection]]] = [t.result() for t in tasks]
        except (RuntimeError, KeyError) as err:
            raise err
        # Set of ADFS. Each tuple includes the adfs name, type and the s3/https storage path
        source_items: list[Item] = []
        adfs: set[tuple[str, str, str]] = set()
        for name, adf_type, (status, item_collection) in aux_items:
            for item in item_collection.items:
                # list with links to be added in derived_from
                source_items.append(item)

                if status:
                    asset = next(iter(item.assets.values()))
                    logger.info(f"ADFS '{name}' of type '{adf_type}': {asset.href}")
                    adfs.add((name, adf_type, asset.href))
                else:
                    raise ValueError(f"The adf input files {next(iter(item.assets.values()))} was not correctly staged")

        # Get optional list of external_modules
        external_modules = extract_external_modules(task_table)

        # generate the dpr payload file
        task_future = generate_payload.submit(
            flow_env,
            unit_list,
            list(adfs),
            dpr_input,
            external_modules=external_modules,
        )
        # get the payload generation result
        generated_payload_res = task_future.result()
        # create the generated payload as a dictionary, as it will be used for
        # the prefect artifact. the SecretStr will be masked here
        generated_payload_res_as_dict = generated_payload_res.dump()
        # create the YAML string first (synchronous). This will be used for writing both the artifact as well
        # as the tmp file
        # md = "# Payload file\n\n```json\n" + json.dumps(generated_payload_res_as_dict, indent=2) + "\n```"
        yaml_str = yaml.dump(generated_payload_res_as_dict, default_flow_style=False, sort_keys=False)
        # Write the payload as prefect artifact
        pretty_markdown = f"```yaml\n{yaml_str}\n```"
        artifact_key_name = "dpr-payload"
        await acreate_markdown_artifact(
            key=artifact_key_name,
            markdown=pretty_markdown,
            description="DPR Payload file",
        )
        logger.info(f"📌 Artifact named '{artifact_key_name}' has been linked to this flow.")

        # re-create the generated payload as a dictionary, as it will be used for
        # the payload file to upload to S3. here, the secrets are revealed
        generated_payload_res_with_secrets = generated_payload_res.dump(reveal_secrets=True)
        yaml_str = yaml.dump(generated_payload_res_with_secrets, default_flow_style=False, sort_keys=False)
        # upload the config payload file to S3
        tmp_dir = std_tempfile.gettempdir()
        tmp_file_path = os.path.join(tmp_dir, f"dpr_payload_{datetime.datetime.now().timestamp()}.yaml")
        async with await anyio.open_file(tmp_file_path, "w", encoding="utf-8") as tmp_file:
            await tmp_file.write(yaml_str)
            # flush to be extra-safe
            await tmp_file.flush()
        logger.debug(f"Writing the payload to file :\n {dpr_input.s3_payload_file}")
        await prefect_utils.s3_upload_file(tmp_file_path, dpr_input.s3_payload_file)

        # clean up the temp payload file
        await anyio.Path(tmp_file_path).unlink()

        # Run the DPR processor
        processed_items = run_processor.submit(
            flow_env.serialize(),
            dpr_input.processor_name,
            generated_payload_res,
            cluster_info,
            dpr_input.s3_payload_file,
            dpr_input.input_products,
            wait_for=[task_future],
        )
        try:
            processed_items.result()
        finally:
            prefect_utils.s3_delete(dpr_input.s3_payload_file)

        # add derived_from link
        processed = processed_items.result()
        logger.debug(f"processed_items: {processed}")

        for processed_item in processed:
            processed_item.stac_item.add_derived_from(*source_items)

        # Publish processed items to the catalog
        published = catalog_flow.publish.submit(
            flow_env.serialize(),
            dpr_input.generated_product_to_collection_identifier,
            processed_items,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        published.result()  # type: ignore[unused-coroutine]

        return
