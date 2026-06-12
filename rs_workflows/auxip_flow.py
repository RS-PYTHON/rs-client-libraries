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

"""Auxiliary product search and staging flows."""

import datetime
import json
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import acreate_markdown_artifact
from pystac import Item, ItemCollection

from rs_client.stac.catalog_client import CatalogClient
from rs_client.stac.stac_base import StacBase
from rs_common.utils import create_valcover_filter
from rs_workflows.flow_utils import AuxiliarySource, FlowEnv, FlowEnvArgs
from rs_workflows.staging_flow import staging_task
from rs_workflows.utils.utils import asset_unzip_decompress

###################
# Auxiliary flows #
###################


def select_search_client_and_kwargs(
    flow_env: FlowEnv,
    source: AuxiliarySource,
) -> tuple[StacBase, dict[str, Any]]:
    """Select the STAC client and source-specific search kwargs."""
    if source == AuxiliarySource.AUXIP:
        return flow_env.rs_client.get_auxip_client(), {}
    if source == AuxiliarySource.CATALOG:
        return flow_env.rs_client.get_catalog_client(), {"owner_id": flow_env.owner_id}
    if source == AuxiliarySource.PRIP:
        return flow_env.rs_client.get_prip_client(), {}
    if source == AuxiliarySource.CADIP:
        return flow_env.rs_client.get_cadip_client(), {}
    if source == AuxiliarySource.CDSE:
        return flow_env.rs_client.get_cdse_client(), {}
    if source == AuxiliarySource.LTA:
        raise ValueError(
            "LTA auxiliary product search is not supported yet: rs-server does not expose an LTA STAC service "
            "endpoint and rs-client-libraries does not provide an LtaClient.",
        )
    raise ValueError(f"Unsupported auxiliary product source: {source.value!r}")


@flow(name="search-aux")
async def search(
    env: FlowEnvArgs,
    aux_cql2: dict,
    error_if_empty: bool = False,
    source: AuxiliarySource = AuxiliarySource.AUXIP,
) -> ItemCollection | None:
    """
    Search AUX products.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        aux_cql2: AUX CQL2 filter read from the processor tasktable.
        error_if_empty: Raise a ValueError if the results are empty.
        source: STAC source where auxiliary products are searched. Default: auxip.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "aux-search"):

        logger.info(f"Start AUX search from {source.value}: {aux_cql2}")
        search_client, source_search_kwargs = select_search_client_and_kwargs(
            flow_env,
            source,
        )
        found = search_client.search(
            method="POST",
            stac_filter=aux_cql2.get("filter"),
            max_items=aux_cql2.get("limit"),
            collections=aux_cql2.get("collections"),
            sortby=aux_cql2.get("sortby"),
            timestamp=aux_cql2.get("timestamp"),
            **source_search_kwargs,
        )
        if (not found) and error_if_empty:
            raise ValueError(
                f"❌ No AUX product found for CQL2 filter: {json.dumps(aux_cql2, indent=2)}",
            )
        logger.info(f"🔍 AUX search found {len(found)} result(s): {found.to_dict()}")
        return found


@flow(name="stage-aux")
async def aux_staging(
    env: FlowEnvArgs,
    cql2_filter: dict,
    catalog_collection_identifier: str,
    timeout_seconds: int = -1,
    source: AuxiliarySource = AuxiliarySource.AUXIP,
    selected_assets: list[str] | None = None,
) -> tuple[bool, ItemCollection | None]:
    """
    Generic flow to retrieve a list of items matching the STAC CQL2 filter given, and to stage the ones
    that are not already in the catalog.

    Args:
        env (FlowEnvArgs): Prefect flow environment
        stac_query (dict): CQL2 filter to select which files to stage
        catalog_collection_identifier (str): Catalog collection identifier where auxiliary data are staged
        timeout_seconds (int): Timeout value for the AUX search task.
            Optional, if no value is given the process will run until it is completed
        source (str): STAC source where auxiliary products are searched. Default: auxip.
        selected_assets: Optional asset keys to stage.

    Returns:
        bool: Return status: False if staging failed, True otherwise
        ItemCollection: List of catalog Items staged from AUX station
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "aux-staging"):

        # Search AUX products
        aux_items: ItemCollection | None = (
            search_task.with_options(timeout_seconds=timeout_seconds if timeout_seconds >= 0 else None)
            .submit(
                flow_env.serialize(),
                aux_cql2=cql2_filter,
                error_if_empty=False,
                source=source,
            )
            .result()  # type: ignore
        )

        # Stop process if search task didn't return any item
        if not aux_items or len(aux_items) == 0:
            logger.info("Nothing to stage: AUX search with given filter returned empty result.")
            return True, None

        # Catalog results are already staged items, so pass them directly to the next processing step.
        if source == AuxiliarySource.CATALOG:
            logger.info("AUX items found in catalog; skipping staging.")
            return True, aux_items

        # Stage AUX items
        asset_names = selected_assets or ({"product"} if source == AuxiliarySource.CDSE else None)
        staged = staging_task.submit(
            flow_env.serialize(),
            aux_items,
            catalog_collection_identifier,
            asset_names=asset_names,
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
                    f"❌ Staging job '{job_name}' with ID {job_result['jobID']} FAILED.\n"
                    f"Status: {job_result['status']} - Reason: {job_result['message']}",
                )
                logger.debug({job_name: job_result})
                return_status = False

        # Get staged items from catalog (to have the correct href)
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        catalog_items = ItemCollection(
            catalog_client.get_items(
                collection_id=catalog_collection_identifier,
                items_ids=[item.id for item in aux_items],
            ),
        )

        # Create artifact if all jobs succeeded
        if return_status:
            logger.info("✅ Staging successful, creating artifact with a list of staged items.")
            artifact_key_name: str = "auxiliary-stac-item"
            md = "# Auxiliary file \n\n```json\n" + json.dumps(catalog_items.to_dict(), indent=2) + "\n```"
            await acreate_markdown_artifact(
                markdown=md,
                key=artifact_key_name,
                description="Auxiliary files added to catalog.",
            )
            logger.info(f"📌 Artifact named '{artifact_key_name}' has been linked to this flow.")

        return return_status, catalog_items


@flow(name="On-demand AUX staging")
async def on_demand_aux_staging(
    env: FlowEnvArgs,
    start_datetime: datetime.datetime | str,
    end_datetime: datetime.datetime | str,
    product_type: str,
    catalog_collection_identifier: str,
) -> tuple[bool, ItemCollection | None]:
    """
    Flow to retrieve AUX files using a ValCover filter with the given time interval defined by
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
        catalog_collection_identifier: Catalog collection identifier where auxiliary data are staged

    Returns:
        bool: Return status: False if staging failed, True otherwise
        ItemCollection: List of Items retrieved from the AUX search and staged to the catalog
    """

    # CQL2 filter: we use a filter combining a ValCover filter and a product type filter
    cql2_filter = create_valcover_filter(start_datetime, end_datetime, product_type)

    return await aux_staging.fn(
        env=env,
        cql2_filter={"filter": cql2_filter},
        catalog_collection_identifier=catalog_collection_identifier,
    )


###########################
# Call the flows as tasks #
###########################


@task(name="search-aux")
async def search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)


@task(name="stage-aux")
async def aux_staging_task(*args, **kwargs) -> tuple[bool, ItemCollection | None]:
    """See: aux_staging"""
    return await aux_staging.fn(*args, **kwargs)


@task(name="Aux unzip and decompress")
async def aux_unzip_decompress_task(*args, **kwargs) -> Item:
    """See: aux_unzip_decompress"""
    return await asset_unzip_decompress.fn(*args, **kwargs)
