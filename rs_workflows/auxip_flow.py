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

"""Auxip flow implementation"""

import datetime
import json

from prefect import flow, get_run_logger, task
from prefect.artifacts import acreate_markdown_artifact
from pystac import ItemCollection

from rs_client.stac.auxip_client import AuxipClient
from rs_common.utils import create_valcover_filter
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.staging_flow import staging_task

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

        logger.info("Start Auxip search")
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
        logger.info(f"Auxip search found {len(found)} results: {found}")
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
        ItemCollection: List of Items retrieved from the Auxip search and staged to the catalog
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

        # Create artifact if all jobs succeeded
        if return_status:
            logger.info("Staging successful, creating artifact with a list of staged items.")
            await acreate_markdown_artifact(
                markdown=f"{json.dumps(auxip_items.to_dict(), indent=2)}",
                key="auxiliary-files",
                description="Auxiliary files added to catalog.",
            )

        return return_status, auxip_items


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
