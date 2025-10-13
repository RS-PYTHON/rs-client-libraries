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
from prefect.artifacts import acreate_table_artifact
from pystac import Item, ItemCollection

from rs_client.stac.auxip_client import AuxipClient
from rs_workflows.catalog_flow import catalog_search_task
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.staging_flow import staging_task_auxip

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
):
    """
    Generic flow to retrieve a list of items matching the STAC CQL2 filter given, and to stage the ones
    that are not already in the catalog.

    Args:
        env (FlowEnvArgs): Prefect flow environment
        stac_query (dict): CQL2 filter to select which files to stage
        catalog_collection_identifier (str): Catalog collection identifier where CADIP sessions and AUX data are staged
        timeout_seconds (int): Timeout value for the Auxip search task.
            Optional, if no value is given the process will run until it is completed
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "auxip-staging"):

        # Search Auxip products
        auxip_search_task = search_task.submit(
            flow_env.serialize(),
            auxip_cql2=cql2_filter,
            error_if_empty=False,
        )

        # Timeout on search task
        if timeout_seconds >= 0:
            auxip_search_task.wait(timeout_seconds)

        auxip_items: ItemCollection = auxip_search_task.result()

        # Stop process if search task didn't return any item
        if len(auxip_items) == 0:
            logger.info("Nothing to stage: Auxip search with given filter returned empty result.")
            return True, None

        # Search catalog items
        catalog_items: ItemCollection = catalog_search_task.submit(
            flow_env.serialize(),
            catalog_cql2=cql2_filter,
            error_if_empty=False,
        ).result()

        # Compare results of Auxip search with results of Catalog search
        # to see what is missing in Catalog
        missing_items_list: list[Item] = []
        for item in auxip_items:
            if item not in catalog_items:
                missing_items_list.append(item)
        missing_auxip_items = ItemCollection(missing_items_list)
        logger.info(f"Number of items missing in the catalog to stage: {len(missing_auxip_items)}")

        # Stop process if all the Auxip items found are already in the catalog
        if len(missing_auxip_items) == 0:
            logger.info("Nothing to stage: all Auxip items found are already in the catalog.")
            return True, None

        # Stage missing Auxip items
        staged = staging_task_auxip.submit(
            flow_env.serialize(),
            missing_auxip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        staging_results: dict = staged.result()

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
            await acreate_table_artifact(
                table=[missing_auxip_items.to_dict()],
                key="auxiliary-files",
                description="Auxiliary files added to catalog.",
            )

        return return_status, missing_auxip_items


@flow(name="On-demand Auxip staging")
async def on_demand_auxip_staging(
    env: FlowEnvArgs,
    start_datetime: datetime.datetime | str,
    end_datetime: datetime.datetime | str,
    product_type: str,
    catalog_collection_identifier: str,
):
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
    """

    # Convert datetime inputs to str
    if isinstance(start_datetime, datetime.datetime):
        start_datetime = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(end_datetime, datetime.datetime):
        end_datetime = end_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # CQL2 filter: we use a filter combining a ValCover filter and a product type filter
    cql2_filter = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "product:type"}, product_type]},
            {
                "op": "t_contains",
                "args": [
                    {"interval": [{"property": "start_datetime"}, {"property": "end_datetime"}]},
                    {"interval": [start_datetime, end_datetime]},
                ],
            },
        ],
    }

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
