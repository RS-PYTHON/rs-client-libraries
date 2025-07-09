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

"""Prefect flows and tasks for on-demand processing"""

import datetime
from pathlib import Path

from prefect import flow, get_run_logger, task
from pystac import ItemCollection

from rs_workflows import auxip_flow, cadip_flow, catalog_flow
from rs_workflows.dpr_flow import (
    read_payload_values,
    read_tasktable,
    run_processor,
    write_payload,
)
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, ProcessorEnum
from rs_workflows.staging_flow import staging_task_auxip, staging_task_cadip


@flow(name="On-demand processing")
async def on_demand_processing(
    env: FlowEnvArgs,
    processor: ProcessorEnum,
    cadip_collection_identifier: str,
    session_identifier: str,
    catalog_collection_identifier: str,
    s3_payload_template: str,
    s3_output_data: str,
    use_dpr_mockup: bool = False,
):
    """
    Prefect flow for on-demand processing.

    Args:
        env: Prefect flow environment
        processor: DPR processor name
        cadip_collection_identifier: CADIP collection identifier that contains the mission and station
            (e.g. s1_ins for Sentinel-1 sessions from the Inuvik station)
        session_identifier: Session identifier
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged
        s3_payload_template: S3 bucket location of the DPR payload file template.
        s3_output_data: S3 bucket location of the output processed products. They will then be copied to the
        catalog bucket.
        use_dpr_mockup: Use the real or the mockup DPR processor ?
    """
    # logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "on-demand-processing"):

        # Read values from the payload file
        payload_values = read_payload_values.submit(s3_payload_template)

        # Search Cadip sessions
        cadip_items = cadip_flow.search_task.submit(
            flow_env.serialize(),
            cadip_collection_identifier,
            session_identifier,
            error_if_empty=True,
        )

        # Read Auxip CQL2 filter from the processor tasktable.
        auxip_cql2 = read_tasktable.submit(flow_env.serialize(), processor, payload_values, cadip_items)

        # Search Auxip products
        auxip_items = auxip_flow.search_task.submit(flow_env.serialize(), auxip_cql2, error_if_empty=True)

        # Auxip and Cadip item ids
        item_ids = []
        for items in [cadip_items.result(), auxip_items.result()]:
            for item in items or []:  # type: ignore[union-attr]
                item_ids.append(item.id)

        # Stage Auxip and Cadip items.
        # Note: the only difference between staging_task_auxip and
        # staging_task_cadip is the task name in the prefect dashboard.
        staged = [
            staging_task_auxip.submit(
                flow_env.serialize(),
                auxip_items,
                catalog_collection_identifier,
            ),
            staging_task_cadip.submit(
                flow_env.serialize(),
                cadip_items,
                catalog_collection_identifier,
            ),
        ]

        # Write the final payload file from its template version and staged items.
        # It will be uploaded in the same s3 dir than the template file.
        s3_payload_run = s3_payload_template + ".run" + Path(s3_payload_template).suffix
        written = write_payload.submit(
            flow_env.serialize(),
            s3_payload_template,
            item_ids,
            catalog_collection_identifier,
            s3_output_data,
            s3_payload_run,
            wait_for=staged,  # wait for items to be staged in the catalog
        )

        # Run the DPR processor
        processed_items = run_processor.submit(
            flow_env.serialize(),
            processor,
            s3_payload_run,
            use_dpr_mockup,
            wait_for=written,
        )

        # Publish processed items to the catalog
        published = catalog_flow.publish.submit(
            flow_env.serialize(),
            catalog_collection_identifier,
            processed_items,
            s3_output_data,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        published.result()  # type: ignore[unused-coroutine]


@flow(name="On-demand Cadip staging")
async def on_demand_cadip_staging(
    env: FlowEnvArgs,
    cadip_collection_identifier: str,
    session_identifier: str,
    catalog_collection_identifier: str,
):
    """
    Flow to retrieve a session, stage it and add the STAC item into the catalog.

    Args:
        env: Prefect flow environment
        cadip_collection_identifier: CADIP collection identifier that contains the mission and station
            (e.g. s1_ins for Sentinel-1 sessions from the Inuvik station)
        session_identifier: Session identifier
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged
    """

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "on-demand-cadip-staging"):

        # Search Cadip sessions
        cadip_items = cadip_flow.search_task.submit(
            flow_env.serialize(),
            cadip_collection_identifier,
            session_identifier,
            error_if_empty=True,
        )

        # Stage Cadip items.
        staged = staging_task_cadip.submit(flow_env.serialize(), cadip_items, catalog_collection_identifier)

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        staged.result()  # type: ignore[unused-coroutine]


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

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "on-demand-auxip-staging"):

        # Convert datetime inputs to str
        if isinstance(start_datetime, datetime.datetime):
            start_datetime = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if isinstance(end_datetime, datetime.datetime):
            end_datetime = end_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # CQL2 ValCover filter. The product:type filter is not added here
        # because for some reason composite filters with "and" don't work
        cql2_filter = {
            "op": "t_contains",
            "args": [
                {"interval": [{"property": "start_datetime"}, {"property": "end_datetime"}]},
                {"interval": [start_datetime, end_datetime]},
            ],
        }

        # Search Auxip products
        auxip_items = auxip_flow.search_task.submit(
            flow_env.serialize(),
            auxip_cql2={"filter": cql2_filter},
            error_if_empty=False,
        )

        # Filtering Auxip items to only keep the ones with the correct product type
        items_to_stage = filter_product_type.submit(auxip_items, product_type)

        # Stage Auxip items.
        staged = staging_task_auxip.submit(
            flow_env.serialize(),
            items_to_stage,
            catalog_collection_identifier,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        staged.result()  # type: ignore[unused-coroutine]


@task(name="Filter product type")
async def filter_product_type(auxip_items: ItemCollection | None, product_type: str) -> ItemCollection:
    """Filter Auxip items to only keep the ones with the correct product type"""
    logger = get_run_logger()
    items_to_stage = []

    if auxip_items:
        for item in auxip_items:  # type: ignore[attr-defined]
            if "product:type" in item.properties.keys() and item.properties["product:type"] == product_type:
                items_to_stage.append(item)

    logger.info(f"Filtering search results: {len(items_to_stage)} item(s) found of product type {product_type}")
    return ItemCollection(items_to_stage)
