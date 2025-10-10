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
import json
import os
from pathlib import Path

from prefect import flow
from prefect.artifacts import acreate_markdown_artifact

from rs_client.ogcapi.dpr_client import ClusterInfo
from rs_common import prefect_utils
from rs_workflows import auxip_flow, cadip_flow, catalog_flow, prip_flow
from rs_workflows.dpr_flow import (
    run_processor,
    write_payload,
)
from rs_workflows.flow_utils import DprProcessIn, FlowEnv, FlowEnvArgs, ProcessorEnum
from rs_workflows.payload_builder import build_units_list
from rs_workflows.staging_flow import (
    staging_task_auxip,
    staging_task_cadip,
    staging_task_prip,
)


@flow(name="dpr-process")
async def dpr_processing(
    dpr_input: DprProcessIn,
):
    """
    Prefect flow for dpr-process.

    Args:
        env: Prefect flow environment
        processor: DPR processor name
        cluster_label (str): Dask cluster label e.g. "dask-l0"
        cadip_collection_identifier: CADIP collection identifier that contains the mission and station
            (e.g. s1_ins for Sentinel-1 sessions from the Inuvik station)
        session_identifier: Session identifier
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged
        s3_payload_template: S3 bucket location of the DPR payload file template.
        s3_output_data: S3 bucket location of the output processed products. They will then be copied to the
        catalog bucket.
        use_dpr_mockup: Use the real or the mockup DPR processor ?
    """
    s3_payload_template = (
        "s3://rs-dev-cluster-temp/prefect-share/users/abutu/l0/config/s3/s3_l0_demo_payload_dpr_mockup_template.yaml"
    )
    auxip_cql2 = {}
    catalog_collection_identifier = "SPRINT24_TEST_COLLECTION"
    cadip_collection_identifier = "sgs_sentinel1"
    session_identifier = "S1A_20200105072204051312"
    processor = ProcessorEnum.S3L0
    s3_output_data = "s3://rs-dev-cluster-temp/prefect-share/users/abutu/l0/output/s3"

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(dpr_input.env)
    with flow_env.start_span(__name__, "on-demand-processing"):

        # Create cluster info from JUPYTERHUB_API_TOKEN env var (only in cluster mode, read from the
        # prefect blocks) and Dask cluster label.
        cluster_info = ClusterInfo(
            jupyter_token=os.environ["JUPYTERHUB_API_TOKEN"] if prefect_utils.cluster_mode else "",
            cluster_label=dpr_input.dask_cluster_label,
        )

        # read tasktable and construct list of processing units
        if not dpr_input.use_dpr_mockup:
            tt = flow_env.rs_client.get_dpr_client().get_process(dpr_input.processor_name.value, cluster_info)
        else:
            tt = flow_env.rs_client.get_dpr_client().get_process("mockup", cluster_info)
            s3_payload_template = (
                f"s3://rs-dev-cluster-temp/prefect-share/users/{flow_env.owner_id}/"
                f"l0/config/s3/s3_l0_demo_payload_dpr_mockup_template.yaml"
            )
        out = build_units_list(
            tasktable=tt,
            pipeline=dpr_input.pipeline,
            unit=dpr_input.unit,
            processing_mode=dpr_input.processing_mode,
        )
        units_list = out["units"]
        md = "# Units list\n\n```json\n" + json.dumps(units_list, indent=2) + "\n```"
        # Artifact key must only contain lowercase letters, numbers, and dashes.
        await acreate_markdown_artifact(key="units-list", markdown=md, description="List of processing units")

        # Search Cadip sessions
        cadip_items = cadip_flow.search_task.submit(
            flow_env.serialize(),
            cadip_collection_identifier,
            session_identifier,
            error_if_empty=False,
        )
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
            cluster_info,
            s3_payload_run,
            dpr_input.use_dpr_mockup,
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


@flow(name="On-demand Prip staging")
async def on_demand_prip_staging(
    env: FlowEnvArgs,
    start_datetime: datetime.datetime | str,
    end_datetime: datetime.datetime | str,
    product_type: str,
    prip_collection: str,
    catalog_collection_identifier: str,
):
    """
    Flow to retrieve Prip files with the given time interval defined by
    start_datetime and end_datetime, select only the type of files wanted,
    stage the files and add STAC items into the catalog.

    Args:
        env: Prefect flow environment
        start_datetime: Start datetime for the time interval used to filter the files
            (date or timestamp, e.g. "2025-08-07T11:51:12.509000Z")
        end_datetime: End datetime for the time interval used to filter the files
            (date or timestamp, e.g. "2025-08-10T14:00:00.509000Z")
        product_type: Prip product type wanted
        prip_collection: PRIP collection identifier (station)
        catalog_collection_identifier: Catalog collection identifier where PRIP data are staged
    """

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "on-demand-prip-staging"):

        # Convert datetime inputs to str
        if isinstance(start_datetime, datetime.datetime):
            start_datetime = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if isinstance(end_datetime, datetime.datetime):
            end_datetime = end_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # CQL2 filter: filter on product type and time interval
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

        # Search Prip products
        prip_items = prip_flow.search_task.submit(
            flow_env.serialize(),
            prip_cql2={"filter": cql2_filter},
            prip_collection=prip_collection,
            error_if_empty=False,
        )

        # Stage Prip items
        staged = staging_task_prip.submit(
            flow_env.serialize(),
            prip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end (unwrap exceptions if any)
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

        # Search Auxip products
        auxip_items = auxip_flow.search_task.submit(
            flow_env.serialize(),
            auxip_cql2={"filter": cql2_filter},
            error_if_empty=False,
        )

        # Stage Auxip items.
        staged = staging_task_auxip.submit(
            flow_env.serialize(),
            auxip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end.
        # NOTE: use .result() and not .wait() to unwrap and propagate exceptions, if any.
        staged.result()  # type: ignore[unused-coroutine]
