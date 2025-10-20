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
# pylint: disable=W0101  # ignore 'unreachable code' (temporar)

import datetime
import json
import os

from prefect import flow, task
from prefect.artifacts import acreate_markdown_artifact

from rs_client.ogcapi.dpr_client import ClusterInfo, DprProcessor
from rs_common import prefect_utils
from rs_common.utils import create_valcover_filter
from rs_workflows import auxip_flow, cadip_flow, prip_flow
from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnv,
    FlowEnvArgs,
)
from rs_workflows.payload_builder import build_cql2_json, build_unit_list
from rs_workflows.staging_flow import staging_task


@task(name="Process input ADFS")
async def process_input_adfs(input_adfs, dpr_input, task_table):
    """
    Stage ADFS files from the tasktable.
    """
    # Return list of auxip items
    all_auxip_items = []

    try:

        # For each "alternative" ( get it following the "order" )
        for idx, alternative in enumerate(input_adfs["alternatives"]):
            # 1. Get the "query" with the "parameters" and "timeout_seconds" information
            # 2. Get the corresponding "query.name" on the section "query" of the task table
            timeout = alternative["timeout_seconds"]  # pylint: disable = unused-variable
            name, parameters = alternative["query"]["name"], alternative["query"]["parameters"]
            # 3. Build the CQL2 JSON by replacing the parameters. Only keep the "stac" field.
            auxip_cql2 = build_cql2_json(task_table, name, parameters)["stac"]
            # save auxip cql2 json as flow artefact
            md = "# Auxip CQL2 filter \n\n```json\n" + json.dumps(auxip_cql2, indent=2) + "\n```"
            # Artifact key must only contain lowercase letters, numbers, and dashes.
            await acreate_markdown_artifact(key="auxip-cql2", markdown=md, description="Auxip CQL2 filter")
            # 4.Choose the mission-aux for "catalog_collection_identifier" between s1-aux, s2-aux or s3-aux
            product_type = parameters.get("product_type", "*")
            default_aux_collection = f"{dpr_input.satellite}-aux-{product_type}"
            collection = dpr_input.auxiliary_product_to_collection_identifier.get(
                product_type,
                dpr_input.auxiliary_product_to_collection_identifier.get("*", default_aux_collection),
            )
            # 5. Call the flow "auxip-staging" with stac_query, catalog_collection_identifier, timeout
            auxip_items = auxip_flow.auxip_staging_task.submit(
                dpr_input.env,
                auxip_cql2,
                collection,
                timeout if timeout else -1,
            ).result()

            all_auxip_items.append(auxip_items)

            if idx == len(input_adfs["alternatives"]) - 1 and not auxip_items:
                #  Last one and still nothing → raise runtime
                raise RuntimeError("All ADFS searched, no items found.")

        return all_auxip_items

    except KeyError as kerr:
        raise RuntimeError(
            f"Unable to read / process tasktable and build cql2-json for: {json.dumps(input_adfs)}",
        ) from kerr


@flow(name="dpr-processing")
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
    """

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(dpr_input.env)

    with flow_env.start_span(__name__, "dpr-processing"):

        # Create cluster info from JUPYTERHUB_API_TOKEN env var (only in cluster mode, read from the
        # prefect blocks) and Dask cluster label.
        cluster_info = ClusterInfo(
            jupyter_token=os.environ["JUPYTERHUB_API_TOKEN"] if prefect_utils.cluster_mode else "",
            cluster_label=dpr_input.dask_cluster_label,
        )

        # read tasktable and construct list of processing units
        if dpr_input.processor_name == DprProcessor.MOCKUP:
            # pylint: disable-next=unused-variable
            s3_payload_template = (  # noqa: F841
                f"s3://rs-dev-cluster-temp/prefect-share/users/{flow_env.owner_id}/"
                f"l0/config/s3/s3_l0_demo_payload_dpr_mockup_template.yaml"
            )
        task_table = flow_env.rs_client.get_dpr_client().get_process(dpr_input.processor_name.value, cluster_info)
        processing_mode = list(dpr_input.processing_mode) if dpr_input.processing_mode else None
        out = build_unit_list(
            tasktable=task_table,
            pipeline=dpr_input.pipeline,
            unit=dpr_input.unit,
            processing_mode=processing_mode,
            start_datetime=dpr_input.start_datetime,
            end_datetime=dpr_input.end_datetime,
        )
        unit_list = out["units"]
        md = "# List of processing units\n\n```json\n" + json.dumps(unit_list, indent=2) + "\n```"
        # Artifact key must only contain lowercase letters, numbers, and dashes.
        await acreate_markdown_artifact(key="processing-unit-list", markdown=md, description="List of processing units")

        tasks = []
        for unit in unit_list:
            # For each input_adfs element computed on STEP 1
            for input_adfs in unit["input_adfs"]:
                tasks.append(process_input_adfs.submit(input_adfs, dpr_input, task_table))

        # pylint: disable-next=unused-variable
        auxip_items = [item for t in tasks for item in t.result()]  # noqa: F841

        # Wait for Alex part
        return


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
        staged = staging_task.submit(flow_env.serialize(), cadip_items, catalog_collection_identifier)

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

        # CQL2 filter: filter on product type and time interval
        cql2_filter = create_valcover_filter(start_datetime, end_datetime, product_type)

        # Search Prip products
        prip_items = prip_flow.search_task.submit(
            flow_env.serialize(),
            prip_cql2={"filter": cql2_filter},
            prip_collection=prip_collection,
            error_if_empty=False,
        )

        # Stage Prip items
        staged = staging_task.submit(
            flow_env.serialize(),
            prip_items,
            catalog_collection_identifier,
        )

        # Wait for last task to end (unwrap exceptions if any)
        staged.result()  # type: ignore[unused-coroutine]
