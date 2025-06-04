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

from pathlib import Path

from prefect import flow

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
    processor_enum: ProcessorEnum,
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
        env: Prefect flow environment (at least the owner_id is required)
        processor_enum: DPR processor name
        cadip_collection_identifier: CADIP collection identifier (to know the station)
        session_identifier: Session identifier
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged
        s3_payload_template: S3 bucket location of the DPR payload file template.
        s3_output_data: S3 bucket location of the output processed products.
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
        auxip_cql2 = read_tasktable.submit(flow_env.serialize(), processor_enum, payload_values, cadip_items)

        # Search Auxip products
        auxip_items = auxip_flow.search_task.submit(flow_env.serialize(), auxip_cql2, error_if_empty=True)

        # Stage Cadip and Auxip items.
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

        # Staged item ids
        item_ids = [item.id for items in [cadip_items.result(), auxip_items.result()] for item in items]

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
            processor_enum,
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
