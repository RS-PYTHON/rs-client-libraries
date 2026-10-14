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

"""Benchmarking flows."""

import asyncio
import json
import os
from os import path as osp

from prefect import flow, task
from prefect.artifacts import acreate_markdown_artifact
from prefect.runtime import flow_run
from prefect.variables import Variable

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_common.prefect_utils import get_share_bucket, s3_delete
from rs_workflows.flow_utils import DprProcessIn, FlowEnv, FlowEnvArgs
from rs_workflows.utils.dpr import dpr_processing_task
from rs_workflows.utils.prefect import get_logger


@flow(name="benchmark-processor")
async def benchmark_processor(
    env: FlowEnvArgs,
    processor_name: str,
    processor_version: str,
    scenario_name: str,
):
    """
    https://pforge-exchange2.astrium.eads.net/jira/browse/RSPY-1099
    This flow reads one Prefect variable named "benchmarking-<processor_name>-settings" associated to the input
    parameter "processor name".
    This variable provides a mocked structured information to define the benchmarking context.

    This flow will:
      - Execute a mock task using the provided input parameters: processor name, version, and scenario.
      - Generate a fake Markdown artifact named "benchmarking-result".

    This artifact contains:
      - The input parameters passed to the benchmark-processor flow.
      - The settings fetched from the Prefect variable.
    """
    # Call the task of the same name
    return benchmark_processor_task.submit(env, processor_name, processor_version, scenario_name).result()


@task(name="Benchmark Processor")
async def benchmark_processor_task(
    env: FlowEnvArgs,
    processor_name: str,
    processor_version: str,
    scenario_name: str,
):
    """Task called by the flow of the same name."""
    logger = get_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "benchmark-processor"):

        # Read the prefect variable value
        all_value = await Variable.get(f"benchmarking-{processor_name}-settings")

        # Read contents for the given scenario and processor version
        this_value = all_value["scenarios"][scenario_name]["processor_versions"][processor_version]

        # Markdown report
        parameters = {
            "owner_id": env.owner_id,
            "processor_name": processor_name,
            "processor_version": processor_version,
            "scenario_name": scenario_name,
        }
        report = f"""
# Benchmarking processor report

### Prefect flow run
{f"{os.environ['RSPY_PREFECT_URL']}/runs/flow-run/{flow_run.id}"}

### Called by
{env.called_by}

### Parameters
```json
{json.dumps(parameters, indent=2)}
```

### Settings
```json
{json.dumps(this_value, indent=2)}
```
"""
        # Save as a markdown artifact
        artifact_key_name: str = "benchmarking-result"
        await acreate_markdown_artifact(
            key=artifact_key_name,
            markdown=report,
            description="Benchmarking processor report",
        )
        logger.info(f"📌 Artifact named '{artifact_key_name}' has been linked to this flow.")

        # TODO Verify input data given: staged and available in catalog
        #   => if failed, stop flow

        # Retrieve list of collection names to empty before processing
        collections_to_empty = []

        for aux_collection in this_value.get("auxiliary_product_to_collection_identifier", []):
            collections_to_empty.append(aux_collection["collection_name"])
        for generated_collection in this_value.get("generated_product_to_collection_identifier", []):
            collections_to_empty.append(generated_collection["collection_name"])

        # Make sure all collection names start with "benchmark", if it's not the case throw an error and stop the process
        for collection_name in collections_to_empty:
            if not collection_name.startswith("benchmark"):
                raise ValueError(
                    f"Cannot empty collection '{collection_name}' because it does not start with 'benchmark'.\nFor safety reasons, all collections used in benchmarking must start with 'benchmark'.",
                )

        # Get the prefect share bucket
        share_bucket, _ = await get_share_bucket()
        # Build s3_path for each collection and delete its content
        for collection_name in collections_to_empty:
            # TODO take owner_id from env or from this_value?
            collection_s3_path = osp.join("s3://", share_bucket, env.owner_id, collection_name)
            logger.info(f"Deleting content of collection '{collection_name}' at '{collection_s3_path}'")
            s3_delete(collection_s3_path, log=True)

        # Build parameters to run a generic DPR process
        benchmark_dpr_process_in: DprProcessIn = DprProcessIn(
            env=env,
            processor_name=DprProcessor(processor_name),
            processor_version=processor_version,
            dask_cluster_label=this_value["dask_cluster_label"],
            s3_payload_file="",  # f"{s3_payload}/payload_{processor_name}.yaml",  TODO payload ?
            pipeline=this_value["pipeline"],
            priority=this_value["priority"],
            workflow_type=all_value["workflow_type"],
            input_products=this_value["input_products"],
            generated_product_to_collection_identifier=this_value["generated_product_to_collection_identifier"],
            auxiliary_product_to_collection_identifier=this_value["auxiliary_product_to_collection_identifier"],
            processing_mode=this_value["processing_mode"],
        )

        return await dpr_processing_task(benchmark_dpr_process_in)
