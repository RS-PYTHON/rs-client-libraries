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
import logging
import os

from prefect import flow, get_run_logger, task
from prefect.artifacts import acreate_markdown_artifact
from prefect.runtime import flow_run
from prefect.variables import Variable

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


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
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

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

        # Sleep a few seconds to simulate a processor runtime
        await asyncio.sleep(10)
