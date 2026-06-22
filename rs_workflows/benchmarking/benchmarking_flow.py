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

import logging

from prefect import flow, get_run_logger, task

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@flow(name="benchmark-processor")
async def benchmark_processor(
    env: FlowEnvArgs,
    processor_name: str,
    processor_version: str,
    scenario_name: str,
) -> dict:
    """
    https://pforge-exchange2.astrium.eads.net/jira/browse/RSPY-1099

    Args:
        env (FlowEnvArgs): Prefect flow environment

    Returns:
    """
    return benchmark_processor_task.submit(env, processor_name, processor_version, scenario_name).result()


@task(name="Benchmark Processor")
async def benchmark_processor_task(
    env: FlowEnvArgs,
    processor_name: str,
    processor_version: str,
    scenario_name: str,
) -> dict:
    """Task called by the flow of the same name."""
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "benchmark-processor"):

        logger.info("Hello")
        return {"message": "hello"}
