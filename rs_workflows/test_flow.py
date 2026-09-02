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

from prefect import flow, get_run_logger, task

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@flow(name="test-flow")
async def testflow(env: FlowEnvArgs, some_param: str):
    flow_env = FlowEnv(env)
    logger = get_run_logger()
    logger.info("Running main test flow")
    logger.debug("Parameter is %s", some_param)
    call_client_task.submit(flow_env.serialize(), some_param)


@flow(name="client-call")
async def call_client(env: FlowEnvArgs, some_param: str):
    flow_env = FlowEnv(env)
    logger = get_run_logger()

    # Function that emits logs
    logger.info("Running client call flow")
    logger.debug("Performing action of param %s", some_param)
    flow_env.rs_client.log_and_raise("This is a test error message", RuntimeError("Original test error message"))


@task(name="client-call")
async def call_client_task(*args, **kwargs):
    return await call_client.fn(*args, **kwargs)
