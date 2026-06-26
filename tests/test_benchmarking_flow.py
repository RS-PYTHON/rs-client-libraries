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

"""Unit tests for benchmarking_flow.py"""

from unittest.mock import AsyncMock

from rs_workflows.benchmarking.benchmarking_flow import benchmark_processor
from rs_workflows.flow_utils import FlowEnv


async def test_benchmark_processor(mocker, flow_env: FlowEnv):
    """Test the benchmark processor flow"""

    mocked_value = {"scenarios": {"scenario_name": {"processor_versions": {"processor_version": {"mocked": "value"}}}}}
    mocker.patch("prefect.variables.Variable.get", AsyncMock(return_value=mocked_value))
    mocker.patch("asyncio.sleep", AsyncMock())
    await benchmark_processor(flow_env.serialize(), "processor_name", "processor_version", "scenario_name")
