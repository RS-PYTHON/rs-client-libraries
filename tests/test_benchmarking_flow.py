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

from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest

from rs_workflows.benchmarking import benchmarking_flow
from rs_workflows.flow_utils import FlowEnvArgs


@pytest.mark.asyncio
async def test_benchmark_processor_task(mocker):
    """Test the benchmark processor task delegates external operations and builds the DPR input."""

    # Arbitrary settings for test
    env = FlowEnvArgs(owner_id="test-owner", called_by="test-caller")
    settings = {
        "dask_cluster_label": "dask-l0",
        "pipeline": "s1_l0_full",
        "priority": "high",
        "input_products": [{"name": "input", "item_id": "item-1", "collection_name": "input-collection"}],
        "generated_product_to_collection_identifier": [
            {"name": "output", "product_type": "L1", "collection_name": "benchmark-output"},
        ],
        "auxiliary_product_to_collection_identifier": [
            {"product_type": "AUX", "collection_name": "benchmark-aux"},
        ],
        "processing_mode": ["always"],
        "external_variables": {
            "start_datetime": "2024-01-01T00:00:00Z",
            "end_datetime": "2024-01-02T00:00:00Z",
            "satellite": "sentinel-1a",
        },
    }
    variable_value = {
        "workflow_type": "benchmarking",
        "scenarios": {"scenario_name": {"processor_versions": {"processor_version": settings}}},
    }
    variable_get_mock = mocker.patch.object(
        benchmarking_flow.Variable,
        "get",
        new_callable=AsyncMock,
        return_value=variable_value,
    )

    # Mock all external calls from the flow
    flow_env_mock = mocker.Mock()
    flow_env_mock.start_span.return_value = nullcontext()
    catalog_client_mock = flow_env_mock.rs_client.get_catalog_client.return_value
    catalog_client_mock.get_item.return_value = mocker.sentinel.catalog_item
    flow_env_patch = mocker.patch.object(benchmarking_flow, "FlowEnv", return_value=flow_env_mock)
    mocker.patch.object(benchmarking_flow, "get_logger", return_value=mocker.Mock())

    artifact_mock = mocker.patch.object(benchmarking_flow, "acreate_markdown_artifact", new_callable=AsyncMock)
    share_bucket = mocker.Mock(bucket_name="shared-bucket")
    get_share_bucket_mock = mocker.patch.object(
        benchmarking_flow,
        "get_share_bucket",
        new_callable=AsyncMock,
        return_value=(share_bucket, None),
    )
    s3_delete_mock = mocker.patch.object(benchmarking_flow, "s3_delete")
    generate_payload_path_mock = mocker.patch.object(
        benchmarking_flow,
        "generate_payload_path",
        new_callable=AsyncMock,
        return_value="s3://payload-bucket/test-owner",
    )
    dpr_result = mocker.Mock()
    dpr_result.is_failed.return_value = False
    dpr_processing_mock = mocker.patch.object(
        benchmarking_flow,
        "dpr_processing_task",
        new_callable=AsyncMock,
        return_value=dpr_result,
    )

    # Run flow
    await benchmarking_flow.benchmark_processor_task.fn(
        env,
        "s1_l0",
        "processor_version",
        "scenario_name",
    )

    # Check external calls
    flow_env_patch.assert_called_once_with(env)
    variable_get_mock.assert_awaited_once_with("benchmarking-s1_l0-settings")
    catalog_client_mock.get_item.assert_called_once_with(
        collection_id="input-collection",
        item_id="item-1",
        owner_id="test-owner",
    )
    get_share_bucket_mock.assert_awaited_once_with()
    s3_delete_mock.assert_has_calls(
        [
            mocker.call("s3://shared-bucket/test-owner/benchmark-aux", log=True),
            mocker.call("s3://shared-bucket/test-owner/benchmark-output", log=True),
        ],
    )
    generate_payload_path_mock.assert_awaited_once_with("test-owner")

    dpr_processing_mock.assert_awaited_once()
    dpr_process_in = dpr_processing_mock.await_args.args[0]

    # Check input for DPR processing task is correct
    assert dpr_processing_mock.await_args.kwargs == {"return_state": True}
    assert dpr_process_in.env == env
    assert dpr_process_in.processor_name == "s1_l0"
    assert dpr_process_in.processor_version == "processor_version"
    assert dpr_process_in.dask_cluster_label == "dask-l0"
    assert dpr_process_in.s3_payload_file == "s3://payload-bucket/test-owner/payload_s1_l0.yaml"
    assert dpr_process_in.pipeline == "s1_l0_full"
    assert dpr_process_in.priority == "high"
    assert dpr_process_in.workflow_type == "benchmarking"

    # Test generated artifact contains the expected information
    assert artifact_mock.await_count == 2
    assert artifact_mock.await_args_list[0].kwargs["key"] == "benchmarking-result"
    assert "scenario_name" in artifact_mock.await_args_list[0].kwargs["markdown"]
    assert "**Status:** success" in artifact_mock.await_args_list[1].kwargs["markdown"]
