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

"""Tests for the Sentinel-3 deployment orchestrator."""

from unittest.mock import AsyncMock, MagicMock, call

from rs_workflows.on_demand.sentinel3 import s3_processing


def _flow_run(result):
    state = MagicMock()
    state.is_completed.return_value = True
    state.result = AsyncMock(return_value=result)
    return MagicMock(state=state)


async def test_process_s3_runs_deployments_in_sequence(mocker):
    """Staging, L0, and L1 run sequentially with L0 products passed to L1."""
    mocker.patch.object(s3_processing, "get_run_logger", return_value=MagicMock())
    l1_products = [{"id": "S03OLCL1__product.zarr"}]
    mocker.patch.object(
        s3_processing.Variable,
        "get",
        new=AsyncMock(
            return_value={
                "s3_l0_finished": [{"S03OLCL0_": "S03OLCL0__product.zarr"}],
            }
        ),
    )
    run = mocker.patch.object(
        s3_processing,
        "run_deployment",
        new=AsyncMock(
            side_effect=[
                _flow_run(None),
                _flow_run(None),
                _flow_run(l1_products),
            ]
        ),
    )

    result = await s3_processing.process_s3.fn("S3A_session")

    assert result is None
    assert run.await_args_list == [
        call(
            name="stage-cadip-with-options/On-demand Cadip staging",
            parameters={
                "env": {"owner_id": "opadeanu"},
                "cadip_collection_identifier": "cadip",
                "session_identifier": "S3A_session",
                "catalog_collection_identifier": "AUTOMATED_S3L0_INPUT",
            },
            flow_run_name="stage-S3A_session",
        ),
        call(
            name="process-s3-l0/on_demand_S3L0",
            parameters={"session": "S3A_session"},
            flow_run_name="s3-l0-S3A_session",
        ),
        call(
            name="process-s3-l1-olci/on_demand_S3L1OLCI",
            parameters={
                "flow_params": {
                    "input_products": [
                        {
                            "name": "S03OLCL0_",
                            "item_id": "S03OLCL0__product.zarr",
                            "collection_name": "AUTOMATED_S3L0_OUTPUT",
                        }
                    ]
                }
            },
            flow_run_name="s3-l1-olci-S3A_session",
        ),
    ]
