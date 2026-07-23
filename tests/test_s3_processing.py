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

from rs_workflows.on_demand.sentinel3 import (
    s3_l1_olci,
    s3_processing,
    s3_processing_utils,
)


def _flow_run(result):
    state = MagicMock()
    state.is_completed.return_value = True
    state.result = AsyncMock(return_value=result)
    return MagicMock(state=state)


async def test_build_olci_l1_inputs_from_catalog(mocker):
    """The dormant catalog alternative builds the processor's four expected inputs."""
    mocker.patch.object(s3_l1_olci, "get_run_logger", return_value=MagicMock())
    catalog_client = MagicMock()
    catalog_client.search.side_effect = [
        MagicMock(
            items=[
                MagicMock(id="S03OLCL0__product-3.zarr"),
                MagicMock(id="S03OLCL0__product-1.zarr"),
                MagicMock(id="S03OLCL0__product-4.zarr"),
                MagicMock(id="S03OLCL0__product-2.zarr"),
            ],
        ),
        MagicMock(items=[MagicMock(id="S03NATL0__product.zarr")]),
    ]
    flow_env = MagicMock()
    flow_env.rs_client.get_catalog_client.return_value = catalog_client
    mocker.patch.object(s3_l1_olci, "FlowEnv", return_value=flow_env)

    result = await s3_l1_olci.build_olci_l1_inputs_from_catalog(
        owner_identifier="opadeanu",
        input_collection="AUTOMATED_S3L0_OUTPUT_2026",
        timestamp="2026-06-30T11:40:00Z/2026-06-30T12:30:00Z",
    )

    assert [product.model_dump() for product in result] == [
        {
            "name": "S3OLCIL0_1",
            "item_id": "S03OLCL0__product-1.zarr",
            "collection_name": "AUTOMATED_S3L0_OUTPUT_2026",
        },
        {
            "name": "S3OLCIL0_2",
            "item_id": "S03OLCL0__product-2.zarr",
            "collection_name": "AUTOMATED_S3L0_OUTPUT_2026",
        },
        {
            "name": "S3OLCIL0_3",
            "item_id": "S03OLCL0__product-3.zarr",
            "collection_name": "AUTOMATED_S3L0_OUTPUT_2026",
        },
        {
            "name": "S3NAVL0_1",
            "item_id": "S03NATL0__product.zarr",
            "collection_name": "AUTOMATED_S3L0_OUTPUT_2026",
        },
    ]


async def test_process_s3_runs_deployments_in_sequence(mocker):
    """Staging, L0, and L1 run sequentially with L0 products passed to L1."""
    mocker.patch.object(s3_processing, "get_run_logger", return_value=MagicMock())
    mocker.patch.object(
        s3_processing_utils.Variable,
        "get",
        new=AsyncMock(
            return_value={
                "orchestration": {
                    "cadip_staging_deployment": "stage-cadip-with-options/On-demand Cadip staging",
                    "s3_l0_deployment": "process-s3-l0/on_demand_S3L0",
                    "s3_l1_olci_deployment": "process-s3-l1-olci/on_demand_S3L1OLCI",
                    "cadip_collection": "cadip",
                    "staging_catalog_collection": "AUTOMATED_S3L0_INPUT",
                    "s3_l0_output_collection": "AUTOMATED_S3L0_OUTPUT",
                },
            },
        ),
    )
    l1_products = [{"id": "S03OLCL1__product.zarr"}]
    l0_products = [
        {
            "id": f"S03OLCL0__product-{index}.zarr",
            "properties": {"product:type": "S03OLCL0_"},
        }
        for index in range(1, 5)
    ]
    l0_products.append(
        {
            "id": "S03NATL0__product.zarr",
            "properties": {"product:type": "S03NATL0_"},
        },
    )
    run = mocker.patch.object(
        s3_processing,
        "run_deployment",
        new=AsyncMock(
            side_effect=[
                _flow_run(None),
                _flow_run(l0_products),
                _flow_run(l1_products),
            ],
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
                            "name": "S3OLCIL0_1",
                            "item_id": "S03OLCL0__product-1.zarr",
                            "collection_name": "AUTOMATED_S3L0_OUTPUT",
                        },
                        {
                            "name": "S3OLCIL0_2",
                            "item_id": "S03OLCL0__product-2.zarr",
                            "collection_name": "AUTOMATED_S3L0_OUTPUT",
                        },
                        {
                            "name": "S3OLCIL0_3",
                            "item_id": "S03OLCL0__product-3.zarr",
                            "collection_name": "AUTOMATED_S3L0_OUTPUT",
                        },
                        {
                            "name": "S3NAVL0_1",
                            "item_id": "S03NATL0__product.zarr",
                            "collection_name": "AUTOMATED_S3L0_OUTPUT",
                        },
                    ],
                },
            },
            flow_run_name="s3-l1-olci-S3A_session",
        ),
    ]
