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

"""Tests for Sentinel-3 processing behavior."""

from unittest.mock import AsyncMock, MagicMock

from rs_workflows.on_demand.sentinel3 import s3_l1_olci


async def test_process_s3l1_olci_builds_inputs_from_raw_l0_products(mocker):
    """Raw products from the L0 event are prepared inside the L1 flow."""
    resolved_params = MagicMock()
    resolved_params.owner_identifier = "toto"
    resolved_params.input_products = []
    flow_params = MagicMock()
    flow_params.resolve = AsyncMock(return_value=resolved_params)
    call_dpr_flow = mocker.patch.object(
        s3_l1_olci,
        "call_dpr_flow",
        new=AsyncMock(return_value=[]),
    )
    mocker.patch.object(s3_l1_olci, "get_run_logger", return_value=MagicMock())
    mocker.patch.object(
        s3_l1_olci,
        "read_s3_orchestration_settings",
        new=AsyncMock(
            return_value=MagicMock(
                s3_l0_output_collection="AUTOMATED_S3L0_OUTPUT_2026",
            ),
        ),
    )
    l0_products = [
        {
            "S03OLCL0_": f"S03OLCL0__product-{index}.zarr",
        }
        for index in range(1, 4)
    ]
    l0_products.append(
        {
            "S03NATL0_": "S03NATL0__product.zarr",
        },
    )

    await s3_l1_olci.process_s3l1_olci.fn(
        flow_params=flow_params,
        l0_products=l0_products,
    )

    actual_inputs = call_dpr_flow.call_args.kwargs["input_products"]
    assert [product.model_dump() for product in actual_inputs] == [
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
