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

import pytest

from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct
from rs_workflows.on_demand.common.types import Level2FlowParams
from rs_workflows.on_demand.sentinel3 import s3_l1_olci, s3_l2_olci


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


@pytest.mark.parametrize("override_inputs", [False, True])
@pytest.mark.parametrize("override_params", [False, True])
async def test_process_s3l2_olci_resolves_settings_and_calls_dpr(mocker, override_inputs, override_params):
    """L2 uses mission-3 settings and gives explicit parameters and inputs precedence."""
    settings_input = FlowInputProduct(name="OLCI_L1", item_id="from-settings", collection_name="olci-l1")
    explicit_input = FlowInputProduct(name="OLCI_L1", item_id="explicit", collection_name="olci-l1")
    read_settings = mocker.patch(
        "rs_workflows.on_demand.common.types._read_prefect_settings",
        new=AsyncMock(
            return_value={
                "owner_identifier": "toto",
                "processor": {"name": "s3_l2olci", "version": "1.0"},
                "dask_cluster_name": "olci-cluster",
                "pipeline": "olci-l2-pipeline",
                "satellite": "S3A",
                "start_datetime": "2026-09-01T00:00:00Z",
                "end_datetime": "2026-09-01T01:00:00Z",
                "input_products": [settings_input.model_dump()],
            },
        ),
    )
    mocker.patch.object(s3_l2_olci, "get_run_logger", return_value=MagicMock())
    expected_result = [{"id": "olci-l2-output"}]
    call_dpr = mocker.patch.object(s3_l2_olci, "call_dpr_flow", new=AsyncMock(return_value=expected_result))
    flow_params = Level2FlowParams(processor_version="2.0") if override_params else None

    result = await s3_l2_olci.process_s3l2_olci.fn(
        flow_params=flow_params,
        input_products=[explicit_input] if override_inputs else None,
    )

    read_settings.assert_awaited_once_with("3", "2")
    call_dpr.assert_awaited_once()
    assert call_dpr.call_args.args == (FlowEnvArgs(owner_id="toto"),)
    kwargs = call_dpr.call_args.kwargs
    assert kwargs["input_products"] == [explicit_input if override_inputs else settings_input]
    assert kwargs["processor_name"] == "s3_l2olci"
    assert kwargs["processor_version"] == ("2.0" if override_params else "1.0")
    assert "logging_level" not in kwargs
    assert kwargs["dask_cluster_label"] == "olci-cluster"
    assert kwargs["pipeline"] == "olci-l2-pipeline"
    assert kwargs["external_variables"]["satellite"] == "S3A"
    assert kwargs["external_variables"]["start_datetime"].isoformat() == "2026-09-01T00:00:00+00:00"
    assert kwargs["external_variables"]["end_datetime"].isoformat() == "2026-09-01T01:00:00+00:00"
    assert kwargs["generated_product_to_collection_identifier"] == []
    assert kwargs["auxiliary_product_to_collection_identifier"] == []
    assert result == expected_result


async def test_process_s3l2_olci_stops_when_settings_resolution_fails(mocker):
    """A settings error is propagated without submitting DPR processing."""
    mocker.patch(
        "rs_workflows.on_demand.common.types._read_prefect_settings",
        new=AsyncMock(side_effect=ValueError("Invalid L2 settings")),
    )
    call_dpr = mocker.patch.object(s3_l2_olci, "call_dpr_flow", new=AsyncMock())

    with pytest.raises(ValueError, match="Invalid L2 settings"):
        await s3_l2_olci.process_s3l2_olci.fn()

    call_dpr.assert_not_awaited()


async def test_process_s3l2_olci_task_forwards_arguments_and_result(mocker):
    """The task wrapper forwards flow arguments and returns the produced items."""
    flow_params = Level2FlowParams()
    expected_result = [{"id": "olci-l2-output"}]
    flow_fn = mocker.patch.object(s3_l2_olci.process_s3l2_olci, "fn", new=AsyncMock(return_value=expected_result))

    result = await s3_l2_olci.process_s3l2_olci_task.fn(flow_params, input_products=None)

    flow_fn.assert_awaited_once_with(flow_params, input_products=None)
    assert result == expected_result


@pytest.mark.parametrize("efr_count", [0, 1, 2])
async def test_process_s3l1_emits_published_efr_inputs(mocker, efr_count):
    """Only published EFR outputs are passed to L2, with their actual collection."""
    products = [
        {
            "id": f"S03OLCEFR_product-{index}.zarr",
            "collection": f"OUTPUT_{index}",
            "properties": {"product:type": "S03OLCEFR"},
        }
        for index in range(efr_count)
    ]
    products.append({"id": "S03OLCERR_other.zarr", "properties": {"product:type": "S03OLCERR"}})
    flow_params = MagicMock()
    resolved_params = MagicMock(owner_identifier="toto")
    flow_params.resolve = AsyncMock(return_value=resolved_params)
    mocker.patch.object(s3_l1_olci, "call_dpr_flow", new=AsyncMock(return_value=products))
    mocker.patch.object(s3_l1_olci, "get_run_logger", return_value=MagicMock())
    mocker.patch.object(s3_l1_olci.runtime.flow_run, "id", "l1-run-id")
    emit_event = mocker.patch.object(s3_l1_olci, "emit_event")

    result = await s3_l1_olci.process_s3l1_olci.fn(flow_params=flow_params)

    assert result is products
    if not efr_count:
        emit_event.assert_not_called()
        return
    emit_event.assert_called_once()
    assert emit_event.call_args.kwargs["event"] == "rs-python.s3-l1.products-ready"
    assert emit_event.call_args.kwargs["payload"] == {
        "flow_run_id": "l1-run-id",
        "input_products": [
            {
                "name": "S3OLCIL1",
                "item_id": f"S03OLCEFR_product-{index}.zarr",
                "collection_name": f"OUTPUT_{index}",
            }
            for index in range(efr_count)
        ],
    }
