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

"""Unit tests for the on_demand Level-0 flows (common.l0, common.l0_last_steps, sentinel1/3)."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from rs_workflows.flow_utils import FlowInputProduct
from rs_workflows.on_demand.common import l0, l0_last_steps
from rs_workflows.on_demand.sentinel1 import s1_l0
from rs_workflows.on_demand.sentinel3 import s3_l0


def _resolved_params():
    """A resolved Level0FlowParams-like object with the attributes used by the flows."""
    return MagicMock(
        owner_identifier="owner",
        dask_cluster_label="cluster",
        session_collection="s01-cadip-session",
        cadip_collections=["s1_sgs"],
        processor_name="proc",
        processor_version="1.0",
        pipeline=None,
        unit=None,
        priority=None,
        processing_mode=[],
        workflow=None,
        generated_product_to_collection_identifier=None,
        auxiliary_product_to_collection_identifier=None,
        logging_level="INFO",
        start_datetime=None,
        end_datetime=None,
    )


def _flow_params():
    """A flow_params object whose resolve() returns the resolved params."""
    params = MagicMock()
    params.resolve = AsyncMock(return_value=_resolved_params())
    return params


# --------------------------------------------------------------------------- #
# process_l0
# --------------------------------------------------------------------------- #
def _patch_l0(
    mocker,
    *,
    dask_running=True,
    item=None,
    station=None,
    staged=True,
    evicted=(False, None),
    published=True,
):
    mocker.patch.object(l0, "get_run_logger", return_value=MagicMock())
    flow_env = MagicMock()
    flow_env.start_span.return_value.__enter__.return_value = MagicMock()
    mocker.patch.object(l0, "FlowEnv", return_value=flow_env)
    mocker.patch.object(l0, "is_dask_cluster_running", new=AsyncMock(return_value=dask_running))
    mocker.patch.object(l0, "get_single_catalog_item", new=AsyncMock(return_value=item))
    mocker.patch.object(l0, "is_evicted", return_value=evicted)
    mocker.patch.object(l0, "is_published", return_value=published)
    mocker.patch.object(l0, "get_cadip_station", new=AsyncMock(return_value=station))
    stage_mock = mocker.patch.object(l0, "stage_session_common", new=AsyncMock(return_value=staged))
    s1_mock = mocker.patch.object(l0, "process_s1l0_task", new=AsyncMock())
    s3_mock = mocker.patch.object(l0, "process_s3l0_task", new=AsyncMock())
    return {"stage": stage_mock, "s1": s1_mock, "s3": s3_mock}


async def test_process_l0_rejects_bad_session_name(mocker):
    """An invalid session name raises ValueError before any processing."""
    mocker.patch.object(l0, "get_run_logger", return_value=MagicMock())
    with pytest.raises(ValueError, match="Invalid session name"):
        await l0.process_l0.fn("INVALID", _flow_params())


async def test_process_l0_raises_when_dask_cluster_not_ready(mocker):
    """An unknown/not-ready dask cluster raises ValueError."""
    _patch_l0(mocker, dask_running=False)
    with pytest.raises(ValueError, match="unknown or not ready"):
        await l0.process_l0.fn("S1A_20230101T000000", _flow_params())


async def test_process_l0_raises_when_session_evicted(mocker):
    """A found but evicted session raises ValueError."""
    _patch_l0(mocker, item=MagicMock(), evicted=(True, "2020-01-01"))
    with pytest.raises(ValueError, match="evicted"):
        await l0.process_l0.fn("S1A_20230101T000000", _flow_params())


async def test_process_l0_raises_when_session_not_published(mocker):
    """A found session that is not published yet raises ValueError."""
    _patch_l0(mocker, item=MagicMock(), evicted=(False, None), published=False)
    with pytest.raises(ValueError, match="not been publised"):
        await l0.process_l0.fn("S1A_20230101T000000", _flow_params())


async def test_process_l0_dispatches_s1_when_found_in_catalog(mocker):
    """A published S1 session in the catalog is dispatched to the S1 processor."""
    mocks = _patch_l0(mocker, item=MagicMock(), published=True)
    await l0.process_l0.fn("S1A_20230101T000000", _flow_params())
    mocks["s1"].assert_awaited_once()
    mocks["s3"].assert_not_awaited()


async def test_process_l0_stages_then_dispatches_s3(mocker):
    """An S3 session absent from the catalog is staged then dispatched to the S3 processor."""
    mocks = _patch_l0(mocker, item=None, station="s3_sgs", staged=True)
    await l0.process_l0.fn("S3A_20230101T000000", _flow_params())
    mocks["stage"].assert_awaited_once()
    mocks["s3"].assert_awaited_once()
    mocks["s1"].assert_not_awaited()


async def test_process_l0_no_dispatch_when_station_not_found(mocker):
    """When the session is neither cataloged nor stageable, no processor runs."""
    mocks = _patch_l0(mocker, item=None, station=None)
    await l0.process_l0.fn("S1A_20230101T000000", _flow_params())
    mocks["stage"].assert_not_awaited()
    mocks["s1"].assert_not_awaited()
    mocks["s3"].assert_not_awaited()


async def test_process_l0_uses_default_params_when_none(mocker):
    """When no flow_params is given, a default Level0FlowParams is built and resolved."""
    mocks = _patch_l0(mocker, item=MagicMock(), published=True)
    mocker.patch.object(l0, "Level0FlowParams", return_value=_flow_params())
    await l0.process_l0.fn("S1A_20230101T000000", None)
    mocks["s1"].assert_awaited_once()


# --------------------------------------------------------------------------- #
# process_l0_last_steps
# --------------------------------------------------------------------------- #
def _patch_last_steps(mocker, item):
    mocker.patch.object(l0_last_steps, "get_run_logger", return_value=MagicMock())
    flow_env = MagicMock()
    flow_env.start_span.return_value.__enter__.return_value = MagicMock()
    mocker.patch.object(l0_last_steps, "FlowEnv", return_value=flow_env)
    mocker.patch.object(l0_last_steps, "get_single_catalog_item", new=AsyncMock(return_value=item))
    return mocker.patch.object(l0_last_steps, "call_dpr_flow", new=AsyncMock(return_value=[]))


async def test_process_l0_last_steps_returns_early_without_item(mocker):
    """When the session is not in the catalog, no DPR flow is triggered."""
    dpr_mock = _patch_last_steps(mocker, item=None)
    await l0_last_steps.process_l0_last_steps("1", "S1A_x", _flow_params(), [], verbose=False)
    dpr_mock.assert_not_awaited()


async def test_process_l0_last_steps_does_not_require_published(mocker):
    """The datetime interval comes from flow parameters, not catalog metadata."""
    item = MagicMock()
    item.properties = {}
    dpr_mock = _patch_last_steps(mocker, item=item)

    await l0_last_steps.process_l0_last_steps("1", "S1A_x", _flow_params(), [], verbose=False)

    assert dpr_mock.call_args.kwargs["external_variables"]["start_datetime"] is None
    assert dpr_mock.call_args.kwargs["external_variables"]["end_datetime"] is None


async def test_process_l0_last_steps_calls_dpr_flow(mocker):
    """A published session triggers call_dpr_flow with the computed satellite and datetimes."""
    item = MagicMock()
    item.properties = {"published": "2023-01-01T00:00:00"}
    dpr_mock = _patch_last_steps(mocker, item=item)
    products = [FlowInputProduct(name="S1CADUS", item_id="S1A_session", collection_name="coll")]

    flow_params = _flow_params()
    flow_params.resolve.return_value.start_datetime = datetime.fromisoformat("2025-06-11T00:00:00+00:00")
    flow_params.resolve.return_value.end_datetime = datetime.fromisoformat("2025-06-13T00:00:00+00:00")

    await l0_last_steps.process_l0_last_steps("1", "S1A_session", flow_params, products, verbose=True)

    dpr_mock.assert_awaited_once()
    kwargs = dpr_mock.call_args.kwargs
    assert kwargs["input_products"] == products
    assert kwargs["external_variables"]["satellite"] == "sentinel-1a"
    assert kwargs["external_variables"]["start_datetime"] == datetime.fromisoformat("2025-06-11T00:00:00+00:00")
    assert kwargs["external_variables"]["end_datetime"] == datetime.fromisoformat("2025-06-13T00:00:00+00:00")


async def test_process_s3_l0_last_steps_returns_all_products(mocker):
    """Successful S3 L0 processing returns every product for Prefect persistence."""
    item = MagicMock()
    item.properties = {}
    dpr_mock = _patch_last_steps(mocker, item=item)
    dpr_mock.return_value = [
        {
            "type": "Feature",
            "id": "S03OLCL0__product.zarr",
            "properties": {"product:type": "S03OLCL0_"},
        },
        {
            "type": "Feature",
            "id": "S03DORDOP_product.zarr",
            "properties": {"product:type": "S03DORDOP"},
        },
        {
            "type": "Feature",
            "id": "S03NATL0__product.zarr",
            "properties": {"product:type": "S03NATL0_"},
        },
    ]
    result = await l0_last_steps.process_l0_last_steps("3", "S3A_session", _flow_params(), [], verbose=False)

    assert result == dpr_mock.return_value


# --------------------------------------------------------------------------- #
# sentinel1 / sentinel3 entry points
# --------------------------------------------------------------------------- #
async def test_process_s1l0_builds_s1_input_and_delegates(mocker):
    """process_s1l0 builds the S1CADUS input product and delegates to the common last steps."""
    last_steps = mocker.patch.object(s1_l0, "process_l0_last_steps", new=AsyncMock())
    flow_params = MagicMock(session_collection="s01-cadip-session")

    await s1_l0.process_s1l0.fn("S1A_session", flow_params, verbose=False)

    last_steps.assert_awaited_once()
    kwargs = last_steps.call_args.kwargs
    assert kwargs["mission"] == "1"
    assert kwargs["session"] == "S1A_session"
    products = kwargs["input_products"]
    assert len(products) == 1
    assert products[0].name == "S1CADUS"
    assert products[0].item_id == "S1A_session"
    assert products[0].collection_name == "s01-cadip-session"


async def test_process_s3l0_builds_s3_input_and_delegates(mocker):
    """process_s3l0 builds the S3ACADUS input product and delegates to the common last steps."""
    products_result = [
        {
            "id": f"S03OLCL0__product-{index}.zarr",
            "properties": {"product:type": "S03OLCL0_"},
        }
        for index in range(1, 4)
    ]
    products_result.append(
        {
            "id": "S03NATL0__product.zarr",
            "properties": {"product:type": "S03NATL0_"},
        },
    )
    last_steps = mocker.patch.object(
        s3_l0,
        "process_l0_last_steps",
        new=AsyncMock(return_value=products_result),
    )
    emitted_event = MagicMock(id="event-id")
    emit_event = mocker.patch.object(s3_l0, "emit_event", return_value=emitted_event)
    mocker.patch.object(s3_l0, "get_run_logger", return_value=MagicMock())
    mocker.patch.object(s3_l0.runtime.flow_run, "id", "flow-run-id")
    orchestration_settings = MagicMock(s3_l0_output_collection="AUTOMATED_S3L0_OUTPUT_2026")
    mocker.patch.object(
        s3_l0,
        "read_s3_orchestration_settings",
        new=AsyncMock(return_value=orchestration_settings),
    )
    resolved_params = _resolved_params()
    resolved_params.session_collection = "s03-cadip-session"
    flow_params = MagicMock()
    flow_params.resolve = AsyncMock(return_value=resolved_params)

    result = await s3_l0.process_s3l0.fn("S3A_session", flow_params, verbose=False)

    assert result == products_result
    flow_params.resolve.assert_awaited_once_with("3")
    last_steps.assert_awaited_once()
    kwargs = last_steps.call_args.kwargs
    assert kwargs["mission"] == "3"
    assert kwargs["flow_params"] is resolved_params
    products = kwargs["input_products"]
    assert products[0].name == "S3ACADUS"
    assert products[0].collection_name == "s03-cadip-session"
    emit_event.assert_called_once_with(
        event="rs-python.s3-l0.products-ready",
        resource={
            "prefect.resource.id": "rs-python.s3-l0-result.flow-run-id",
            "prefect.resource.name": "S3A_session",
            "rs-python.session-id": "S3A_session",
        },
        related=[
            {
                "prefect.resource.id": "prefect.flow-run.flow-run-id",
                "prefect.resource.role": "flow-run",
            },
        ],
        payload={
            "flow_run_id": "flow-run-id",
            "session_id": "S3A_session",
            "owner_identifier": resolved_params.owner_identifier,
            "products": products_result,
            "input_products": [
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
            ],
        },
    )


async def test_process_s3l0_task_delegates_to_flow(mocker):
    """The S3 task wrapper runs the underlying flow function."""
    process = mocker.patch.object(s3_l0.process_s3l0, "fn", new=AsyncMock())
    await s3_l0.process_s3l0_task.fn("S3A_session")
    process.assert_awaited_once_with("S3A_session")


async def test_process_s1l0_task_delegates_to_flow(mocker):
    """The S1 task wrapper simply runs the underlying flow function."""
    delegate = mocker.patch.object(s1_l0.process_s1l0, "fn", new=AsyncMock())
    await s1_l0.process_s1l0_task.fn(session="S1A_session", flow_params=MagicMock(), verbose=False)
    delegate.assert_awaited_once()
