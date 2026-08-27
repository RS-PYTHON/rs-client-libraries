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

from datetime import datetime, timezone
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
    unpublished=(False, None),
    published=(True, datetime.now(timezone.utc)),
):
    mocker.patch.object(l0, "get_run_logger", return_value=MagicMock())
    flow_env = MagicMock()
    flow_env.start_span.return_value.__enter__.return_value = MagicMock()
    mocker.patch.object(l0, "FlowEnv", return_value=flow_env)
    mocker.patch.object(l0, "is_dask_cluster_running", new=AsyncMock(return_value=dask_running))
    mocker.patch.object(l0, "get_single_catalog_item", new=AsyncMock(return_value=item))
    mocker.patch.object(l0, "is_unpublished", return_value=unpublished)
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


async def test_process_l0_raises_when_session_unpublished(mocker):
    """A found but unpublished session raises ValueError."""
    _patch_l0(mocker, item=MagicMock(), unpublished=(True, "2020-01-01"))
    with pytest.raises(ValueError, match="unpublished"):
        await l0.process_l0.fn("S1A_20230101T000000", _flow_params())


async def test_process_l0_raises_when_session_not_published(mocker):
    """A found session that is not published yet raises ValueError."""
    _patch_l0(mocker, item=MagicMock(), unpublished=(False, None), published=(False, None))
    with pytest.raises(ValueError, match="not been published"):
        await l0.process_l0.fn("S1A_20230101T000000", _flow_params())


async def test_process_l0_dispatches_s1_when_found_in_catalog(mocker):
    """A published S1 session in the catalog is dispatched to the S1 processor."""
    mocks = _patch_l0(mocker, item=MagicMock(), published=(True, datetime.now(timezone.utc)))
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
    mocks = _patch_l0(mocker, item=MagicMock(), published=(True, datetime.now(timezone.utc)))
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
    return mocker.patch.object(l0_last_steps, "call_dpr_flow", new=AsyncMock())


async def test_process_l0_last_steps_returns_early_without_item(mocker):
    """When the session is not in the catalog, no DPR flow is triggered."""
    dpr_mock = _patch_last_steps(mocker, item=None)
    await l0_last_steps.process_l0_last_steps("1", "S1A_x", _flow_params(), [], verbose=False)
    dpr_mock.assert_not_awaited()


async def test_process_l0_last_steps_raises_on_missing_published(mocker):
    """A missing/invalid 'published' property raises ValueError."""
    item = MagicMock()
    item.properties = {}
    _patch_last_steps(mocker, item=item)
    with pytest.raises(ValueError, match="published"):
        await l0_last_steps.process_l0_last_steps("1", "S1A_x", _flow_params(), [], verbose=False)


async def test_process_l0_last_steps_calls_dpr_flow(mocker):
    """A published session triggers call_dpr_flow with the computed satellite and datetimes."""
    item = MagicMock()
    item.properties = {"published": "2023-01-01T00:00:00"}
    dpr_mock = _patch_last_steps(mocker, item=item)
    products = [FlowInputProduct(name="S1CADUS", item_id="S1A_session", collection_name="coll")]

    await l0_last_steps.process_l0_last_steps("1", "S1A_session", _flow_params(), products, verbose=True)

    dpr_mock.assert_awaited_once()
    kwargs = dpr_mock.call_args.kwargs
    assert kwargs["input_products"] == products
    assert kwargs["external_variables"]["satellite"] == "sentinel-1a"
    assert kwargs["external_variables"]["start_datetime"] == datetime.fromisoformat("2023-01-01T00:00:00")
    assert kwargs["external_variables"]["end_datetime"] == datetime.fromisoformat("2023-01-01T00:00:00")


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
    last_steps = mocker.patch.object(s3_l0, "process_l0_last_steps", new=AsyncMock())
    flow_params = MagicMock(session_collection="s03-cadip-session")

    await s3_l0.process_s3l0.fn("S3A_session", flow_params, verbose=False)

    last_steps.assert_awaited_once()
    kwargs = last_steps.call_args.kwargs
    assert kwargs["mission"] == "3"
    products = kwargs["input_products"]
    assert products[0].name == "S3ACADUS"
    assert products[0].collection_name == "s03-cadip-session"


async def test_process_s1l0_task_delegates_to_flow(mocker):
    """The S1 task wrapper simply runs the underlying flow function."""
    delegate = mocker.patch.object(s1_l0.process_s1l0, "fn", new=AsyncMock())
    await s1_l0.process_s1l0_task.fn(session="S1A_session", flow_params=MagicMock(), verbose=False)
    delegate.assert_awaited_once()


async def test_process_s3l0_task_delegates_to_flow(mocker):
    """The S3 task wrapper simply runs the underlying flow function."""
    delegate = mocker.patch.object(s3_l0.process_s3l0, "fn", new=AsyncMock())
    await s3_l0.process_s3l0_task.fn(session="S3A_session", flow_params=MagicMock(), verbose=False)
    delegate.assert_awaited_once()
