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

"""Unit tests for rs_workflows.on_demand.adf.convert_adf_set."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from rs_workflows.flow_utils import AuxiliaryProductMapping, FlowEnvArgs
from rs_workflows.on_demand.adf import config as adf_config
from rs_workflows.on_demand.adf import convert_adf_set as cas

AUX_MAPPING = [AuxiliaryProductMapping(product_type="AX", collection_name="AUX")]


def _logger(mocker):
    mocker.patch.object(cas, "get_run_logger", return_value=MagicMock())


def test_config_package_exposes_version():
    """The adf config package is importable and exposes a version."""
    assert isinstance(adf_config.__version__, str)


# --------------------------------------------------------------------------- #
# compute_cql2
# --------------------------------------------------------------------------- #
def test_compute_cql2_returns_structure_and_substitutes_dt(mocker):
    """compute_cql2 loads the config and substitutes the dTa/dTb values."""
    _logger(mocker)
    result = cas.compute_cql2("ValIntersect", 1, 2, "S1A")
    assert set(result) == {"filter", "sortby", "limit"}
    dumped = json.dumps(result)
    assert "{dTa}" not in dumped
    assert "{dTb}" not in dumped
    # placeholders resolved later in the pipeline are left untouched here
    assert "{product_type}" in dumped


def test_compute_cql2_unknown_query_raises(mocker):
    """An unknown query name cannot be found in the configuration."""
    _logger(mocker)
    with pytest.raises(StopIteration):
        cas.compute_cql2("does-not-exist", 0, 0, None)


# --------------------------------------------------------------------------- #
# convert_adf_group
# --------------------------------------------------------------------------- #
def _patch_group(mocker):
    _logger(mocker)
    sched = mocker.patch.object(cas, "schedule_adf_conversion")
    sched.with_options.return_value = AsyncMock()
    past = mocker.patch.object(cas, "past_adf_conversion")
    past.with_options.return_value = AsyncMock()
    return sched, past


async def test_convert_adf_group_rejects_bad_chronology(mocker):
    """period_start must be strictly before period_end."""
    _logger(mocker)
    now = datetime.now(timezone.utc)
    with pytest.raises(ValueError, match="before period_end"):
        await cas.convert_adf_group.fn(now, now - timedelta(days=1), {})


async def test_convert_adf_group_invalid_configuration_format(mocker):
    """A non-dict configuration raises ValueError."""
    _logger(mocker)
    now = datetime.now(timezone.utc)
    with pytest.raises(ValueError, match="❌ Configuration has got an invalid format."):
        await cas.convert_adf_group.fn(now - timedelta(days=1), now + timedelta(days=1), "{..")


async def test_convert_adf_group_past_only(mocker):
    """A fully-past period runs the conversion for each aux and schedules nothing."""
    aux = [
        {"product_type": "ADF_A", "cql2_query_name": "ValIntersect", "period_in_hours": 6},
        {"product_type": "ADF_B", "cql2_query_name": "ValIntersect", "period_in_hours": 0},
    ]
    settings = {"satellite": "S1A", "aux-to-be-generated": aux, "auxiliary-product-to-collection-identifier": []}
    sched, past = _patch_group(mocker)
    now = datetime.now(timezone.utc)

    await cas.convert_adf_group.fn(now - timedelta(days=2), now - timedelta(days=1), settings)

    assert past.with_options.call_count == 2
    sched.with_options.assert_not_called()


async def test_convert_adf_group_future_only(mocker):
    """A fully-future period schedules a conversion for each aux and runs nothing now."""
    aux = [{"product_type": "ADF_A", "cql2_query_name": "ValIntersect", "period_in_hours": 6}]
    settings = {"satellite": "S1A", "aux-to-be-generated": aux, "auxiliary-product-to-collection-identifier": []}
    sched, past = _patch_group(mocker)
    now = datetime.now(timezone.utc)

    await cas.convert_adf_group.fn(now + timedelta(days=1), now + timedelta(days=2), settings)

    assert sched.with_options.return_value.await_count == 1
    past.with_options.assert_not_called()


# --------------------------------------------------------------------------- #
# past_adf_conversion
# --------------------------------------------------------------------------- #
async def test_past_adf_conversion_single_run(mocker):
    """period_in_hours == 0 runs the conversion once over the whole period."""
    _logger(mocker)
    mocker.patch.object(cas, "compute_cql2", return_value={})
    conv = mocker.patch.object(cas, "adf_conversion_task")
    conv.with_options.return_value = AsyncMock()
    now = datetime.now(timezone.utc)

    await cas.past_adf_conversion.fn(
        "owner",
        "ADF_A",
        "ValIntersect",
        0,
        0,
        0,
        now - timedelta(hours=2),
        now,
        AUX_MAPPING,
        "S1A",
    )

    conv.with_options.return_value.assert_awaited_once()


async def test_past_adf_conversion_splits_period(mocker):
    """A non-zero period splits the range into sub-periods (12h / 6h -> 3 runs)."""
    _logger(mocker)
    mocker.patch.object(cas, "compute_cql2", return_value={})
    conv = mocker.patch.object(cas, "adf_conversion_task")
    conv.with_options.return_value = AsyncMock()
    start = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    await cas.past_adf_conversion.fn(
        "owner",
        "ADF_A",
        "ValIntersect",
        0,
        0,
        6,
        start,
        start + timedelta(hours=12),
        AUX_MAPPING,
        "S",
    )

    assert conv.with_options.return_value.await_count == 3


# --------------------------------------------------------------------------- #
# schedule_adf_conversion
# --------------------------------------------------------------------------- #
async def test_schedule_adf_conversion_single_rule(mocker):
    """A zero period produces a one-shot HOURLY;UNTIL rule."""
    _logger(mocker)
    mocker.patch.object(cas, "compute_cql2", return_value={})
    flow_mock = mocker.patch.object(cas, "schedule_conversion_flow", new=AsyncMock())
    start = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    await cas.schedule_adf_conversion.fn(
        "owner",
        "ADF_A",
        "ValIntersect",
        0,
        0,
        0,
        start,
        start + timedelta(hours=5),
        [],
        "S",
    )

    flow_mock.assert_awaited_once()
    rule = flow_mock.call_args.args[1]
    assert "FREQ=HOURLY;UNTIL=" in rule
    assert "INTERVAL" not in rule


async def test_schedule_adf_conversion_interval_rule(mocker):
    """A non-zero period produces a recurring HOURLY;INTERVAL rule."""
    _logger(mocker)
    mocker.patch.object(cas, "compute_cql2", return_value={})
    flow_mock = mocker.patch.object(cas, "schedule_conversion_flow", new=AsyncMock())
    start = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    await cas.schedule_adf_conversion.fn(
        "owner",
        "ADF_A",
        "ValIntersect",
        0,
        0,
        6,
        start,
        start + timedelta(hours=12),
        [],
        "S",
    )

    rule = flow_mock.call_args.args[1]
    assert "INTERVAL=6" in rule


# --------------------------------------------------------------------------- #
# adf_conversion_scheduled
# --------------------------------------------------------------------------- #
async def test_adf_conversion_scheduled_runs_conversion(mocker):
    """The scheduled flow decodes the period and runs the conversion for the window."""
    _logger(mocker)
    flow_run_mock = mocker.patch.object(cas, "flow_run")
    flow_run_mock.scheduled_start_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    conv = mocker.patch.object(cas, "adf_conversion_task")
    conv.with_options.return_value = AsyncMock()

    await cas.adf_conversion_scheduled.fn(FlowEnvArgs(owner_id="owner"), "ADF_A", {}, "3600", AUX_MAPPING)

    conv.with_options.return_value.assert_awaited_once()


# --------------------------------------------------------------------------- #
# schedule_conversion_flow
# --------------------------------------------------------------------------- #
async def test_schedule_conversion_flow_deploys(mocker):
    """schedule_conversion_flow reads the deployment and deploys a scheduled flow."""
    _logger(mocker)
    mocker.patch.object(cas, "runtime", MagicMock())
    mocker.patch.object(cas, "GitRepository", return_value=MagicMock())

    deployment = MagicMock()
    deployment.work_pool_name = "pool"
    deployment.pull_steps = [{"git_clone": {"repository": "https://example.com/repo.git", "branch": "main"}}]
    client = MagicMock()
    client.read_deployment = AsyncMock(return_value=deployment)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=client)
    ctx.__aexit__ = AsyncMock(return_value=None)
    mocker.patch.object(cas, "get_client", return_value=ctx)

    flow_obj = MagicMock()
    flow_obj.deploy = AsyncMock()
    mocker.patch.object(cas.flow, "from_source", new=AsyncMock(return_value=flow_obj))

    await cas.schedule_conversion_flow("owner", "RULE", {}, "ADF_A", timedelta(hours=1), [])

    flow_obj.deploy.assert_awaited_once()
    assert flow_obj.deploy.call_args.kwargs["work_pool_name"] == "pool"
    assert flow_obj.deploy.call_args.kwargs["rrule"] == "RULE"
