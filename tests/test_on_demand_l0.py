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

from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct, WorkflowType
from rs_workflows.on_demand.common import l0, l0_last_steps
from rs_workflows.on_demand.sentinel1 import s1_l0
from rs_workflows.on_demand.sentinel3 import s3_l0
from rs_workflows.utils.dpr import ProcessingId
from tests.common import flow_env_mock, processor_settings

ENV_ARGS = FlowEnvArgs(owner_id="owner")
SETTINGS = processor_settings(input_collections=["s0x-cadip-session", "s0x-other-session"], cadip_collections=["sgs"])
S1_SESSION = "S1A_20230101T000000"
S3_SESSION = "S3A_20230101T000000"


# --------------------------------------------------------------------------- #
# process_l0
# --------------------------------------------------------------------------- #
def _patch_l0(
    mocker,
    *,
    item=None,
    station=None,
    staged=True,
    unpublished=(False, None),
    published=(True, datetime.now(timezone.utc)),
):
    mocker.patch.object(l0, "get_logger", return_value=MagicMock())
    mocker.patch.object(l0, "FlowEnv", return_value=flow_env_mock())
    load_settings = mocker.patch.object(l0, "load_processor_settings", new=AsyncMock(return_value=SETTINGS))
    get_item = mocker.patch.object(l0, "get_single_catalog_item", new=AsyncMock(return_value=item))
    mocker.patch.object(l0, "is_unpublished", return_value=unpublished)
    mocker.patch.object(l0, "is_published", return_value=published)
    get_station = mocker.patch.object(l0, "get_cadip_station", new=AsyncMock(return_value=station))
    stage_mock = mocker.patch.object(l0, "stage_session_common", new=AsyncMock(return_value=staged))
    flows = {"1": AsyncMock(return_value=["s1-product"]), "3": AsyncMock(return_value=["s3-product"])}
    mocker.patch.dict(l0.L0_FLOWS, flows)
    return {
        "load_settings": load_settings,
        "get_item": get_item,
        "get_station": get_station,
        "stage": stage_mock,
        "s1_flow": flows["1"],
        "s3_flow": flows["3"],
    }


def test_l0_flows():
    """The Level-0 flow of each mission is run by process-l0."""
    assert l0.L0_FLOWS == {"1": s1_l0.process_s1l0, "3": s3_l0.process_s3l0}


@pytest.mark.parametrize(
    "session,message",
    [("INVALID", "Invalid session name"), ("S2A_20230101T000000", "No Level-0 processing for Sentinel-2")],
)
async def test_process_l0_rejects_bad_session(mocker, session, message):
    """An invalid session name, or a mission without Level-0 processing, raises ValueError before reading the
    settings."""
    mocks = _patch_l0(mocker)
    with pytest.raises(ValueError, match=message):
        await l0.process_l0.fn(session, ENV_ARGS)
    mocks["load_settings"].assert_not_awaited()


async def test_process_l0_checks_the_settings_before_searching_the_session(mocker):
    """An invalid setting or a Dask cluster not ready raises an error before searching the session."""
    mocks = _patch_l0(mocker)
    mocks["load_settings"].side_effect = ValueError("❌ 'cluster' is unknown or not ready.")
    with pytest.raises(ValueError, match="unknown or not ready"):
        await l0.process_l0.fn(S1_SESSION, ENV_ARGS)
    mocks["get_item"].assert_not_awaited()


async def test_process_l0_requires_the_session_collections(mocker):
    """The settings of a Level-0 processing must give the collections of the sessions."""
    mocks = _patch_l0(mocker)
    mocks["load_settings"].return_value = processor_settings()
    with pytest.raises(ValueError, match="must define the 'input_collections'"):
        await l0.process_l0.fn(S1_SESSION, ENV_ARGS)
    mocks["get_item"].assert_not_awaited()


async def test_process_l0_raises_when_session_unpublished(mocker):
    """A found but unpublished session raises ValueError."""
    _patch_l0(mocker, item=MagicMock(), unpublished=(True, "2020-01-01"))
    with pytest.raises(ValueError, match="unpublished"):
        await l0.process_l0.fn(S1_SESSION, ENV_ARGS)


async def test_process_l0_raises_when_session_not_published(mocker):
    """A found session that is not published yet raises ValueError."""
    _patch_l0(mocker, item=MagicMock(), unpublished=(False, None), published=(False, None))
    with pytest.raises(ValueError, match="not been published"):
        await l0.process_l0.fn(S1_SESSION, ENV_ARGS)


@pytest.mark.parametrize("force_systematic", [False, True])
async def test_process_l0_runs_the_s1_flow_for_a_session_found_in_catalog(mocker, force_systematic):
    """A published S1 session in the catalog is processed by the S1 Level-0 flow, systematic when forced."""
    mocks = _patch_l0(mocker, item=MagicMock())
    result = await l0.process_l0.fn(S1_SESSION, ENV_ARGS, force_systematic=force_systematic)
    assert result == ["s1-product"]
    mocks["load_settings"].assert_awaited_once_with(ProcessingId.S1_L0)
    mocks["get_item"].assert_awaited_once()
    assert mocks["get_item"].call_args.args[1:] == (S1_SESSION, ["s0x-cadip-session", "s0x-other-session"])
    mocks["stage"].assert_not_awaited()
    mocks["s1_flow"].assert_awaited_once_with(
        session=S1_SESSION,
        env_args=ENV_ARGS,
        force_systematic=force_systematic,
        verbose=False,
    )
    mocks["s3_flow"].assert_not_awaited()


async def test_process_l0_stages_then_runs_the_s3_flow(mocker):
    """An S3 session absent from the catalog is staged from the CADIP collections of the settings, then processed
    by the S3 Level-0 flow."""
    mocks = _patch_l0(mocker, item=None, station="s3_sgs", staged=True)
    result = await l0.process_l0.fn(S3_SESSION, ENV_ARGS, force_systematic=True)
    assert result == ["s3-product"]
    mocks["load_settings"].assert_awaited_once_with(ProcessingId.S3_L0)
    assert mocks["get_station"].call_args.args[1:] == (S3_SESSION, ["sgs"])
    mocks["stage"].assert_awaited_once()
    mocks["s3_flow"].assert_awaited_once_with(
        session=S3_SESSION,
        env_args=ENV_ARGS,
        force_systematic=True,
        verbose=False,
    )
    mocks["s1_flow"].assert_not_awaited()


async def test_process_l0_not_processed_when_station_not_found(mocker):
    """When the session is neither cataloged nor stageable, it is not processed."""
    mocks = _patch_l0(mocker, item=None, station=None)
    assert await l0.process_l0.fn(S1_SESSION, ENV_ARGS) is None
    mocks["stage"].assert_not_awaited()
    mocks["s1_flow"].assert_not_awaited()


# --------------------------------------------------------------------------- #
# process_staged_session and process_l0_last_steps
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "mission,processing_id,force_systematic,workflow",
    [
        ("1", ProcessingId.S1_L0, False, WorkflowType.ON_DEMAND),
        ("3", ProcessingId.S3_L0, True, WorkflowType.SYSTEMATIC),
    ],
)
async def test_process_staged_session(mocker, mission, processing_id, force_systematic, workflow):
    """A staged session is processed with the settings of the Level-0 processing of its mission."""
    load_settings = mocker.patch.object(
        l0_last_steps,
        "load_processor_settings",
        new=AsyncMock(return_value=SETTINGS),
    )
    last_steps = mocker.patch.object(l0_last_steps, "process_l0_last_steps", new=AsyncMock(return_value=["product"]))

    result = await l0_last_steps.process_staged_session(mission, "session", ENV_ARGS, force_systematic, True)

    assert result == ["product"]
    load_settings.assert_awaited_once_with(processing_id)
    last_steps.assert_awaited_once_with(
        mission=mission,
        session="session",
        env_args=ENV_ARGS,
        settings=SETTINGS,
        workflow=workflow,
        verbose=True,
    )


def _patch_last_steps(mocker, item):
    mocker.patch.object(l0_last_steps, "get_logger", return_value=MagicMock())
    mocker.patch.object(l0_last_steps, "FlowEnv", return_value=flow_env_mock())
    mocker.patch.object(l0_last_steps, "get_single_catalog_item", new=AsyncMock(return_value=item))
    return mocker.patch.object(l0_last_steps, "call_dpr_flow", new=AsyncMock(return_value=[]))


def _session_item(properties):
    item = MagicMock()
    item.properties = properties
    item.collection_id = "s0x-other-session"
    return item


async def test_process_l0_last_steps_returns_early_without_item(mocker):
    """When the session is not in the catalog, no DPR flow is triggered."""
    dpr_mock = _patch_last_steps(mocker, item=None)
    result = await l0_last_steps.process_l0_last_steps("1", "S1A_x", ENV_ARGS, SETTINGS, WorkflowType.ON_DEMAND, False)
    assert result is None
    dpr_mock.assert_not_awaited()


async def test_process_l0_last_steps_requires_the_session_datetime(mocker):
    """A session without datetime cannot be processed."""
    dpr_mock = _patch_last_steps(mocker, item=_session_item({}))
    with pytest.raises(ValueError, match="'datetime' property"):
        await l0_last_steps.process_l0_last_steps("1", "S1A_x", ENV_ARGS, SETTINGS, WorkflowType.ON_DEMAND, False)
    dpr_mock.assert_not_awaited()


@pytest.mark.parametrize(
    "mission,session,input_name,satellite",
    [("1", "S1A_session", "S1CADUS", "sentinel-1a"), ("3", "S3B_session", "S3ACADUS", "sentinel-3b")],
)
async def test_process_l0_last_steps_calls_dpr_flow(mocker, mission, session, input_name, satellite):
    """A session found in a collection of the settings is processed with the settings, its satellite, and its
    publication date as sensing dates; all the generated products are returned for Prefect persistence."""
    dpr_mock = _patch_last_steps(mocker, item=_session_item({"datetime": "2023-01-01T00:00:00"}))
    dpr_mock.return_value = [{"id": "S03OLCL0__product.zarr"}, {"id": "S03NATL0__product.zarr"}]

    result = await l0_last_steps.process_l0_last_steps(
        mission,
        session,
        ENV_ARGS,
        SETTINGS,
        WorkflowType.SYSTEMATIC,
        True,
    )

    assert result == dpr_mock.return_value
    session_datetime = datetime.fromisoformat("2023-01-01T00:00:00")
    dpr_mock.assert_awaited_once_with(
        ENV_ARGS,
        settings=SETTINGS,
        input_products=[FlowInputProduct(name=input_name, item_id=session, collection_name="s0x-other-session")],
        external_variables={
            "start_datetime": session_datetime,
            "end_datetime": session_datetime,
            "satellite": satellite,
        },
        workflow=WorkflowType.SYSTEMATIC,
    )


# --------------------------------------------------------------------------- #
# sentinel1 / sentinel3 entry points
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("module,mission", [(s1_l0, "1"), (s3_l0, "3")])
async def test_process_mission_l0(mocker, module, mission):
    """The flow of a mission processes the staged session with the settings of the mission."""
    process = mocker.patch.object(module, "process_staged_session", new=AsyncMock(return_value=["product"]))
    flow = s1_l0.process_s1l0 if module is s1_l0 else s3_l0.process_s3l0

    result = await flow.fn("session", ENV_ARGS, force_systematic=True)

    assert result == ["product"]
    process.assert_awaited_once_with(mission, "session", ENV_ARGS, True, False)
