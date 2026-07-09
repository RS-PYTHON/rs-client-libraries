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

"""Unit tests for rs_workflows.on_demand.common.staging."""

from datetime import timedelta
from enum import Enum
from unittest.mock import AsyncMock, MagicMock

import pytest

from rs_workflows.flow_utils import FlowEnvArgs
from rs_workflows.on_demand.common import staging
from rs_workflows.on_demand.common.staging import (
    CadipCollections,
    cadip_session_stage,
    create_result_artifact,
    make_session_enum,
    stage_latest_session,
    stage_selected_session,
    stage_session_common,
)


def _flow_env_mock():
    """Return a FlowEnv mock whose start_span() works as a context manager."""
    flow_env = MagicMock()
    flow_env.start_span.return_value.__enter__.return_value = MagicMock()
    return flow_env


# --------------------------------------------------------------------------- #
# make_session_enum / CadipCollections
# --------------------------------------------------------------------------- #
def test_make_session_enum_inverts_mapping():
    """make_session_enum turns {id: label} into an Enum {label: id}."""
    result = make_session_enum({"id-1": "label one", "id-2": "label two"})
    assert issubclass(result, Enum)  # type: ignore[arg-type]
    assert result["label one"].value == "id-1"  # type: ignore[index]
    assert result["label two"].value == "id-2"  # type: ignore[index]


def test_cadip_collection_satellite_index():
    """The second character of a CADIP collection identifies the satellite number."""
    assert CadipCollections.S1_SGS[1] == "1"
    assert CadipCollections.S3_SGS[1] == "3"


# --------------------------------------------------------------------------- #
# create_result_artifact
# --------------------------------------------------------------------------- #
async def test_create_result_artifact_builds_markdown_and_link(mocker):
    """The result artifact contains the session id/duration and a monitoring link."""
    markdown_mock = AsyncMock()
    link_mock = AsyncMock()
    mocker.patch.object(staging, "acreate_markdown_artifact", markdown_mock)
    mocker.patch.object(staging, "acreate_link_artifact", link_mock)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())

    await create_result_artifact.fn("SESSION_42", timedelta(seconds=90))

    markdown = markdown_mock.call_args.kwargs["markdown"]
    assert "SESSION_42" in markdown
    assert "0:01:30" in markdown  # str(timedelta(seconds=90))
    link_mock.assert_awaited_once()
    assert "from=" in link_mock.call_args.kwargs["link"]
    assert "to=" in link_mock.call_args.kwargs["link"]


# --------------------------------------------------------------------------- #
# cadip_session_stage
# --------------------------------------------------------------------------- #
async def test_cadip_session_stage_returns_status_for_host(mocker):
    """cadip_session_stage returns the status keyed by the search URL hostname."""
    flow_env = _flow_env_mock()
    staging_client = flow_env.rs_client.get_staging_client.return_value
    staging_client.run_staging.return_value = "job-status"
    staging_client.wait_for_jobs.return_value = {"cadip.example.com": {"status": "successful"}}
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())

    result = await cadip_session_stage.fn(
        FlowEnvArgs(owner_id="owner"),
        "https://cadip.example.com/search?ids=SESSION_42",
        "s01-cadip-session",
    )

    assert result == "successful"
    staging_client.run_staging.assert_called_once_with(
        "https://cadip.example.com/search?ids=SESSION_42",
        "s01-cadip-session",
    )


async def test_cadip_session_stage_missing_status_returns_empty(mocker):
    """A host entry without a status yields an empty string."""
    flow_env = _flow_env_mock()
    staging_client = flow_env.rs_client.get_staging_client.return_value
    staging_client.wait_for_jobs.return_value = {"cadip.example.com": {}}
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())

    result = await cadip_session_stage.fn(
        FlowEnvArgs(owner_id="owner"),
        "https://cadip.example.com/search?ids=SESSION",
        "s01-cadip-session",
    )
    assert result == ""


# --------------------------------------------------------------------------- #
# stage_session_common
# --------------------------------------------------------------------------- #
def _patch_stage_session_common(mocker, status):
    """Patch the collaborators used by stage_session_common; return its mocks."""
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())
    check_mock = mocker.patch.object(staging, "check_and_create_collection", new=AsyncMock())

    stage_mock = mocker.patch.object(staging, "cadip_session_stage")
    stage_mock.submit.return_value.result.return_value = status

    artifact_mock = mocker.patch.object(staging, "create_result_artifact")
    artifact_mock.submit.return_value.result.return_value = None

    return check_mock, stage_mock, artifact_mock


async def test_stage_session_common_success(mocker):
    """A successful staging returns True and builds the expected collection name."""
    check_mock, stage_mock, _ = _patch_stage_session_common(mocker, "successful")
    flow_env = MagicMock()
    report = MagicMock()

    result = await stage_session_common.fn(flow_env, CadipCollections.S1_SGS, "SESSION_42", report)

    assert result is True
    check_mock.assert_awaited_once_with(flow_env, "s01-cadip-session")
    report.success_step.assert_called_once()
    # the staging task is submitted with the session id inside the search URL
    assert "SESSION_42" in stage_mock.submit.call_args.kwargs["cadip_search_url"]


async def test_stage_session_common_failure(mocker):
    """A non-successful staging returns False and reports the failure."""
    _patch_stage_session_common(mocker, "failed")
    flow_env = MagicMock()
    report = MagicMock()

    result = await stage_session_common.fn(flow_env, CadipCollections.S3_SGS, "SESSION", report)

    assert result is False
    report.failed_step.assert_called_once()


async def test_stage_session_common_works_without_report(mocker):
    """The report manager is optional."""
    _patch_stage_session_common(mocker, "successful")
    result = await stage_session_common.fn(MagicMock(), CadipCollections.S1_SGS, "SESSION")
    assert result is True


# --------------------------------------------------------------------------- #
# stage_latest_session
# --------------------------------------------------------------------------- #
async def test_stage_latest_session_raises_when_no_session(mocker):
    """When no session is found, the flow raises ValueError."""
    flow_env = _flow_env_mock()
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())
    search_mock = mocker.patch.object(staging, "cadip_session_search")
    search_mock.submit.return_value.result.return_value = []

    with pytest.raises(ValueError, match="No Cadip session found"):
        await stage_latest_session.fn(CadipCollections.S1_SGS)


async def test_stage_latest_session_stages_found_session(mocker):
    """The latest session found is forwarded to stage_session_common."""
    flow_env = _flow_env_mock()
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())
    search_mock = mocker.patch.object(staging, "cadip_session_search")
    search_mock.submit.return_value.result.return_value = [MagicMock(id="SESSION_42")]
    stage_common = mocker.patch.object(staging, "stage_session_common", new=AsyncMock())

    await stage_latest_session.fn(CadipCollections.S1_SGS)

    stage_common.assert_awaited_once_with(flow_env, CadipCollections.S1_SGS, "SESSION_42", None)


async def test_stage_latest_session_verbose_pushes_report(mocker):
    """In verbose mode a ReportManager report is pushed at the end."""
    flow_env = _flow_env_mock()
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())
    search_mock = mocker.patch.object(staging, "cadip_session_search")
    search_mock.submit.return_value.result.return_value = [MagicMock(id="SESSION_42")]
    mocker.patch.object(staging, "stage_session_common", new=AsyncMock())

    report_instance = MagicMock()
    report_instance.push_report = AsyncMock()
    mocker.patch.object(staging, "ReportManager", return_value=report_instance)

    await stage_latest_session.fn(CadipCollections.S1_SGS, verbose=True)

    report_instance.success_step.assert_called_once()
    report_instance.push_report.assert_awaited_once()


# --------------------------------------------------------------------------- #
# stage_selected_session (early no-session branch)
# --------------------------------------------------------------------------- #
async def test_stage_selected_session_raises_when_no_session(mocker):
    """stage_selected_session raises ValueError when no session is found."""
    flow_env = _flow_env_mock()
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())
    search_mock = mocker.patch.object(staging, "cadip_session_search")
    search_mock.submit.return_value.result.return_value = None

    with pytest.raises(ValueError, match="No Cadip session found"):
        await stage_selected_session.fn(CadipCollections.S1_SGS)


async def test_stage_selected_session_stages_user_choice(mocker):
    """stage_selected_session builds the session list, pauses for input, then stages the choice."""
    flow_env = _flow_env_mock()
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())

    published, orbit = "2023-01-01T00:00:00Z", 123
    item = MagicMock(id="SESSION_42")
    item.properties = {"published": published, "sat:absolute_orbit": orbit}
    search_mock = mocker.patch.object(staging, "cadip_session_search")
    search_mock.submit.return_value.result.return_value = MagicMock(items=[item])

    # The paused input returns the enum value, which is the display key of the session.
    display_key = f"📡 SESSION_42 🕒 {published} 🌍 {orbit}"
    selection = MagicMock()
    selection.selected.value = display_key
    mocker.patch.object(staging, "apause_flow_run", new=AsyncMock(return_value=selection))
    stage_common = mocker.patch.object(staging, "stage_session_common", new=AsyncMock())

    await stage_selected_session.fn(CadipCollections.S1_SGS)

    stage_common.assert_awaited_once_with(flow_env, CadipCollections.S1_SGS, "SESSION_42")


async def test_stage_latest_session_verbose_no_session_reports_failure(mocker):
    """In verbose mode, a missing session records a failed step before raising."""
    flow_env = _flow_env_mock()
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())
    search_mock = mocker.patch.object(staging, "cadip_session_search")
    search_mock.submit.return_value.result.return_value = []
    report_instance = MagicMock()
    report_instance.push_report = AsyncMock()
    mocker.patch.object(staging, "ReportManager", return_value=report_instance)

    with pytest.raises(ValueError, match="No Cadip session found"):
        await stage_latest_session.fn(CadipCollections.S1_SGS, verbose=True)

    report_instance.failed_step.assert_called_once()


async def test_stage_latest_session_none_id_does_not_stage(mocker):
    """A found session with a None id is not staged and is reported as missing."""
    flow_env = _flow_env_mock()
    mocker.patch.object(staging, "FlowEnv", return_value=flow_env)
    mocker.patch.object(staging, "get_run_logger", return_value=MagicMock())
    search_mock = mocker.patch.object(staging, "cadip_session_search")
    search_mock.submit.return_value.result.return_value = [MagicMock(id=None)]
    stage_common = mocker.patch.object(staging, "stage_session_common", new=AsyncMock())
    report_instance = MagicMock()
    report_instance.push_report = AsyncMock()
    mocker.patch.object(staging, "ReportManager", return_value=report_instance)

    await stage_latest_session.fn(CadipCollections.S1_SGS, verbose=True)

    stage_common.assert_not_awaited()
    report_instance.failed_step.assert_called_once()
