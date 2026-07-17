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

"""Unit tests for rs_workflows.on_demand.common.types."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, call

import pytest

from rs_workflows.flow_utils import (
    LoggingLevel,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand.common import types
from rs_workflows.on_demand.common.types import (
    DEFAULT_PREFECT_CONFIGURATION,
    Level0FlowParams,
    Level1FlowParams,
    ProcessingFlowParams,
)


def _patch_variable(mocker, value):
    """Patch the Prefect Variable.get used by ProcessingFlowParams._resolve."""
    mocker.patch.object(types.Variable, "get", new=AsyncMock(return_value=value))


def test_default_prefect_configuration_format():
    """The default configuration template resolves to the sX-lY variable name."""
    assert DEFAULT_PREFECT_CONFIGURATION.format(mission="1", level="0") == "s1-l0-default-setting"


def test_resolve_specific_base_returns_empty():
    """The base ProcessingFlowParams injects no mission-specific field."""
    # pylint: disable=protected-access
    assert not ProcessingFlowParams()._resolve_specific({"anything": 1})


def test_level1_datetime_fields_parse_iso_strings():
    """Level-1 datetime fields are annotated Pydantic fields accepting ISO input."""
    params = Level1FlowParams.model_validate(
        {
            "start_datetime": "2024-05-27T09:44:09.509000Z",
            "end_datetime": "2024-05-27T10:44:09.509000Z",
        }
    )

    assert isinstance(params.start_datetime, datetime)
    assert isinstance(params.end_datetime, datetime)


async def test_level1_resolve_uses_prefect_datetime_settings(mocker):
    """Level-1 parameters fall back to the datetime window configured in Prefect."""
    _patch_variable(
        mocker,
        {
            "common": {"owner_identifier": "settings-owner"},
            "l1": {
                "satellite": "sentinel-3a",
                "start_datetime": "2025-06-12T02:11:13Z",
                "end_datetime": "2025-06-12T02:13:13Z",
            },
        },
    )

    resolved = await Level1FlowParams().resolve("3")

    assert resolved.satellite == "sentinel-3a"
    assert resolved.owner_identifier == "settings-owner"
    assert resolved.start_datetime == datetime(2025, 6, 12, 2, 11, 13, tzinfo=timezone.utc)
    assert resolved.end_datetime == datetime(2025, 6, 12, 2, 13, 13, tzinfo=timezone.utc)


async def test_level0_resolve_uses_prefect_datetime_settings(mocker):
    """Level-0 parameters use the datetime window configured in Prefect."""
    _patch_variable(
        mocker,
        {
            "l0": {
                "start_datetime": "2025-06-11T00:00:00Z",
                "end_datetime": "2025-06-13T00:00:00Z",
            },
        },
    )

    resolved = await Level0FlowParams().resolve("3")

    assert resolved.start_datetime == datetime(2025, 6, 11, tzinfo=timezone.utc)
    assert resolved.end_datetime == datetime(2025, 6, 13, tzinfo=timezone.utc)


async def test_resolve_uses_model_defaults_when_variable_missing(mocker):
    """A missing Prefect variable falls back to the model defaults."""
    _patch_variable(mocker, None)
    resolved = await Level0FlowParams().resolve("1")
    assert resolved == Level0FlowParams()


async def test_resolve_uses_defaults_when_raw_not_dict(mocker):
    """A non-dict variable payload is treated as empty settings (defaults kept)."""
    _patch_variable(mocker, "not-a-dict")
    resolved = await Level0FlowParams().resolve("1")
    assert resolved.owner_identifier == ""
    assert resolved.dask_cluster_label == ""
    assert resolved.pipeline is None
    assert resolved.unit is None
    assert resolved.cadip_collections == []
    assert resolved.logging_level == LoggingLevel.INFO


async def test_resolve_pipeline_and_unit_are_mutually_exclusive(mocker):
    """Setting both pipeline and unit is rejected."""
    _patch_variable(mocker, {})
    with pytest.raises(ValueError, match="mutually exclusive"):
        await Level0FlowParams(pipeline="my-pipe", unit="my-unit").resolve("1")


async def test_resolve_user_pipeline_takes_precedence(mocker):
    """A user-provided pipeline wins over settings and clears the unit."""
    _patch_variable(mocker, {"pipeline": "settings-pipe", "unit": "settings-unit"})
    resolved = await Level0FlowParams(pipeline="user-pipe").resolve("1")
    assert resolved.pipeline == "user-pipe"
    assert resolved.unit is None


async def test_resolve_user_unit_takes_precedence(mocker):
    """A user-provided unit wins over settings and clears the pipeline."""
    _patch_variable(mocker, {"pipeline": "settings-pipe"})
    resolved = await Level0FlowParams(unit="user-unit").resolve("1")
    assert resolved.unit == "user-unit"
    assert resolved.pipeline is None


async def test_resolve_falls_back_to_settings_pipeline(mocker):
    """When neither is provided, pipeline/unit come from the settings."""
    _patch_variable(mocker, {"pipeline": "settings-pipe"})
    resolved = await Level0FlowParams().resolve("1")
    assert resolved.pipeline == "settings-pipe"
    assert resolved.unit is None


async def test_resolve_merges_settings_and_params(mocker):
    """Parameters override settings; empty parameters are filled from settings."""
    settings = {
        "owner_identifier": "settings-owner",
        "dask_cluster_name": "settings-cluster",
        "processor": {"name": "proc", "version": "1.2.3"},
        "priority": "high",
        "processing_mode": ["nrt"],
        "workflow": "on-demand",
        "session_collection": "s1-cadip-sessions",
        "cadip_collections": ["s1_sgs", "s1_mps"],
        "generated_product_to_collection_identifier": [
            {"name": "GP", "product_type": "T", "collection_name": "C"},
        ],
        "auxiliary_product_to_collection_identifier": [
            {"product_type": "AX", "collection_name": "AUX"},
        ],
    }
    _patch_variable(mocker, settings)

    resolved = await Level0FlowParams(owner_identifier="param-owner").resolve("1")

    assert resolved.owner_identifier == "param-owner"  # parameter wins
    assert resolved.dask_cluster_label == "settings-cluster"  # settings key is dask_cluster_name
    assert resolved.processor_name == "proc"
    assert resolved.processor_version == "1.2.3"
    assert resolved.priority == Priority.HIGH
    assert resolved.processing_mode == [ProcessingMode.NRT]
    assert resolved.workflow == WorkflowType.ON_DEMAND
    assert resolved.session_collection == "s1-cadip-sessions"
    assert resolved.cadip_collections == ["s1_sgs", "s1_mps"]
    assert resolved.generated_product_to_collection_identifier[0].name == "GP"  # type: ignore[index]
    assert resolved.auxiliary_product_to_collection_identifier[0].product_type == "AX"  # type: ignore[index]


async def test_resolve_uses_correct_variable_name_per_mission(mocker):
    """Sentinel-3 levels use the unified processing configuration."""
    get_mock = AsyncMock(side_effect=[{"l0": {}}, {}])
    mocker.patch.object(types.Variable, "get", new=get_mock)
    await Level0FlowParams().resolve("3")
    assert get_mock.await_args_list == [
        call("s3-processing-default-setting", default={}),
        call("s3-l0-default-setting", default={}),
    ]
