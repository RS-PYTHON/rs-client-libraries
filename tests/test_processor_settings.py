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

"""Unit tests for the default settings of the processings and the DPR flow call (rs_workflows.utils.dpr)."""

import json
from unittest.mock import AsyncMock

import pytest
from jsonschema import Draft202012Validator

from rs_client.ogcapi.dpr_client import DprPipeline, DprProcessor
from rs_workflows.flow_utils import (
    FlowEnvArgs,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.utils import dpr
from rs_workflows.utils.dpr import ProcessingId
from tests.common import PROCESSOR_SETTINGS, processor_settings

ENV_ARGS = FlowEnvArgs(owner_id="owner")
INPUT_PRODUCTS = [FlowInputProduct(name="S3OLCIL0_1", item_id="olcl0-item", collection_name="s03olcl0_")]
EXTERNAL_VARIABLES = {"start_datetime": "2026-10-01T10:00:00.000Z", "satellite": "sentinel-3a"}
PUBLISHED_ITEMS = [{"id": "S03OLCEFR_item", "properties": {"product:type": "S03OLCEFR"}}]
L0_SETTINGS = PROCESSOR_SETTINGS | {
    "processor": {"name": "s3_l0", "version": "1.0"},
    "pipeline": "s3_l0_full",
    "auxiliary_product_to_collection_identifier": [],
    "generated_product_to_collection_identifier": [{"name": "S03OLCL0_", "product_type": "S03OLCL0_"}],
    "input_collections": ["s03-cadip-session"],
    "cadip_collections": ["s3_sgs"],
}
ARD_SETTINGS = PROCESSOR_SETTINGS | {
    "processor": {"name": "s1_ard", "version": "1.0"},
    "pipeline": "SLC__1_NRB__1",
    "auxiliary_product_to_collection_identifier": [
        {"product_type": "ETAD", "collection_name": "s1-etad", "source": "cdse", "selected_assets": ["etad"]},
    ],
    "input_collections": ["s01siwslc"],
}


def _patch_settings(mocker, settings=None, running_clusters=("dask-processing", dpr.DASK_CLUSTER_STAGING)):
    """Mock the Prefect variable of the settings and the Dask clusters."""
    variable_get = mocker.patch.object(
        dpr.Variable,
        "get",
        new=AsyncMock(return_value=PROCESSOR_SETTINGS if settings is None else settings),
    )
    is_cluster_running = mocker.patch.object(
        dpr,
        "is_dask_cluster_running",
        new=AsyncMock(side_effect=lambda cluster: cluster in running_clusters),
    )
    return variable_get, is_cluster_running


# --------------------------------------------------------------------------- #
# load_processor_settings
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("settings", [PROCESSOR_SETTINGS, L0_SETTINGS, ARD_SETTINGS], ids=["olci", "l0", "ard"])
def test_settings_schema_is_a_valid_json_schema(settings):
    """The JSON schema of the default settings is itself valid, and accepts the settings documented."""
    schema = json.loads(dpr.PROCESSOR_SETTINGS_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(settings)


@pytest.mark.parametrize("processing_id", list(ProcessingId))
async def test_load_processor_settings(mocker, processing_id):
    """The settings are read from the Prefect variable of the processing, once its Dask clusters are checked."""
    variable_get, is_cluster_running = _patch_settings(mocker)

    settings = await dpr.load_processor_settings(processing_id)

    variable_get.assert_awaited_once_with(f"{processing_id.value}-default-setting")
    assert [call.args for call in is_cluster_running.await_args_list] == [
        ("dask-processing",),
        (dpr.DASK_CLUSTER_STAGING,),
    ]
    assert settings.processor.name == "s3_l1olci"
    assert settings.priority == Priority.HIGH
    assert settings.processing_mode == [ProcessingMode.NRT]
    assert settings.auxiliary_product_to_collection_identifier[1].source == "catalog"
    assert not settings.cadip_collections
    assert not settings.input_collections


async def test_load_processor_settings_of_l0(mocker):
    """The Level-0 settings contain the collections of the sessions."""
    _patch_settings(mocker, L0_SETTINGS)

    settings = await dpr.load_processor_settings(ProcessingId.S3_L0)

    assert settings.input_collections == ["s03-cadip-session"]
    assert settings.cadip_collections == ["s3_sgs"]
    assert settings.generated_product_to_collection_identifier[0].collection_name is None


async def test_load_processor_settings_fails_without_variable(mocker):
    """Without the Prefect variable of the processing, the settings cannot be read."""
    _, is_cluster_running = _patch_settings(mocker)
    mocker.patch.object(dpr.Variable, "get", new=AsyncMock(return_value=None))

    with pytest.raises(RuntimeError, match="'s3l1olci-default-setting' is missing"):
        await dpr.load_processor_settings(ProcessingId.S3_OLCI_L1)

    is_cluster_running.assert_not_awaited()


@pytest.mark.parametrize(
    "settings",
    [
        {key: value for key, value in PROCESSOR_SETTINGS.items() if key != "processor"},
        {key: value for key, value in PROCESSOR_SETTINGS.items() if key != "pipeline"},
        PROCESSOR_SETTINGS | {"unit": "ol1_unit"},
        PROCESSOR_SETTINGS | {"priority": "urgent"},
        PROCESSOR_SETTINGS | {"processing_mode": []},
        PROCESSOR_SETTINGS | {"unknown": "field"},
        PROCESSOR_SETTINGS | {"cadip_collections": "s3_sgs"},
    ],
    ids=[
        "no-processor",
        "no-pipeline-nor-unit",
        "pipeline-and-unit",
        "bad-priority",
        "no-mode",
        "unknown-field",
        "bad-cadip-collections",
    ],
)
async def test_load_processor_settings_rejects_invalid_settings(mocker, settings):
    """Settings that don't match the JSON schema are rejected before checking the Dask clusters."""
    _, is_cluster_running = _patch_settings(mocker, settings)

    with pytest.raises(ValueError, match="Invalid JSON schema .* 's3l1olci-default-setting'"):
        await dpr.load_processor_settings(ProcessingId.S3_OLCI_L1)

    is_cluster_running.assert_not_awaited()


@pytest.mark.parametrize(
    "running_clusters,missing_cluster",
    [((dpr.DASK_CLUSTER_STAGING,), "dask-processing"), (("dask-processing",), dpr.DASK_CLUSTER_STAGING)],
)
async def test_load_processor_settings_requires_running_dask_clusters(mocker, running_clusters, missing_cluster):
    """The processing and staging Dask clusters must be running."""
    _patch_settings(mocker, running_clusters=running_clusters)

    with pytest.raises(ValueError, match=f"'{missing_cluster}' is unknown or not ready"):
        await dpr.load_processor_settings(ProcessingId.S3_OLCI_L1)


# --------------------------------------------------------------------------- #
# call_dpr_flow
# --------------------------------------------------------------------------- #
def _patch_dpr_flow(mocker):
    """Mock the payload path, the generic processing flow and the events."""
    mocker.patch.object(dpr, "generate_payload_path", new=AsyncMock(return_value="/shared/owner/date"))
    dpr_processing = mocker.patch.object(dpr, "dpr_processing_task", new=AsyncMock(return_value=PUBLISHED_ITEMS))
    emit_events = mocker.patch.object(dpr, "emit_product_generated_events")
    return dpr_processing, emit_events


@pytest.mark.parametrize("workflow", [WorkflowType.ON_DEMAND, WorkflowType.SYSTEMATIC])
async def test_call_dpr_flow(mocker, workflow):
    """The processor is run with the settings of the processing, then the products events are emitted with the
    workflow of the processing."""
    dpr_processing, emit_events = _patch_dpr_flow(mocker)
    settings = processor_settings()

    result = await dpr.call_dpr_flow(ENV_ARGS, settings, INPUT_PRODUCTS, EXTERNAL_VARIABLES, workflow)

    assert result == PUBLISHED_ITEMS
    dpr_input = dpr_processing.call_args.args[0]
    assert dpr_input.env == ENV_ARGS
    assert dpr_input.processor_name == DprProcessor.S3L1OLCI
    assert dpr_input.processor_version == "1.0"
    assert dpr_input.dask_cluster_label == "dask-processing"
    assert dpr_input.s3_payload_file == "/shared/owner/date/payload_s3_l1olci.yaml"
    assert dpr_input.pipeline == "ol1_eo"
    assert dpr_input.unit is None
    assert dpr_input.priority == Priority.HIGH
    assert dpr_input.processing_mode == [ProcessingMode.NRT]
    assert dpr_input.workflow_type == workflow
    assert dpr_input.input_products == INPUT_PRODUCTS
    assert dpr_input.start_datetime is not None
    assert dpr_input.satellite == "sentinel-3a"
    assert dpr_input.generated_product_to_collection_identifier == settings.generated_product_to_collection_identifier
    assert dpr_input.auxiliary_product_to_collection_identifier == settings.auxiliary_product_to_collection_identifier
    emit_events.assert_called_once_with("owner", PUBLISHED_ITEMS, workflow)


async def test_call_dpr_flow_runs_a_unit(mocker):
    """A unit of the task table can be run instead of a pipeline."""
    dpr_processing, _ = _patch_dpr_flow(mocker)
    settings = processor_settings(pipeline=None, unit="ol1_unit")

    await dpr.call_dpr_flow(ENV_ARGS, settings, INPUT_PRODUCTS, EXTERNAL_VARIABLES, WorkflowType.ON_DEMAND)

    dpr_input = dpr_processing.call_args.args[0]
    assert dpr_input.pipeline is None
    assert dpr_input.unit == "ol1_unit"


async def test_call_dpr_flow_runs_the_given_pipeline(mocker):
    """The pipeline given by the flow replaces the pipeline or the unit of the settings."""
    dpr_processing, _ = _patch_dpr_flow(mocker)
    settings = processor_settings(processor={"name": "s1_ard", "version": "1.0"}, pipeline=None, unit="ard_unit")

    await dpr.call_dpr_flow(
        ENV_ARGS,
        settings,
        INPUT_PRODUCTS,
        {},
        WorkflowType.ON_DEMAND,
        pipeline="ARD_REFERENCE_PIPELINE",
        temporary_shared=True,
    )

    dpr_input = dpr_processing.call_args.args[0]
    assert dpr_input.pipeline == DprPipeline.S1ARD_REF
    assert dpr_input.unit is None
    assert dpr_input.temporary_shared is True
