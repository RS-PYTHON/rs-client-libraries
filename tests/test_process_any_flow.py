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

"""Unit tests for the process-any flow (rs_workflows.process_any_flow)."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from jsonschema import Draft202012Validator

from rs_workflows import process_any_flow
from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct, WorkflowType
from rs_workflows.process_any_flow import FlowInputItem, ProcessingId

PROCESSING_DASK_CLUSTER = "dask-s3-olci"

DEFAULT_SETTINGS = {
    "processor": {"name": "s3_l1olci", "version": "1.0"},
    "pipeline": "ol1_eo",
    "priority": "high",
    "processing_mode": ["nrt"],
    "dask_cluster_name": PROCESSING_DASK_CLUSTER,
    "auxiliary_product_to_collection_identifier": [
        {"product_type": "AX___OSF_AX", "collection_name": "ax___osf_ax"},
        {"source": "catalog", "product_type": "ADF_OLCAL", "collection_name": "adf-olci"},
    ],
    "generated_product_to_collection_identifier": [
        {"name": "S03OLCEFR", "product_type": "S03OLCEFR", "collection_name": "s03olcefr"},
    ],
}

ENV_ARGS = FlowEnvArgs(owner_id="owner")
INPUT_PRODUCTS = [FlowInputProduct(name="S3OLCIL0_1", item_id="olcl0-item", collection_name="s03olcl0_")]
EXTERNAL_VARIABLES = {
    "start_datetime": "2026-10-01T10:00:00.000Z",
    "end_datetime": "2026-10-01T10:03:00.000Z",
    "satellite": "sentinel-3a",
}
PROPERTIES = {"product:type": "S03OLCL0_", "platform": "sentinel-3a"}
PUBLISHED_ITEMS = [
    {
        "id": "S03OLCEFR_item",
        "collection": "s03olcefr",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"product:type": "S03OLCEFR", "platform": "sentinel-3a"},
    },
]


def _flow_env():
    """A FlowEnv mock whose start_span can be used as a context manager."""
    flow_env = MagicMock()
    flow_env.start_span.return_value.__enter__.return_value = MagicMock()
    return flow_env


def _patch_process_any(mocker, settings=None, running_clusters=(PROCESSING_DASK_CLUSTER, "dask-staging")):
    """Mock the Prefect variable, the Dask clusters, the DPR flow call and the events of process-any."""
    mocker.patch.object(process_any_flow, "FlowEnv", return_value=_flow_env())
    variable_get = mocker.patch.object(
        process_any_flow.Variable,
        "get",
        new=AsyncMock(return_value=DEFAULT_SETTINGS if settings is None else settings),
    )
    mocker.patch.object(
        process_any_flow,
        "is_dask_cluster_running",
        new=AsyncMock(side_effect=lambda cluster: cluster in running_clusters),
    )
    call_dpr_flow = mocker.patch.object(process_any_flow, "call_dpr_flow", new=AsyncMock(return_value=PUBLISHED_ITEMS))
    emit_events = mocker.patch.object(process_any_flow, "emit_product_generated_events")
    return variable_get, call_dpr_flow, emit_events


async def _process_any(processing_id=ProcessingId.S3_OLCI_L1, workflow=WorkflowType.ON_DEMAND):
    """Run the process-any flow function with the test inputs."""
    return await process_any_flow.process_any.fn(
        env_args=ENV_ARGS,
        input_products=INPUT_PRODUCTS,
        external_variables=EXTERNAL_VARIABLES,
        processing_id=processing_id,
        workflow=workflow,
    )


def test_default_settings_schema_is_a_valid_json_schema():
    """The JSON schema of the default settings is itself valid, and accepts the settings documented."""
    schema = json.loads(process_any_flow.DEFAULT_SETTINGS_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(DEFAULT_SETTINGS)


@pytest.mark.parametrize("processing_id", list(ProcessingId))
@pytest.mark.parametrize("workflow", [WorkflowType.ON_DEMAND, WorkflowType.SYSTEMATIC])
async def test_process_any_runs_the_processor_with_the_default_settings(mocker, processing_id, workflow):
    """The processor is run with the settings of the Prefect variable, and the products events are emitted with
    the workflow of the processing."""
    variable_get, call_dpr_flow, emit_events = _patch_process_any(mocker)

    result = await _process_any(processing_id, workflow)

    assert result == PUBLISHED_ITEMS
    variable_get.assert_awaited_once_with(f"{processing_id.value}-default-setting")
    call_dpr_flow.assert_awaited_once_with(
        env=ENV_ARGS,
        input_products=INPUT_PRODUCTS,
        external_variables=EXTERNAL_VARIABLES,
        dask_cluster_label=PROCESSING_DASK_CLUSTER,
        processor_name="s3_l1olci",
        processor_version="1.0",
        pipeline="ol1_eo",
        unit=None,
        priority="high",
        processing_mode=["nrt"],
        workflow=workflow,
        generated_product_to_collection_identifier=DEFAULT_SETTINGS["generated_product_to_collection_identifier"],
        auxiliary_product_to_collection_identifier=DEFAULT_SETTINGS["auxiliary_product_to_collection_identifier"],
    )
    emit_events.assert_called_once_with("owner", PUBLISHED_ITEMS, workflow)


async def test_process_any_runs_a_unit(mocker):
    """A unit of the task table can be run instead of a pipeline."""
    settings = {key: value for key, value in DEFAULT_SETTINGS.items() if key != "pipeline"} | {"unit": "ol1_unit"}
    _, call_dpr_flow, _ = _patch_process_any(mocker, settings)

    await _process_any()

    assert call_dpr_flow.call_args.kwargs["pipeline"] is None
    assert call_dpr_flow.call_args.kwargs["unit"] == "ol1_unit"


async def test_process_any_fails_without_default_settings(mocker):
    """Without the Prefect variable of the processor, nothing is processed."""
    _, call_dpr_flow, _ = _patch_process_any(mocker)
    mocker.patch.object(process_any_flow.Variable, "get", new=AsyncMock(return_value=None))

    with pytest.raises(RuntimeError, match="'s3l1olci-default-setting' is missing"):
        await _process_any()

    call_dpr_flow.assert_not_awaited()


@pytest.mark.parametrize(
    "settings",
    [
        {key: value for key, value in DEFAULT_SETTINGS.items() if key != "processor"},
        {key: value for key, value in DEFAULT_SETTINGS.items() if key != "pipeline"},
        DEFAULT_SETTINGS | {"unit": "ol1_unit"},
        DEFAULT_SETTINGS | {"priority": "urgent"},
        DEFAULT_SETTINGS | {"processing_mode": []},
        DEFAULT_SETTINGS | {"unknown": "field"},
    ],
    ids=["no-processor", "no-pipeline-nor-unit", "pipeline-and-unit", "bad-priority", "no-mode", "unknown-field"],
)
async def test_process_any_rejects_invalid_default_settings(mocker, settings):
    """Default settings that don't match the JSON schema are rejected before any processing."""
    _, call_dpr_flow, _ = _patch_process_any(mocker, settings)

    with pytest.raises(ValueError, match="Invalid JSON schema .* 's3l1olci-default-setting'"):
        await _process_any()

    call_dpr_flow.assert_not_awaited()


@pytest.mark.parametrize(
    "running_clusters,missing_cluster",
    [(("dask-staging",), PROCESSING_DASK_CLUSTER), ((PROCESSING_DASK_CLUSTER,), "dask-staging")],
)
async def test_process_any_requires_running_dask_clusters(mocker, running_clusters, missing_cluster):
    """The processing and staging Dask clusters must be running."""
    _, call_dpr_flow, _ = _patch_process_any(mocker, running_clusters=running_clusters)

    with pytest.raises(ValueError, match=f"'{missing_cluster}' is unknown or not ready"):
        await _process_any()

    call_dpr_flow.assert_not_awaited()


@pytest.mark.parametrize(
    "force_systematic,workflow",
    [(False, WorkflowType.ON_DEMAND), (True, WorkflowType.SYSTEMATIC)],
)
async def test_process_catalog_product(mocker, force_systematic, workflow):
    """An on-demand processing reads the product from the catalog, and is systematic only when forced."""
    flow_env = _flow_env()
    mocker.patch.object(process_any_flow, "FlowEnv", return_value=flow_env)
    get_item = mocker.patch.object(
        process_any_flow,
        "get_single_catalog_item",
        new=AsyncMock(return_value=MagicMock(properties=PROPERTIES)),
    )
    process_product = AsyncMock(return_value=PUBLISHED_ITEMS)

    result = await process_any_flow.process_catalog_product(
        ENV_ARGS,
        FlowInputItem(item_id="olcl0-item", collection_name="s03olcl0_"),
        force_systematic,
        process_product,
    )

    assert result == PUBLISHED_ITEMS
    get_item.assert_awaited_once_with(flow_env, "olcl0-item", ["s03olcl0_"])
    process_product.assert_awaited_once_with(ENV_ARGS, "olcl0-item", "s03olcl0_", PROPERTIES, workflow)


async def test_process_catalog_product_fails_without_the_product(mocker):
    """The processing fails when the product is not in the catalog."""
    mocker.patch.object(process_any_flow, "FlowEnv", return_value=_flow_env())
    mocker.patch.object(process_any_flow, "get_single_catalog_item", new=AsyncMock(return_value=None))
    process_product = AsyncMock()

    with pytest.raises(RuntimeError, match="STAC item 'olcl0-item' not found in collection 's03olcl0_'"):
        await process_any_flow.process_catalog_product(
            ENV_ARGS,
            FlowInputItem(item_id="olcl0-item", collection_name="s03olcl0_"),
            False,
            process_product,
        )

    process_product.assert_not_awaited()


async def test_process_event_product(mocker):
    """A systematic processing processes the product of the event, on behalf of its owner."""
    mocker.patch.object(process_any_flow, "FlowEnv", return_value=_flow_env())
    process_product = AsyncMock(return_value=PUBLISHED_ITEMS)
    payload = {
        "owner_id": "event-owner",
        "product": {
            "id": "olcl0-item",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "collection": "s03olcl0_",
            "properties": PROPERTIES,
        },
    }

    result = await process_any_flow.process_event_product(payload, process_product)

    assert result == PUBLISHED_ITEMS
    process_product.assert_awaited_once_with(
        FlowEnvArgs(owner_id="event-owner"),
        "olcl0-item",
        "s03olcl0_",
        PROPERTIES,
        WorkflowType.SYSTEMATIC,
    )
