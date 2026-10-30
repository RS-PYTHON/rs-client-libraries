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

"""Unit tests for the processing of a single product (rs_workflows.on_demand.common.product_processing)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from rs_workflows.flow_utils import (
    FlowEnvArgs,
    FlowEnvProcess,
    WorkflowType,
    WriteEngine,
)
from rs_workflows.on_demand.common import product_processing
from rs_workflows.on_demand.common.product_processing import FlowInputItem
from rs_workflows.utils.dpr import ProcessingId
from tests.common import flow_env_mock, processor_settings

ENV_ARGS = FlowEnvArgs(owner_id="owner")
PROPERTIES = {"product:type": "S03OLCL0_", "platform": "sentinel-3a"}
PUBLISHED_ITEMS = [{"id": "S03OLCEFR_item", "properties": {"product:type": "S03OLCEFR"}}]
SETTINGS = processor_settings()


@pytest.mark.parametrize(
    "force_systematic,workflow",
    [(False, WorkflowType.ON_DEMAND), (True, WorkflowType.SYSTEMATIC)],
)
def test_workflow_of_run(force_systematic, workflow):
    """A run is on-demand, unless the systematic processing is forced."""
    assert WorkflowType.of_run(force_systematic) == workflow


def test_flow_env_process_defaults_and_nesting():
    """The process options have defaults, and are parsed from the nested `process` of the flow environment."""
    assert FlowEnvArgs(owner_id="owner").process == FlowEnvProcess()
    assert FlowEnvProcess().force_systematic is False
    assert FlowEnvProcess().verbose is False
    args = FlowEnvArgs.model_validate(
        {
            "owner_id": "owner",
            "process": {
                "write_engine": "fast_zarr",
                "use_obstore": True,
                "triggering__stage_s3_upload_max_concurrency": 4,
                "force_systematic": True,
            },
        },
    )
    assert args.process.write_engine == WriteEngine.FAST_ZARR
    assert args.process.use_obstore is True
    assert args.process.triggering__stage_s3_upload_max_concurrency == 4
    assert args.process.force_systematic is True


def _patch(mocker, item):
    """Mock the settings, the flow environment and the catalog."""
    load_settings = mocker.patch.object(
        product_processing,
        "load_processor_settings",
        new=AsyncMock(return_value=SETTINGS),
    )
    flow_env = flow_env_mock()
    mocker.patch.object(product_processing, "FlowEnv", return_value=flow_env)
    get_item = mocker.patch.object(product_processing, "get_single_catalog_item", new=AsyncMock(return_value=item))
    return load_settings, flow_env, get_item


@pytest.mark.parametrize(
    "force_systematic,workflow",
    [(False, WorkflowType.ON_DEMAND), (True, WorkflowType.SYSTEMATIC)],
)
async def test_process_catalog_product(mocker, force_systematic, workflow):
    """An on-demand processing reads its settings, then the product from the catalog, and is systematic only when
    forced."""
    load_settings, flow_env, get_item = _patch(mocker, MagicMock(properties=PROPERTIES))
    process_product = AsyncMock(return_value=PUBLISHED_ITEMS)

    env_args = FlowEnvArgs(owner_id="owner", process=FlowEnvProcess(force_systematic=force_systematic))

    result = await product_processing.process_catalog_product(
        env_args,
        FlowInputItem(item_id="olcl0-item", collection_name="s03olcl0_"),
        ProcessingId.S3_OLCI_L1,
        process_product,
    )

    assert result == PUBLISHED_ITEMS
    load_settings.assert_awaited_once_with(ProcessingId.S3_OLCI_L1)
    get_item.assert_awaited_once_with(flow_env, "olcl0-item", ["s03olcl0_"])
    process_product.assert_awaited_once_with(env_args, SETTINGS, "olcl0-item", "s03olcl0_", PROPERTIES, workflow)


async def test_process_catalog_product_checks_the_settings_first(mocker):
    """Invalid settings or Dask clusters are reported before searching the product in the catalog."""
    _, _, get_item = _patch(mocker, MagicMock(properties=PROPERTIES))
    mocker.patch.object(
        product_processing,
        "load_processor_settings",
        new=AsyncMock(side_effect=ValueError("'dask-staging' is unknown or not ready.")),
    )

    with pytest.raises(ValueError, match="'dask-staging' is unknown or not ready"):
        await product_processing.process_catalog_product(
            ENV_ARGS,
            FlowInputItem(item_id="olcl0-item", collection_name="s03olcl0_"),
            ProcessingId.S3_OLCI_L1,
            AsyncMock(),
        )

    get_item.assert_not_awaited()


async def test_process_catalog_product_fails_without_the_product(mocker):
    """The processing fails when the product is not in the catalog."""
    _patch(mocker, None)
    process_product = AsyncMock()

    with pytest.raises(RuntimeError, match="STAC item 'olcl0-item' not found in collection 's03olcl0_'"):
        await product_processing.process_catalog_product(
            ENV_ARGS,
            FlowInputItem(item_id="olcl0-item", collection_name="s03olcl0_"),
            ProcessingId.S3_OLCI_L1,
            process_product,
        )

    process_product.assert_not_awaited()


async def test_process_event_product(mocker):
    """A systematic processing processes the product of the event, on behalf of its owner."""
    load_settings, _, _ = _patch(mocker, None)
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

    result = await product_processing.process_event_product(payload, ProcessingId.S3_OLCI_L2, process_product)

    assert result == PUBLISHED_ITEMS
    load_settings.assert_awaited_once_with(ProcessingId.S3_OLCI_L2)
    process_product.assert_awaited_once_with(
        FlowEnvArgs(owner_id="event-owner"),
        SETTINGS,
        "olcl0-item",
        "s03olcl0_",
        PROPERTIES,
        WorkflowType.SYSTEMATIC,
    )
