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

"""Unit tests for the on-demand and systematic Sentinel-3 OLCI processing flows."""

from datetime import datetime, timezone
from functools import partial
from unittest.mock import AsyncMock, MagicMock

import pytest

from rs_workflows import process_any_flow
from rs_workflows.flow_utils import FlowEnvArgs, FlowInputProduct, WorkflowType
from rs_workflows.on_demand.common import events
from rs_workflows.on_demand.sentinel3 import (
    process_s3l1olci_ondemand_flow,
    process_s3l2olci_ondemand_flow,
    s3_processing_utils,
)
from rs_workflows.process_any_flow import ProcessingId
from rs_workflows.systematic import (
    process_s3l1olci_systematic_flow,
    process_s3l2olci_systematic_flow,
)

ENV_ARGS = FlowEnvArgs(owner_id="owner")
OLCL0_PROPERTIES = {
    "product:type": "S03OLCL0_",
    "platform": "sentinel-3a",
    "start_datetime": "2026-10-01T10:00:00+00:00",
    "end_datetime": "2026-10-01T10:03:00+00:00",
}
OLCEFR_PROPERTIES = OLCL0_PROPERTIES | {"product:type": "S03OLCEFR"}
GENERATED_PRODUCTS = [{"id": "generated-item", "collection": "generated-collection"}]
DEFAULT_SETTINGS = {
    "processor": {"name": "s3_l1olci", "version": "1.0"},
    "pipeline": "ol1_eo",
    "priority": "low",
    "processing_mode": ["always"],
    "dask_cluster_name": "dask-s3-olci",
    "auxiliary_product_to_collection_identifier": [],
    "generated_product_to_collection_identifier": [],
}


def _flow_env():
    """A FlowEnv mock whose start_span can be used as a context manager."""
    flow_env = MagicMock()
    flow_env.start_span.return_value.__enter__.return_value = MagicMock()
    return flow_env


def _stac_item(product_type, item_id, properties):
    """A published STAC item."""
    return {
        "id": item_id,
        "collection": product_type.lower(),
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": properties,
    }


def _patch_process_any(mocker, published_items):
    """Run the real process-any, with its settings, Dask clusters and DPR processing mocked."""
    mocker.patch.object(process_any_flow, "FlowEnv", return_value=_flow_env())
    mocker.patch.object(s3_processing_utils, "process_any", new=process_any_flow.process_any.fn)
    mocker.patch.object(process_any_flow.Variable, "get", new=AsyncMock(return_value=DEFAULT_SETTINGS))
    mocker.patch.object(process_any_flow, "is_dask_cluster_running", new=AsyncMock(return_value=True))
    mocker.patch.object(s3_processing_utils, "emit_olci_quicklook_event")
    mocker.patch.object(process_s3l1olci_ondemand_flow, "get_latest_product_covering_interval", return_value=[])
    return mocker.patch.object(process_any_flow, "call_dpr_flow", new=AsyncMock(return_value=published_items))


# --------------------------------------------------------------------------- #
# Code shared by the OLCI flows
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("level,processing_id", [("l1", ProcessingId.S3_OLCI_L1), ("l2", ProcessingId.S3_OLCI_L2)])
async def test_process_olci(mocker, level, processing_id):
    """The OLCI processing is run with process-any, then the quicklooks of the generated products are requested."""
    input_products = [FlowInputProduct(name="INPUT", item_id="item", collection_name="collection")]
    process_any = mocker.patch.object(
        s3_processing_utils,
        "process_any",
        new=AsyncMock(return_value=GENERATED_PRODUCTS),
    )
    emit_quicklook_event = mocker.patch.object(s3_processing_utils, "emit_olci_quicklook_event")

    result = await s3_processing_utils.process_olci(
        level,
        ENV_ARGS,
        input_products,
        {"start_datetime": "start"},
        WorkflowType.ON_DEMAND,
    )

    assert result == GENERATED_PRODUCTS
    process_any.assert_awaited_once_with(
        env_args=ENV_ARGS,
        input_products=input_products,
        external_variables={"start_datetime": "start"},
        processing_id=processing_id,
        workflow=WorkflowType.ON_DEMAND,
    )
    emit_quicklook_event.assert_called_once_with(level, "owner", GENERATED_PRODUCTS)


# --------------------------------------------------------------------------- #
# Sentinel-3 OLCI Level-1
# --------------------------------------------------------------------------- #
def test_flow_input_s3l1olci():
    """The input product of the on-demand flow is in the S03OLCL0_ collection by default."""
    single_input_product = process_s3l1olci_ondemand_flow.FlowInputS3l1olci(item_id="olcl0-item")
    assert single_input_product.model_dump() == {"item_id": "olcl0-item", "collection_name": "s03olcl0_"}


async def test_get_latest_product_covering_interval(mocker):
    """The catalog is searched for the latest product of the satellite included in the interval."""
    found = [MagicMock(id="found-item")]
    catalog_search = mocker.patch.object(
        process_s3l1olci_ondemand_flow,
        "catalog_search_task",
        new=AsyncMock(return_value=found),
    )

    result = await process_s3l1olci_ondemand_flow.get_latest_product_covering_interval(
        ENV_ARGS,
        datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 10, 1, 10, 3, tzinfo=timezone.utc),
        "S03OLCL0_",
        "sentinel-3a",
        "s03olcl0_",
    )

    assert result == found
    catalog_search.assert_awaited_once_with(
        ENV_ARGS,
        {
            "filter": {
                "op": "and",
                "args": [
                    {"op": "=", "args": [{"property": "product:type"}, "S03OLCL0_"]},
                    {"op": "=", "args": [{"property": "platform"}, "sentinel-3a"]},
                    {
                        "op": "t_contains",
                        "args": [
                            {"interval": [{"property": "start_datetime"}, {"property": "end_datetime"}]},
                            {
                                "interval": [
                                    {"op": "+", "args": ["2026-10-01T10:00:00.000Z", "2"]},
                                    {"op": "-", "args": ["2026-10-01T10:03:00.000Z", "2"]},
                                ],
                            },
                        ],
                    },
                ],
            },
            "sortby": [{"field": "created", "direction": "desc"}],
            "limit": 1,
        },
        collections=["s03olcl0_"],
    )


@pytest.mark.parametrize(
    "collection_name,natl0_collection",
    [("s03olcl0_", "s03natl0_"), ("s3-l0", "s3-l0")],
)
@pytest.mark.parametrize("next_found", [True, False])
@pytest.mark.parametrize("previous_found", [True, False])
@pytest.mark.parametrize("natl0_found", [True, False])
async def test_compute_s3l1olci_inputs(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    mocker,
    collection_name,
    natl0_collection,
    next_found,
    previous_found,
    natl0_found,
):
    """The next and previous S03OLCL0_ products and the S03NATL0_ product are searched around the given product."""
    found = {
        ("S03OLCL0_", datetime.fromisoformat("2026-10-01T10:03:00+00:00")): "next-item" if next_found else None,
        ("S03OLCL0_", datetime.fromisoformat("2026-10-01T09:57:00+00:00")): "previous-item" if previous_found else None,
        ("S03NATL0_", datetime.fromisoformat("2026-10-01T10:00:00+00:00")): "natl0-item" if natl0_found else None,
    }

    async def search(env_args, begin, end, product_type, satellite, collection):
        """Return the product found in the catalog, as an ItemCollection-like list."""
        assert env_args is ENV_ARGS
        assert (end - begin).total_seconds() == 180
        assert satellite == "sentinel-3a"
        assert collection == (natl0_collection if product_type == "S03NATL0_" else collection_name)
        item_id = found[(product_type, begin)]
        return [MagicMock(id=item_id)] if item_id else []

    search_mock = mocker.patch.object(
        process_s3l1olci_ondemand_flow,
        "get_latest_product_covering_interval",
        side_effect=search,
    )

    input_products = await process_s3l1olci_ondemand_flow.compute_s3l1olci_inputs(
        ENV_ARGS,
        "olcl0-item",
        collection_name,
        "sentinel-3a",
        OLCL0_PROPERTIES["start_datetime"],
        OLCL0_PROPERTIES["end_datetime"],
        "OLCI_",
        "NAV_",
    )

    assert search_mock.call_count == 3
    expected = [("OLCI_1", "olcl0-item", collection_name)]
    if next_found:
        expected.append((f"OLCI_{len(expected) + 1}", "next-item", collection_name))
    if previous_found:
        expected.append((f"OLCI_{len(expected) + 1}", "previous-item", collection_name))
    if natl0_found:
        expected.append(("NAV_1", "natl0-item", natl0_collection))
    assert input_products == [
        FlowInputProduct(name=name, item_id=item_id, collection_name=collection)
        for name, item_id, collection in expected
    ]


async def test_process_s3l1olci_product(mocker):
    """The inputs are computed around the product, and its dates and satellite are the external variables."""
    input_products = [FlowInputProduct(name="S3OLCIL0_1", item_id="olcl0-item", collection_name="s03olcl0_")]
    compute_inputs = mocker.patch.object(
        process_s3l1olci_ondemand_flow,
        "compute_s3l1olci_inputs",
        new=AsyncMock(return_value=input_products),
    )
    process_olci = mocker.patch.object(
        process_s3l1olci_ondemand_flow,
        "process_olci",
        new=AsyncMock(return_value=GENERATED_PRODUCTS),
    )

    result = await process_s3l1olci_ondemand_flow.process_s3l1olci_product(
        ENV_ARGS,
        "olcl0-item",
        "s03olcl0_",
        OLCL0_PROPERTIES,
        WorkflowType.SYSTEMATIC,
    )

    assert result == GENERATED_PRODUCTS
    compute_inputs.assert_awaited_once_with(
        ENV_ARGS,
        "olcl0-item",
        "s03olcl0_",
        "sentinel-3a",
        OLCL0_PROPERTIES["start_datetime"],
        OLCL0_PROPERTIES["end_datetime"],
        "S3OLCIL0_",
        "S3NAVL0_",
    )
    process_olci.assert_awaited_once_with(
        "l1",
        ENV_ARGS,
        input_products,
        {
            "start_datetime": OLCL0_PROPERTIES["start_datetime"],
            "end_datetime": OLCL0_PROPERTIES["end_datetime"],
            "satellite": "sentinel-3a",
        },
        WorkflowType.SYSTEMATIC,
    )


@pytest.mark.parametrize("force_systematic", [False, True])
async def test_process_s3l1olci_ondemand(mocker, force_systematic):
    """The on-demand flow processes the given product read from the catalog, with the given task table names."""
    process_catalog_product = mocker.patch.object(
        process_s3l1olci_ondemand_flow,
        "process_catalog_product",
        new=AsyncMock(return_value=GENERATED_PRODUCTS),
    )
    single_input_product = process_s3l1olci_ondemand_flow.FlowInputS3l1olci(item_id="olcl0-item")

    result = await process_s3l1olci_ondemand_flow.process_s3l1olci_ondemand.fn(
        ENV_ARGS,
        single_input_product,
        force_systematic,
        "OLCI_",
        "NAV_",
    )

    assert result == GENERATED_PRODUCTS
    args = process_catalog_product.call_args.args
    assert args[:3] == (ENV_ARGS, single_input_product, force_systematic)
    assert isinstance(args[3], partial)
    assert args[3].func is process_s3l1olci_ondemand_flow.process_s3l1olci_product
    assert args[3].keywords == {"olcil0_base_name": "OLCI_", "navl0_base_name": "NAV_"}


async def test_process_s3l1olci_systematic(mocker):
    """The systematic flow processes the S03OLCL0_ product of the event that started it."""
    process_event_product = mocker.patch.object(
        process_s3l1olci_systematic_flow,
        "process_event_product",
        new=AsyncMock(return_value=GENERATED_PRODUCTS),
    )
    payload = {"owner_id": "owner", "product": _stac_item("S03OLCL0_", "olcl0-item", OLCL0_PROPERTIES)}

    result = await process_s3l1olci_systematic_flow.process_s3l1olci_systematic.fn(payload)

    assert result == GENERATED_PRODUCTS
    process_event_product.assert_awaited_once_with(payload, process_s3l1olci_ondemand_flow.process_s3l1olci_product)


# --------------------------------------------------------------------------- #
# Sentinel-3 OLCI Level-2
# --------------------------------------------------------------------------- #
def test_flow_input_s3l2olci():
    """The input product of the on-demand flow is in the S03OLCEFR collection by default."""
    single_input_product = process_s3l2olci_ondemand_flow.FlowInputS3l2olci(item_id="olcefr-item")
    assert single_input_product.model_dump() == {"item_id": "olcefr-item", "collection_name": "s03olcefr"}


async def test_process_s3l2olci_product(mocker):
    """The S03OLCEFR product is the single input, and its dates are the external variables."""
    process_olci = mocker.patch.object(
        process_s3l2olci_ondemand_flow,
        "process_olci",
        new=AsyncMock(return_value=GENERATED_PRODUCTS),
    )

    result = await process_s3l2olci_ondemand_flow.process_s3l2olci_product(
        ENV_ARGS,
        "olcefr-item",
        "s03olcefr",
        OLCEFR_PROPERTIES,
        WorkflowType.ON_DEMAND,
        "OLCEFR",
    )

    assert result == GENERATED_PRODUCTS
    process_olci.assert_awaited_once_with(
        "l2",
        ENV_ARGS,
        [FlowInputProduct(name="OLCEFR", item_id="olcefr-item", collection_name="s03olcefr")],
        {"start_datetime": OLCEFR_PROPERTIES["start_datetime"], "end_datetime": OLCEFR_PROPERTIES["end_datetime"]},
        WorkflowType.ON_DEMAND,
    )


@pytest.mark.parametrize("force_systematic", [False, True])
async def test_process_s3l2olci_ondemand(mocker, force_systematic):
    """The on-demand flow processes the given product read from the catalog, with the given task table name."""
    process_catalog_product = mocker.patch.object(
        process_s3l2olci_ondemand_flow,
        "process_catalog_product",
        new=AsyncMock(return_value=GENERATED_PRODUCTS),
    )
    single_input_product = process_s3l2olci_ondemand_flow.FlowInputS3l2olci(item_id="olcefr-item")

    result = await process_s3l2olci_ondemand_flow.process_s3l2olci_ondemand.fn(
        ENV_ARGS,
        single_input_product,
        force_systematic,
        "OLCEFR",
    )

    assert result == GENERATED_PRODUCTS
    args = process_catalog_product.call_args.args
    assert args[:3] == (ENV_ARGS, single_input_product, force_systematic)
    assert isinstance(args[3], partial)
    assert args[3].func is process_s3l2olci_ondemand_flow.process_s3l2olci_product
    assert args[3].keywords == {"olcefr_base_name": "OLCEFR"}


async def test_process_s3l2olci_systematic(mocker):
    """The systematic flow processes the S03OLCEFR product of the event that started it."""
    process_event_product = mocker.patch.object(
        process_s3l2olci_systematic_flow,
        "process_event_product",
        new=AsyncMock(return_value=GENERATED_PRODUCTS),
    )
    payload = {"owner_id": "owner", "product": _stac_item("S03OLCEFR", "olcefr-item", OLCEFR_PROPERTIES)}

    result = await process_s3l2olci_systematic_flow.process_s3l2olci_systematic.fn(payload)

    assert result == GENERATED_PRODUCTS
    process_event_product.assert_awaited_once_with(payload, process_s3l2olci_ondemand_flow.process_s3l2olci_product)


# --------------------------------------------------------------------------- #
# Chain of the flows
# --------------------------------------------------------------------------- #
async def test_systematic_flows_accept_the_product_events(mocker):
    """The payload of the product events emitted by a processing starts the systematic flow of the next one."""
    emit_event = mocker.patch.object(events, "emit_event")
    events.emit_product_generated_events(
        "owner",
        [
            _stac_item("S03OLCL0_", "olcl0-item", OLCL0_PROPERTIES),
            _stac_item("S03OLCEFR", "olcefr-item", OLCEFR_PROPERTIES),
        ],
        WorkflowType.SYSTEMATIC,
    )
    olcl0_payload, olcefr_payload = (call.kwargs["payload"] for call in emit_event.call_args_list)
    call_dpr_flow = _patch_process_any(mocker, [])

    await process_s3l1olci_systematic_flow.process_s3l1olci_systematic.fn(olcl0_payload)
    await process_s3l2olci_systematic_flow.process_s3l2olci_systematic.fn(olcefr_payload)

    l1_call, l2_call = (call.kwargs for call in call_dpr_flow.call_args_list)
    assert l1_call["input_products"] == [
        FlowInputProduct(name="S3OLCIL0_1", item_id="olcl0-item", collection_name="s03olcl0_"),
    ]
    assert l1_call["external_variables"]["satellite"] == "sentinel-3a"
    assert l2_call["input_products"] == [
        FlowInputProduct(name="S03OLCEFR", item_id="olcefr-item", collection_name="s03olcefr"),
    ]
    for dpr_call in (l1_call, l2_call):
        assert dpr_call["env"] == FlowEnvArgs(owner_id="owner")
        assert dpr_call["workflow"] == WorkflowType.SYSTEMATIC


@pytest.mark.parametrize(
    "force_systematic,workflow",
    [(False, "on-demand"), (True, "systematic")],
    ids=["only-l1", "l1-and-l2"],
)
async def test_ondemand_flow_starts_the_next_processing_only_when_forced(mocker, force_systematic, workflow):
    """An on-demand L1 run emits the events of its products with its workflow: only a run with force_systematic
    emits events of a systematic workflow, that the automations use to start the L2 processing."""
    mocker.patch.object(
        process_any_flow,
        "get_single_catalog_item",
        new=AsyncMock(return_value=MagicMock(properties=OLCL0_PROPERTIES)),
    )
    call_dpr_flow = _patch_process_any(mocker, [_stac_item("S03OLCEFR", "olcefr-item", OLCEFR_PROPERTIES)])
    emit_event = mocker.patch.object(events, "emit_event")

    await process_s3l1olci_ondemand_flow.process_s3l1olci_ondemand.fn(
        ENV_ARGS,
        process_s3l1olci_ondemand_flow.FlowInputS3l1olci(item_id="olcl0-item"),
        force_systematic,
    )

    assert call_dpr_flow.call_args.kwargs["workflow"] == workflow
    emit_event.assert_called_once()
    assert emit_event.call_args.kwargs["event"] == "product.S03OLCEFR.generated"
    assert {"prefect.resource.id": workflow, "prefect.resource.role": "workflow"} in emit_event.call_args.kwargs[
        "related"
    ]
