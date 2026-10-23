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

"""Unit tests for the on_demand Sentinel-1 ARD flows (sentinel1/s1_ard)."""

# pylint: disable=protected-access

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from rs_workflows.on_demand.sentinel1 import s1_ard

COLLECTIONS = ["s01sewslc", "s01siwslc", "s01ssmslc"]
SLC_IW_1 = "S1C_IW_SLC__1SDV_20260408T174632_20260408T174659_007128_00E6F6_E998"
SLC_IW_2 = "S1C_IW_SLC__1SDV_20260327T174632_20260327T174659_006953_00E115_D167"


def _resolved_params(pipeline="SLC__1_NRB__1"):
    """A resolved Level1ArdFlowParams-like object with the attributes used by the flows."""
    return MagicMock(
        owner_identifier="owner",
        dask_cluster_label="cluster",
        input_collections=COLLECTIONS,
        processor_name="s1_ard",
        processor_version="1.0",
        pipeline=pipeline,
        unit=None,
        priority=None,
        processing_mode=[],
        workflow=None,
        generated_product_to_collection_identifier=None,
        auxiliary_product_to_collection_identifier=None,
    )


def _flow_params(pipeline="SLC__1_NRB__1"):
    """A flow_params object whose resolve() returns the resolved params."""
    params = MagicMock()
    params.resolve = AsyncMock(return_value=_resolved_params(pipeline))
    return params


def _items(*modes):
    """Catalog items with the given sar:instrument_mode values."""
    item_collection = MagicMock()
    item_collection.items = [MagicMock(properties={"sar:instrument_mode": mode}) for mode in modes]
    item_collection.__len__.return_value = len(modes)
    return item_collection


def _patch(mocker, item_collection):
    """Mock the catalog, the flow environment and the DPR flow call."""
    mocker.patch.object(s1_ard, "get_run_logger", return_value=MagicMock())
    flow_env = MagicMock()
    flow_env.start_span.return_value.__enter__.return_value = MagicMock()
    mocker.patch.object(s1_ard, "FlowEnv", return_value=flow_env)
    mocker.patch.object(s1_ard, "get_catalog_items", new=AsyncMock(return_value=item_collection))
    return mocker.patch.object(s1_ard, "call_dpr_flow", new=AsyncMock())


@pytest.mark.parametrize(
    "slc,expected",
    [
        (SLC_IW_1, "s01siwslc"),
        ("S1A_EW_SLC__1SDH_20260408T174632_20260408T174659_007128_00E6F6_E998", "s01sewslc"),
        ("S1D_S3_SLC__1SDV_20260408T174632_20260408T174659_007128_00E6F6_E998", "s01ssmslc"),
        ("UNKNOWN_PRODUCT", "s01sewslc"),
    ],
)
def test_get_slc_collection(slc, expected):
    """The collection is chosen from the instrument mode in the product name, else the first one."""
    assert s1_ard._get_slc_collection(slc, COLLECTIONS) == expected


def test_get_slc_collection_falls_back_on_first_collection():
    """When no collection matches the instrument mode, the first collection is used."""
    assert s1_ard._get_slc_collection(SLC_IW_1, ["my_collection"]) == "my_collection"


@pytest.mark.parametrize(
    "slcs,reference_date",
    [([], None), ([SLC_IW_1, SLC_IW_2], None)],
)
async def test_process_s1ard_rejects_bad_inputs(mocker, slcs, reference_date):
    """Without SLC, or with several SLCs but no reference date, the processing is not launched."""
    dpr_mock = _patch(mocker, _items("IW"))
    flow_params = _flow_params()
    await s1_ard.process_s1ard.fn(slcs, reference_date, "key", flow_params)
    flow_params.resolve.assert_not_awaited()
    dpr_mock.assert_not_awaited()


@pytest.mark.parametrize(
    "item_collection",
    [None, _items(), _items("WV"), _items("IW", "EW")],
)
async def test_process_s1ard_rejects_bad_catalog_items(mocker, item_collection):
    """Missing items, invalid or different instrument modes prevent the processing."""
    dpr_mock = _patch(mocker, item_collection)
    await s1_ard.process_s1ard.fn([SLC_IW_1, SLC_IW_2], date(2026, 4, 8), "key", _flow_params())
    dpr_mock.assert_not_awaited()


async def test_process_s1ard_calls_dpr_flow(mocker):
    """The nominal flow calls the DPR flow with the reference date and instrument mode."""
    dpr_mock = _patch(mocker, _items("IW", "IW"))
    await s1_ard.process_s1ard.fn([SLC_IW_1, SLC_IW_2], date(2026, 4, 8), "key", _flow_params())

    dpr_mock.assert_awaited_once()
    kwargs = dpr_mock.call_args.kwargs
    assert kwargs["external_variables"] == {
        "instrument_mode": "IW",
        "reference_date": "2026-04-08",
        "edh_api_key": "key",
    }
    assert [p.item_id for p in kwargs["input_products"]] == [SLC_IW_1, SLC_IW_2]
    assert {p.collection_name for p in kwargs["input_products"]} == {"s01siwslc"}
    assert kwargs["pipeline"] == "SLC__1_NRB__1"
    assert kwargs["temporary_folder"] == s1_ard.TEMPORARY_FOLDER


async def test_process_s1ard_single_slc_nrb_uses_slc_date(mocker):
    """With a single SLC and no reference date, the NRB pipeline uses the SLC date as reference date."""
    dpr_mock = _patch(mocker, _items("IW"))
    await s1_ard.process_s1ard.fn([SLC_IW_1], None, "", _flow_params())

    assert dpr_mock.call_args.kwargs["external_variables"] == {
        "instrument_mode": "IW",
        "reference_date": "2026-04-08",
    }


async def test_process_s1ard_single_slc_with_reference_date(mocker):
    """With a single SLC, the given reference date is useless but still used."""
    dpr_mock = _patch(mocker, _items("SM"))
    await s1_ard.process_s1ard.fn([SLC_IW_1], date(2026, 4, 1), "", _flow_params("SLC__1_GSL__1"))

    assert dpr_mock.call_args.kwargs["external_variables"] == {
        "instrument_mode": "SM",
        "reference_date": "2026-04-01",
    }


async def test_process_s1ard_reference(mocker):
    """The reference pipeline flow calls the DPR flow without reference date."""
    dpr_mock = _patch(mocker, _items("EW"))
    await s1_ard.process_s1ard_reference.fn(SLC_IW_1, "key", _flow_params("ARD_REFERENCE_PIPELINE"))

    dpr_mock.assert_awaited_once()
    kwargs = dpr_mock.call_args.kwargs
    assert kwargs["external_variables"] == {"instrument_mode": "EW", "edh_api_key": "key"}
    assert kwargs["pipeline"] == "ARD_REFERENCE_PIPELINE"


async def test_process_s1ard_tasks(mocker):
    """The Prefect tasks delegate to the flow functions."""
    flow_mock = mocker.patch.object(s1_ard, "_do_process_s1ard", new=AsyncMock())
    await s1_ard.process_s1l1ard_task.fn([SLC_IW_1], None, "key", "params")
    await s1_ard.process_s1l1ard_reference_task.fn(SLC_IW_1, "key", "params")
    assert flow_mock.await_count == 2


async def test_level1ard_flow_params_resolve(mocker):
    """Level1ArdFlowParams resolves its specific settings from the 1ard prefect variable."""
    resolve_mock = mocker.patch.object(s1_ard.ProcessingFlowParams, "_resolve", new=AsyncMock(return_value="ok"))
    params = s1_ard.Level1ArdFlowParams()
    assert await params.resolve("1") == "ok"
    resolve_mock.assert_awaited_once_with("1", "1ard")
    assert params._resolve_specific({}) == {"input_collections": COLLECTIONS}
