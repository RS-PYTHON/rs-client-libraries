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

from rs_workflows.flow_utils import FlowEnvArgs, WorkflowType
from rs_workflows.on_demand.sentinel1 import s1_ard
from rs_workflows.payload_generator import EDH_API_KEY_ENV_VAR
from rs_workflows.utils.dpr import ProcessingId
from rs_workflows.utils.zarr_utils import ZARR_MEDIA_TYPE
from tests.common import (
    DASK_ENV_VARS,
    PROCESSOR_SETTINGS,
    flow_env_mock,
    patch_dask_env_read_by_flow_env,
    processor_settings,
)

COLLECTIONS = s1_ard.DEFAULT_INPUT_COLLECTIONS
SLC_IW_1 = "S1C_IW_SLC__1SDV_20260408T174632_20260408T174659_007128_00E6F6_E998"
SLC_IW_2 = "S1C_IW_SLC__1SDV_20260327T174632_20260327T174659_006953_00E115_D167"
ENV_ARGS = FlowEnvArgs(owner_id="owner")
SETTINGS = processor_settings(processor={"name": "s1_ard", "version": "1.0"}, pipeline="SLC__1_NRB__1")


@pytest.fixture(autouse=True)
def edh_api_key(monkeypatch):
    """EarthDataHub API key, read by the FlowEnv from the Prefect blocks of the owner."""
    monkeypatch.setenv(EDH_API_KEY_ENV_VAR, "key")


def _item(item_id, mode="IW", collection_id="s01siwslc", href=None, media_type=ZARR_MEDIA_TYPE):
    """Catalog item of an SLC, in the Zarr format by default."""
    asset = MagicMock(href=href or f"s3://bucket/{collection_id}/{item_id}.zarr", media_type=media_type)
    return MagicMock(
        id=item_id,
        collection_id=collection_id,
        properties={"sar:instrument_mode": mode},
        assets={item_id: asset},
    )


def _items(*modes, items=None):
    """Catalog items of SLC_IW_1 and SLC_IW_2 with the given sar:instrument_mode values, or the given items."""
    item_collection = MagicMock()
    item_collection.items = items or [_item(slc, mode) for slc, mode in zip([SLC_IW_1, SLC_IW_2], modes)]
    item_collection.__len__.return_value = len(item_collection.items)
    return item_collection


def _patch_settings_and_catalog(mocker, item_collection, settings=SETTINGS):
    """Mock the settings and the catalog."""
    load_settings = mocker.patch.object(s1_ard, "load_processor_settings", new=AsyncMock(return_value=settings))
    get_items = mocker.patch.object(s1_ard, "get_catalog_items", new=AsyncMock(return_value=item_collection))
    return load_settings, get_items


def _patch(mocker, item_collection, settings=SETTINGS):
    """Mock the settings, the catalog, the flow environment and the DPR flow call."""
    mocker.patch.object(s1_ard, "get_run_logger", return_value=MagicMock())
    _patch_settings_and_catalog(mocker, item_collection, settings)
    mocker.patch.object(s1_ard, "FlowEnv", return_value=flow_env_mock())
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


@pytest.mark.parametrize(
    "slc,expected",
    [
        (SLC_IW_1, "s1-l1-iw-slc"),
        ("S1A_EW_SLC__1SDH_20260408T174632_20260408T174659_007128_00E6F6_E998", "s1-l1-ew-slc"),
        ("S1D_S3_SLC__1SDV_20260408T174632_20260408T174659_007128_00E6F6_E998", "s1-l1-sm-slc"),
    ],
)
def test_get_slc_collection_with_separators(slc, expected):
    """The collection names can have a separator before 'slc', e.g. the legacy SLC collections."""
    collections = ["s1-l1-ew-slc", "s1-l1-iw-slc", "s1-l1-sm-slc"]
    assert s1_ard._get_slc_collection(slc, collections) == expected


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
    load_settings, _ = _patch_settings_and_catalog(mocker, _items("IW"))
    await s1_ard.process_s1ard.fn(slcs, reference_date, ENV_ARGS)
    load_settings.assert_not_awaited()
    dpr_mock.assert_not_awaited()


@pytest.mark.parametrize(
    "item_collection",
    [None, _items(), _items("WV"), _items("IW", "EW")],
)
async def test_process_s1ard_rejects_bad_catalog_items(mocker, item_collection):
    """Missing items, invalid or different instrument modes prevent the processing."""
    dpr_mock = _patch(mocker, item_collection)
    await s1_ard.process_s1ard.fn([SLC_IW_1, SLC_IW_2], date(2026, 4, 8), ENV_ARGS)
    dpr_mock.assert_not_awaited()


async def test_process_s1ard_passes_the_slcs_not_in_zarr_to_the_payload_check(mocker):
    """An SLC found only in a legacy SAFE collection is passed to the DPR flow, that rejects it when it checks that the
    input products of a Zarr reader are in the Zarr format."""
    safe_item = _item(
        SLC_IW_1,
        collection_id="s1-l1-iw-slc",
        href=f"s3://bucket/s1-l1-iw-slc/{SLC_IW_1}",
        media_type=None,
    )
    dpr_mock = _patch(mocker, _items(items=[safe_item]), processor_settings(input_collections=["s1-l1-iw-slc"]))
    await s1_ard.process_s1ard_reference.fn(SLC_IW_1, ENV_ARGS)

    assert [p.collection_name for p in dpr_mock.call_args.kwargs["input_products"]] == ["s1-l1-iw-slc"]


async def test_process_s1ard_rejects_missing_slcs(mocker):
    """If an SLC is not found in the catalog, the processing is not launched."""
    dpr_mock = _patch(mocker, _items("IW"))
    await s1_ard.process_s1ard.fn([SLC_IW_1, SLC_IW_2], date(2026, 4, 8), ENV_ARGS)
    dpr_mock.assert_not_awaited()


async def test_process_s1ard_reads_the_zarr_slc_from_its_collection(mocker):
    """When an SLC is in a legacy SAFE collection and in a Zarr collection of the settings, with the same ID, the Zarr
    one is read from the collection where it was found, not from the first collection of its instrument mode."""
    items = [
        _item(SLC_IW_1, collection_id="s1-l1-iw-slc", href=f"s3://bucket/s1-l1-iw-slc/{SLC_IW_1}", media_type=None),
        _item(SLC_IW_1, collection_id="s01siwslc"),
    ]
    dpr_mock = _patch(
        mocker,
        _items(items=items),
        processor_settings(input_collections=["s1-l1-iw-slc", "s01siwslc"]),
    )
    await s1_ard.process_s1ard_reference.fn(SLC_IW_1, ENV_ARGS)

    dpr_mock.assert_awaited_once()
    assert [p.collection_name for p in dpr_mock.call_args.kwargs["input_products"]] == ["s01siwslc"]


async def test_process_s1ard_requires_the_edh_api_key(mocker, monkeypatch):
    """Without the EarthDataHub API key in the Prefect blocks of the owner, the processing is not launched."""
    monkeypatch.delenv(EDH_API_KEY_ENV_VAR)
    dpr_mock = _patch(mocker, _items("IW"))
    load_settings, _ = _patch_settings_and_catalog(mocker, _items("IW"))
    await s1_ard.process_s1ard_reference.fn(SLC_IW_1, ENV_ARGS)
    load_settings.assert_not_awaited()
    dpr_mock.assert_not_awaited()


async def test_process_s1ard_calls_dpr_flow(mocker):
    """The nominal flow reads its settings, then calls the DPR flow with the reference date and instrument mode."""
    dpr_mock = _patch(mocker, _items("IW", "IW"))
    load_settings, get_items = _patch_settings_and_catalog(mocker, _items("IW", "IW"))
    await s1_ard.process_s1ard.fn([SLC_IW_1, SLC_IW_2], date(2026, 4, 8), ENV_ARGS)

    load_settings.assert_awaited_once_with(ProcessingId.S1_ARD)
    get_items.assert_awaited_once()
    assert get_items.call_args.args[2] == COLLECTIONS
    dpr_mock.assert_awaited_once()
    assert dpr_mock.call_args.args == (ENV_ARGS,)
    kwargs = dpr_mock.call_args.kwargs
    assert kwargs["settings"] is SETTINGS
    assert kwargs["workflow"] == WorkflowType.ON_DEMAND
    assert kwargs["external_variables"] == {
        "instrument_mode": "IW",
        "reference_date": "2026-04-08",
    }
    assert [p.item_id for p in kwargs["input_products"]] == [SLC_IW_1, SLC_IW_2]
    assert {p.collection_name for p in kwargs["input_products"]} == {"s01siwslc"}
    # The S1 ARD processor reads the SLCs in the Zarr format: they are checked before launching it
    assert all(p.zarr for p in kwargs["input_products"])
    assert kwargs["pipeline"] == "SLC__1_NRB__1"
    assert kwargs["temporary_folder"] == s1_ard.TEMPORARY_FOLDER


async def test_process_s1ard_single_slc_nrb_uses_slc_date(mocker):
    """With a single SLC and no reference date, the NRB pipeline uses the SLC date as reference date."""
    dpr_mock = _patch(mocker, _items("IW"))
    await s1_ard.process_s1ard.fn([SLC_IW_1], None, ENV_ARGS)

    assert dpr_mock.call_args.kwargs["external_variables"] == {
        "instrument_mode": "IW",
        "reference_date": "2026-04-08",
    }


async def test_process_s1ard_single_slc_with_reference_date(mocker):
    """With a single SLC, the given reference date is useless but still used, with the given pipeline."""
    dpr_mock = _patch(mocker, _items("SM"))
    await s1_ard.process_s1ard.fn([SLC_IW_1], date(2026, 4, 1), ENV_ARGS, "SLC__1_GSL__1")

    assert dpr_mock.call_args.kwargs["external_variables"] == {
        "instrument_mode": "SM",
        "reference_date": "2026-04-01",
    }
    assert dpr_mock.call_args.kwargs["pipeline"] == "SLC__1_GSL__1"


async def test_process_s1ard_uses_the_input_collections_of_the_settings(mocker):
    """The SLCs are searched in the input collections of the settings, when given."""
    dpr_mock = _patch(mocker, _items("IW"))
    _, get_items = _patch_settings_and_catalog(
        mocker,
        _items("IW"),
        processor_settings(input_collections=["my-iw-slc"]),
    )
    await s1_ard.process_s1ard.fn([SLC_IW_1], date(2026, 4, 1), "", ENV_ARGS)

    assert get_items.call_args.args[2] == ["my-iw-slc"]
    assert {p.collection_name for p in dpr_mock.call_args.kwargs["input_products"]} == {"my-iw-slc"}


async def test_process_s1ard_reference(mocker):
    """The reference pipeline flow calls the DPR flow without reference date."""
    dpr_mock = _patch(mocker, _items("EW"))
    await s1_ard.process_s1ard_reference.fn(SLC_IW_1, ENV_ARGS)

    dpr_mock.assert_awaited_once()
    kwargs = dpr_mock.call_args.kwargs
    assert kwargs["external_variables"] == {"instrument_mode": "EW"}
    assert kwargs["pipeline"] == "ARD_REFERENCE_PIPELINE"


async def test_process_s1ard_reads_the_prefect_blocks_before_checking_the_dask_clusters(mocker, monkeypatch):
    """Non-regression: the FlowEnv, that reads the Dask gateway env vars from the Prefect blocks, is created before
    the settings are loaded and the Dask clusters are checked, instead of failing with
    KeyError('DASK_GATEWAY_ADDRESS')."""
    dpr_mock = _patch(mocker, _items("IW", "IW"))
    flow_env_class, gateway = patch_dask_env_read_by_flow_env(
        mocker,
        monkeypatch,
        s1_ard,
        PROCESSOR_SETTINGS | {"processor": {"name": "s1_ard", "version": "1.0"}, "pipeline": "SLC__1_NRB__1"},
    )

    await s1_ard.process_s1ard.fn([SLC_IW_1, SLC_IW_2], date(2026, 4, 8), ENV_ARGS)

    flow_env_class.assert_called_once_with(ENV_ARGS)
    assert gateway.call_args.kwargs["address"] == DASK_ENV_VARS["DASK_GATEWAY_ADDRESS"]
    dpr_mock.assert_awaited_once()
    assert dpr_mock.call_args.kwargs["settings"] == SETTINGS


async def test_process_s1ard_tasks(mocker):
    """The Prefect tasks delegate to the flow functions."""
    flow_mock = mocker.patch.object(s1_ard, "_do_process_s1ard", new=AsyncMock())
    await s1_ard.process_s1l1ard_task.fn([SLC_IW_1], None, ENV_ARGS)
    await s1_ard.process_s1l1ard_reference_task.fn(SLC_IW_1, ENV_ARGS)
    assert flow_mock.await_count == 2
