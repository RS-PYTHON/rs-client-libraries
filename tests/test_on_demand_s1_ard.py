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

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pystac import Item

from rs_workflows.flow_utils import FlowEnvArgs, WorkflowType
from rs_workflows.on_demand.sentinel1 import s1_ard
from rs_workflows.payload_generator import EDH_API_KEY_ENV_VAR
from rs_workflows.utils.dpr import ProcessingId
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
REFERENCE_DB_UUID = "477a37db-4648-4639-b571-3ad9dd50e000"
REFERENCE_DB_PATH = f"s3://my-bucket/owner/s1-ard-reference-db/{REFERENCE_DB_UUID}"


@pytest.fixture(autouse=True)
def edh_api_key(monkeypatch):
    """EarthDataHub API key, read by the FlowEnv from the Prefect blocks of the owner."""
    monkeypatch.setenv(EDH_API_KEY_ENV_VAR, "key")


def _items(*modes):
    """Catalog items with the given sar:instrument_mode values."""
    item_collection = MagicMock()
    item_collection.items = [MagicMock(properties={"sar:instrument_mode": mode}) for mode in modes]
    item_collection.__len__.return_value = len(modes)
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
    flow_env = flow_env_mock()
    mocker.patch.object(s1_ard, "FlowEnv", return_value=flow_env)
    await s1_ard.process_s1ard.fn([SLC_IW_1, SLC_IW_2], date(2026, 4, 8), ENV_ARGS)

    load_settings.assert_awaited_once_with(ProcessingId.S1_ARD, flow_env)
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


def _patch_reference_db(mocker):
    """Mock the check of the REFERENCE_DB collection, the allocation of its folder and its publication."""
    check = mocker.patch.object(s1_ard.catalog_flow, "check_collections_exist", new=AsyncMock())
    mocker.patch.object(s1_ard, "_allocate_reference_db_path", return_value=REFERENCE_DB_PATH)
    publish = mocker.patch.object(s1_ard, "_publish_reference_db", new=AsyncMock())
    return check, publish


async def test_process_s1ard_reference(mocker):
    """The reference pipeline flow calls the DPR flow without reference date, gives it the folder of the
    REFERENCE_DB, then publishes the REFERENCE_DB."""
    item_collection = _items("EW")
    dpr_mock = _patch(mocker, item_collection)
    check, publish = _patch_reference_db(mocker)
    # The generic flow reads the folder of the REFERENCE_DB from the context variable
    dpr_mock.side_effect = lambda *args, **kwargs: seen.append(s1_ard.REFERENCE_DB_PATH.get())
    seen: list[str | None] = []

    await s1_ard.process_s1ard_reference.fn(SLC_IW_1, ENV_ARGS)

    dpr_mock.assert_awaited_once()
    kwargs = dpr_mock.call_args.kwargs
    assert kwargs["external_variables"] == {"instrument_mode": "EW"}
    assert kwargs["pipeline"] == "ARD_REFERENCE_PIPELINE"
    check.assert_awaited_once()
    assert check.call_args.args[1] == [s1_ard.REFERENCE_DB_COLLECTION]
    assert seen == [REFERENCE_DB_PATH]
    assert s1_ard.REFERENCE_DB_PATH.get() is None
    publish.assert_awaited_once()
    assert publish.call_args.args[1:] == (REFERENCE_DB_PATH, item_collection.items[0])


async def test_process_s1ard_reference_not_published_if_the_processing_fails(mocker):
    """When the processing fails, the REFERENCE_DB is not published, and the context variable is reset."""
    dpr_mock = _patch(mocker, _items("IW"))
    dpr_mock.side_effect = RuntimeError("processing failed")
    _, publish = _patch_reference_db(mocker)

    with pytest.raises(RuntimeError, match="processing failed"):
        await s1_ard.process_s1ard_reference.fn(SLC_IW_1, ENV_ARGS)

    publish.assert_not_awaited()
    assert s1_ard.REFERENCE_DB_PATH.get() is None


async def test_process_s1ard_nominal_has_no_reference_db(mocker):
    """The nominal flow gives no REFERENCE_DB folder to the generic flow, and publishes nothing itself."""
    dpr_mock = _patch(mocker, _items("IW"))
    check, publish = _patch_reference_db(mocker)
    dpr_mock.side_effect = lambda *args, **kwargs: seen.append(s1_ard.REFERENCE_DB_PATH.get())
    seen: list[str | None] = []

    await s1_ard.process_s1ard.fn([SLC_IW_1], None, ENV_ARGS)

    assert seen == [None]
    check.assert_not_awaited()
    publish.assert_not_awaited()


def test_allocate_reference_db_path(mocker, monkeypatch):
    """The REFERENCE_DB folder is in the bucket configured for the owner and the REFERENCE_DB collection."""
    monkeypatch.setenv("RSPY_HOST_OSAM", "http://osam")
    fetch = mocker.patch.object(s1_ard, "fetch_csv_from_endpoint", return_value=[["config"]])
    find = mocker.patch.object(s1_ard, "find_s3_output_bucket", return_value="my-bucket")

    path = s1_ard._allocate_reference_db_path("owner")

    fetch.assert_called_once_with("http://osam/internal/configuration")
    find.assert_called_once_with([["config"]], "owner", "s1-ard-reference-db", "ARD_REFERENCE_DB")
    prefix = "s3://my-bucket/owner/s1-ard-reference-db/"
    assert path.startswith(prefix)
    UUID(path.removeprefix(prefix))
    assert s1_ard._allocate_reference_db_path("owner") != path


def _slc_item() -> Item:
    """STAC item of a master SLC from the catalog."""
    item = Item(
        id=f"{SLC_IW_1}.zarr",
        geometry={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
        bbox=[0, 0, 1, 1],
        datetime=datetime(2026, 4, 8, 17, 46, 32, tzinfo=timezone.utc),
        properties={
            "start_datetime": "2026-04-08T17:46:32Z",
            "end_datetime": "2026-04-08T17:46:59Z",
            "platform": "sentinel-1c",
            "constellation": "sentinel-1",
            "sar:instrument_mode": "IW",
            "sat:relative_orbit": 117,
            "product:type": "S01SIWSLC",
            "eopf:datatake_id": "not copied",
        },
        stac_extensions=["https://stac-extensions.github.io/sar/v1.3.0/schema.json"],
    )
    item.set_self_href(f"https://rspy.example.com/catalog/collections/owner:s01siwslc/items/{SLC_IW_1}.zarr")
    return item


def test_build_reference_db_item():
    """The REFERENCE_DB item is built from the master SLC item, and derived from it."""
    item = s1_ard._build_reference_db_item(REFERENCE_DB_PATH, _slc_item())

    assert item.id == REFERENCE_DB_UUID
    assert item.geometry == _slc_item().geometry
    assert item.bbox == [0, 0, 1, 1]
    assert item.datetime == datetime(2026, 4, 8, 17, 46, 32, tzinfo=timezone.utc)
    assert item.stac_extensions == ["https://stac-extensions.github.io/sar/v1.3.0/schema.json"]
    properties = item.properties
    assert properties["product:type"] == "ARD_REFERENCE_DB"
    assert properties["sar:instrument_mode"] == "IW"
    assert properties["sat:relative_orbit"] == 117
    assert properties["platform"] == "sentinel-1c"
    assert "eopf:datatake_id" not in properties
    assert "created" in properties
    assert list(item.assets) == ["REFERENCE_DB"]
    asset = item.assets["REFERENCE_DB"]
    assert asset.href == REFERENCE_DB_PATH
    assert asset.roles == ["data"]
    assert [link.target for link in item.links if link.rel == "derived_from"] == [
        f"/catalog/collections/owner:s01siwslc/items/{SLC_IW_1}.zarr",
    ]


async def test_publish_reference_db(mocker):
    """The REFERENCE_DB is published in its collection as an output product."""
    publish = mocker.patch.object(s1_ard.catalog_flow, "publish", new=AsyncMock(return_value=[{"id": "x"}]))
    mocker.patch.object(s1_ard, "get_run_logger", return_value=MagicMock())
    flow_env = flow_env_mock()

    await s1_ard._publish_reference_db(flow_env, REFERENCE_DB_PATH, _slc_item())

    publish.assert_awaited_once()
    mapping, items_metadata = publish.call_args.args[1:]
    assert [(m.name, m.product_type, m.collection_name) for m in mapping] == [
        (REFERENCE_DB_UUID, "ARD_REFERENCE_DB", "s1-ard-reference-db"),
    ]
    assert [(m.output_product_id, m.product_type, m.stac_item.id) for m in items_metadata] == [
        (REFERENCE_DB_UUID, "ARD_REFERENCE_DB", REFERENCE_DB_UUID),
    ]


async def test_publish_reference_db_fails_if_not_published(mocker):
    """The flow fails if the catalog rejects the REFERENCE_DB item: the publish task only logs a warning."""
    mocker.patch.object(s1_ard.catalog_flow, "publish", new=AsyncMock(return_value=[]))
    with pytest.raises(RuntimeError, match="not published"):
        await s1_ard._publish_reference_db(flow_env_mock(), REFERENCE_DB_PATH, _slc_item())


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
