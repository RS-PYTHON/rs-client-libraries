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

"""Test the Prefect workflows"""

import json
import typing
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import pytest_responses  # pylint: disable=unused-import # noqa: F401 # used to avoid adding @responses.activate
import responses
from pystac import Asset, Item, ItemCollection
from starlette import status

from rs_client.stac import catalog_client
from rs_common import prefect_utils
from rs_workflows import (
    aux_flow,
    cadip_flow,
    catalog_flow,
    on_demand_processing,
    prip_flow,
)
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessedItemMetadata,
    DprProcessIn,
    FlowEnvArgs,
    FlowGeneratedProduct,
    FlowInputProduct,
    ProcessingMode,
)
from rs_workflows.payload_generator import RSPY_CATALOG_BUCKET
from tests.conftest import (
    COLLECTION_ID,
    MOCKED_BUCKET,
    MOCKED_RSPY_WEBSITE,
    OWNER_ID,
)
from tests.test_utils import setup_worklow_test_env

CONFIG_DIR = Path(__file__).parent / "resources"


##################
# Mock variables #
##################

JUPYTERHUB_API_TOKEN = "JUPYTERHUB_API_TOKEN"
DASK_CLUSTER_LABEL = "DASK_CLUSTER_LABEL"
DASK_CLUSTER_INSTANCE = "dask-gateway.test-cluster-instance"
DASK_GATEWAY_PUBLIC = "http://test-dask-gateway-public"

MAP_PRODUCT_TO_COLLECTION = [
    {"name": "S03OLCL0_", "product_type": "S1_GRD", "collection_name": "OUTPUT_GRD_COLLECTION"},
    {"name": "S03OLCL0_", "product_type": "S2_NTC", "collection_name": "OUTPUT_NTC_COLLECTION"},
]

##################
# Mock functions #
##################


@pytest.fixture(name="mocked_tasktable")
def _mocked_tasktable():
    """Mock the mockup processor tasktable"""
    with open(CONFIG_DIR / "tasktable.json", encoding="utf-8") as f:
        responses.get(
            url=f"{MOCKED_RSPY_WEBSITE}/dpr/processes/mockup?"
            f"jupyter_token={JUPYTERHUB_API_TOKEN}&cluster_label={DASK_CLUSTER_LABEL}"
            f"&cluster_instance={DASK_CLUSTER_INSTANCE}",
            json=json.load(f),
            status=status.HTTP_200_OK,
        )


@pytest.fixture(autouse=True)
def mock_record_performance_indicators(mocker):
    """
    Auto-applied fixture that mocks the Prefect task `record_performance_indicators`
    so that no real DB or side effects are triggered during tests.
    """
    fake_task = MagicMock()
    fake_task.fn = MagicMock()
    mocker.patch("rs_workflows.dpr_flow.record_performance_indicators", fake_task, create=True)

    return fake_task


#############
# DPR flows #
#############


@typing.no_type_check
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mocked_stac_catalog_search_inside_collection",
    [["auxip", "catalog"]],
    indirect=True,
    ids=[""],
)
@pytest.mark.parametrize("mocked_dpr_response", ["mockup"], indirect=True, ids=[""])
async def test_dpr_processing(
    monkeypatch,
    mocker,
    mocked_s3,
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, /...
    mocked_stac_catalog_get_collection,  # /catalog/collections[/...]
    mocked_stac_catalog_search_inside_collection,  # /auxip/search[/...], /catalog/search[/...]
    mocked_staging_response,  # /processes/staging/execution, /jobs/{job_id}
    mocked_tasktable,  # /dpr/processes/mockup?...
    mocked_dpr_response,  # /dpr/processes/mockup/execution, /dpr/jobs/{job_id}
    mocked_processor_output,
):  # pylint: disable=unused-argument
    """Test the dpr_processing flow"""

    #########
    # Mocks #
    #########

    # Spy/patch artifact creation to assert keys
    artifact_mock = AsyncMock()
    mocker.patch.object(on_demand_processing, "acreate_markdown_artifact", artifact_mock)

    # Mock posting of the items in the catalog
    item_collections = [col["collection_name"] for col in MAP_PRODUCT_TO_COLLECTION]
    for result_collection_id in item_collections:
        responses.post(
            f"{MOCKED_RSPY_WEBSITE}/catalog/collections/{OWNER_ID}:{result_collection_id}/items",
            json={"status": status.HTTP_200_OK},
            status=status.HTTP_200_OK,
        )

    # Spy on function calls
    spy_add_item = mocker.spy(catalog_client.CatalogClient, "add_item")
    spy_s3_upload_file = mocker.spy(prefect_utils, "s3_upload_file")
    spy_s3_delete = mocker.spy(prefect_utils, "s3_delete")
    monkeypatch.setenv("RSPY_HOST_OSAM", "https://dummy-osam")
    mocker.patch(
        "rs_workflows.payload_generator.fetch_csv_from_endpoint",
        return_value=[["*", "*", "*", "90", RSPY_CATALOG_BUCKET]],
    )
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, f"s3://{MOCKED_BUCKET}/S1CADUS"),
    )

    ################
    # Init and run #
    ################

    # Save env vars in prefect secret blocks
    await setup_worklow_test_env(
        {
            "JUPYTERHUB_API_TOKEN": JUPYTERHUB_API_TOKEN,
            "DASK_GATEWAY_PUBLIC": DASK_GATEWAY_PUBLIC,
            "RSPY_HOST_OSAM": "https://dummy-osam",
        },
    )

    # build realistic input
    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name="mockup",
        processor_version="1.0",
        pipeline="mockup_full",
        dask_cluster_label=DASK_CLUSTER_LABEL,
        dask_cluster_instance=DASK_CLUSTER_INSTANCE,
        input_products=[
            {
                "name": "S1CADUS",
                "item_id": "S1A_OPER_AUX_PREORB_OPOD_20240527T062732_V20240527T062732_20240527T062732.EOF",
                "collection_name": COLLECTION_ID,
            },
        ],
        generated_product_to_collection_identifier=MAP_PRODUCT_TO_COLLECTION,  # type: ignore
        auxiliary_product_to_collection_identifier=[{"product_type": "*", "collection_name": COLLECTION_ID}],
        processing_mode=[ProcessingMode.NRT],  # type: ignore[list-item]
        start_datetime=datetime(2023, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        satellite="S1A",
        s3_payload_file=f"s3://{MOCKED_BUCKET}/payload.yaml",
    )

    # run the flow
    await on_demand_processing.dpr_processing(dpr_input)

    ###########
    # Asserts #
    ###########

    # catalog_client.CatalogClient.add_item is the last step of the flow.
    # We check that it was called with the expected items, generated by the processor.
    _, expected_items = mocked_processor_output

    # Update expected origin_datetime for mockup processor, and add the UUID in the href
    output_uuid = "00000000-0000-0000-0000-000000000001"
    for item in expected_items.values():
        item["properties"]["eopf:origin_datetime"] = "2026-01-01T00:00:00Z"
        # The href must include the UUID generated by payload_generator
        product_name = item["id"]
        old_href = item["assets"][product_name]["href"]
        # wanted: s3://.../OUTPUT_COLLECTION/UUID/product_name
        base_path = old_href.rsplit("/", 1)[0]
        item["assets"][product_name]["href"] = f"{base_path}/{output_uuid}/{product_name}"

    result_collection_ids = []
    result_items = {}
    for i in range(len(expected_items)):
        _, result_collection_id, result_item = spy_add_item.call_args_list[i][0]
        result_collection_ids.append(result_collection_id)
        result_items[result_item.id] = result_item.to_dict()

    assert sorted(result_collection_ids) == sorted([col["collection_name"] for col in MAP_PRODUCT_TO_COLLECTION])

    links = result_items["GRD"]["links"]
    assert any(link["rel"] == "derived_from" for link in links)

    assert any(link["rel"] == "processing-execution" for link in links)

    for item in result_items.values():
        item.pop("links", None)

    for item in expected_items.values():
        item.pop("links", None)

    assert result_items == expected_items

    # --- verify s3_upload_file was called with the expected destination (second arg) ---
    upload_calls = spy_s3_upload_file.call_args_list
    assert len(upload_calls) == 1
    args = upload_calls[0].args
    assert isinstance(args[0], (str, Path))  # temp file path
    assert args[1] == dpr_input.s3_payload_file  # destination S3 path

    # --- verify s3_delete was called with the payload file ---
    delete_calls = spy_s3_delete.call_args_list  # pylint: disable=no-member
    assert len(delete_calls) == 1
    args = delete_calls[0].args
    assert args[0] == dpr_input.s3_payload_file  # destination S3 path for payload file

    # Verify the two artifact calls use the correct keys
    keys = [c.kwargs.get("key") for c in artifact_mock.await_args_list]
    assert artifact_mock.await_count == 4
    assert keys == ["dpr-task-table", "aux-cql2-filter", "aux-cql2-filter", "dpr-payload"]


@pytest.mark.asyncio
async def test_normalize_archived_auxip_items_updates_collection_and_catalog(mocker):
    """Normalized archived items should replace collection entries and be persisted to the catalog."""
    item_a = Item(id="item-a", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    item_a.add_asset("archive.zip", Asset(href="s3://bucket/path/archive.zip"))
    item_b = Item(id="item-b", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    item_b.add_asset("plain.bin", Asset(href="s3://bucket/path/plain.bin"))
    item_collection = ItemCollection([item_a, item_b])

    normalized_item = Item(id="item-a", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    normalized_item.add_asset("archive", Asset(href="s3://bucket/path/archive/file"))

    future_mock = MagicMock()
    future_mock.result.return_value = normalized_item
    submit_mock = MagicMock(return_value=future_mock)
    mocker.patch.object(on_demand_processing.aux_flow.aux_unzip_decompress_task, "submit", submit_mock)

    catalog_client_mock = MagicMock()
    flow_env_mock = MagicMock()
    flow_env_mock.rs_client.get_catalog_client.return_value = catalog_client_mock
    mocker.patch.object(on_demand_processing, "FlowEnv", MagicMock(return_value=flow_env_mock))

    dpr_input = MagicMock()
    dpr_input.env = FlowEnvArgs(owner_id=OWNER_ID)

    result = await on_demand_processing._normalize_archived_aux_items(  # pylint: disable=protected-access
        item_collection,
        dpr_input,
    )

    assert result is item_collection
    assert result.items[0] is normalized_item
    assert result.items[1].id == item_b.id
    assert result.items[1].assets["plain.bin"].href == item_b.assets["plain.bin"].href
    assert submit_mock.call_count == 1
    submitted_item = submit_mock.call_args.args[0]
    assert submitted_item.id == item_a.id
    assert submitted_item.assets["archive.zip"].href == item_a.assets["archive.zip"].href
    catalog_client_mock.update_item.assert_called_once_with(normalized_item)


@pytest.mark.asyncio
async def test_normalize_archived_auxip_items_wraps_catalog_update_errors(mocker):
    """Catalog update failures should be wrapped with the task-specific RuntimeError."""
    item = Item(id="item-a", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    item.add_asset("archive.zip", Asset(href="s3://bucket/path/archive.zip"))
    item_collection = ItemCollection([item])

    normalized_item = Item(id="item-a", geometry=None, bbox=None, datetime=datetime.now(timezone.utc), properties={})
    normalized_item.add_asset("archive", Asset(href="s3://bucket/path/archive/file"))

    future_mock = MagicMock()
    future_mock.result.return_value = normalized_item
    submit_mock = MagicMock(return_value=future_mock)
    mocker.patch.object(on_demand_processing.aux_flow.aux_unzip_decompress_task, "submit", submit_mock)

    catalog_client_mock = MagicMock()
    catalog_client_mock.update_item.side_effect = ValueError("boom")
    flow_env_mock = MagicMock()
    flow_env_mock.rs_client.get_catalog_client.return_value = catalog_client_mock
    mocker.patch.object(on_demand_processing, "FlowEnv", MagicMock(return_value=flow_env_mock))

    dpr_input = MagicMock()
    dpr_input.env = FlowEnvArgs(owner_id=OWNER_ID)

    with pytest.raises(RuntimeError, match="Error while trying to update the item collection"):
        await on_demand_processing._normalize_archived_aux_items(  # pylint: disable=protected-access
            item_collection,
            dpr_input,
        )


def test_resolve_specific_input_product_stac_items_nominal(mocker):
    """
    Nominal case:
    - multiplicity = one_per_input
    - exactly one referenced input product
    - regex matches at least one STAC asset
    """

    # --- Mock logger ---
    mock_logger = MagicMock()
    mocker.patch(
        "rs_workflows.on_demand_processing.get_run_logger",
        return_value=mock_logger,
    )

    # Mock STAC resolution to return different paths
    mocker.patch(
        "rs_workflows.on_demand_processing.resolve_stac_input_path",
        side_effect=[
            ("item1", "s3://path/to/item1"),
            ("item2", "s3://path/to/item2"),
        ],
    )

    # --- Mock RS client / catalog ---
    mock_catalog = MagicMock()
    mock_rs_client = MagicMock()
    mock_rs_client.get_catalog_client.return_value = mock_catalog

    # --- Input ADFS ---
    input_adfs = {
        "name": "ADFS_INPUT",
    }

    # --- Task table ---
    task_table = {
        "io": [
            {
                "name": "ADFS_INPUT",
                "multiplicity": "one_per_input",
                "alternatives": [
                    {
                        "order": 1,
                        "timeout_seconds": 0,
                        "query": {
                            "name": "LatestValCover",
                            "parameters": {
                                "product_type": "SOMETHING",
                                "start_datetime": "{S1CADUS.start_datetime}",
                                "end_datetime": "{S1CADUS.end_datetime}",
                                "satellite": "{S1CADUS.platform}",
                                "dTa": 0,
                                "dTb": 0,
                            },
                        },
                    },
                ],
            },
            {
                "name": "S1CADUS",
                "store_params": {"regex": r".*item\d"},
            },
        ],
    }

    # --- Unit config ---
    unit = {
        "input_products": [
            {"name": "S1CADUS"},
        ],
    }

    # --- Provided input products ---
    provided_input_products = [
        FlowInputProduct(name="S1CADUS", collection_name="stac_collection", item_id="item1"),
        FlowInputProduct(name="S1CADUS", collection_name="stac_collection", item_id="item2"),
    ]

    # --- Call ---
    ref_name, items = (
        on_demand_processing._resolve_specific_input_product_stac_items(  # pylint:disable=protected-access
            input_adfs,
            task_table,
            unit,
            provided_input_products,
            mock_rs_client,
        )
    )

    # --- Assertions ---
    assert ref_name == "S1CADUS"
    assert items == ["item1", "item2"]

    mock_logger.info.assert_any_call("ADFS multiplicity 'one_per_input' refers to input 'S1CADUS'")


async def test_dpr_processing_raises_on_unstaged_adf(
    mocker,
    mocked_tasktable,  # /dpr/processes/mockup?...
):  # pylint: disable=unused-argument
    """The flow should raise ValueError when an ADF could not be staged (status=False)."""

    #########
    # Mocks #
    #########

    class PrefectFutureFailStub:  # pylint: disable=too-few-public-methods
        """Mock future that returns a failed ADF staging result to trigger ValueError."""

        def result(self):
            """Return [(False, ItemCollection([...]))] with an item that has one asset."""
            it = Item(
                id="unstaged1",
                properties={"product:type": "AUX_MOCK"},
                geometry={},
                bbox=[],
                datetime=datetime.now(),
            )
            it.add_asset("data", Asset(href=f"s3://{MOCKED_BUCKET}/unstaged1.bin"))
            return ("ADFS_NAME", "filename", (False, ItemCollection([it])))

    class ProcessInputAdfsTaskFailMock(Mock):
        """Mock of process_input_adfs to force status=False in the flow."""

        def submit(self, *_, **__):
            """Return a PrefectFutureFailStub."""
            return PrefectFutureFailStub()

    mocker.patch.object(on_demand_processing, "process_input_adfs", ProcessInputAdfsTaskFailMock())

    ################
    # Init and run #
    ################

    with open(CONFIG_DIR / "tasktable.json", encoding="utf-8") as f:
        responses.get(
            url=f"{MOCKED_RSPY_WEBSITE}/dpr/processes/mockup?"
            f"jupyter_token={JUPYTERHUB_API_TOKEN}&cluster_label=dask-eopf-mockup"
            f"&cluster_instance={DASK_CLUSTER_INSTANCE}",
            json=json.load(f),
            status=status.HTTP_200_OK,
        )

    await setup_worklow_test_env(
        {
            "JUPYTERHUB_API_TOKEN": JUPYTERHUB_API_TOKEN,
            "DASK_GATEWAY_EOPF_MOCKUP_PUBLIC": DASK_GATEWAY_PUBLIC,
        },
    )

    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name="mockup",
        processor_version="1.0",
        pipeline="mockup_full",
        dask_cluster_label="dask-eopf-mockup",
        dask_cluster_instance=DASK_CLUSTER_INSTANCE,
        input_products=[
            FlowInputProduct(name="input_name", item_id="stac_item_id", collection_name="collection_name"),
        ],
        generated_product_to_collection_identifier=[
            FlowGeneratedProduct(
                name="output_folder",
                product_type="AUX_MOCK",
                collection_name="CATALOG_COLLECTION_ID",
            ),
        ],
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(product_type="*", collection_name=COLLECTION_ID),
        ],
        processing_mode=["nrt"],  # type: ignore[list-item]
        start_datetime=datetime(2023, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        satellite="S1A",
        s3_payload_file=f"s3://{MOCKED_BUCKET}/payload.yaml",
    )
    with pytest.raises(ValueError, match="was not correctly staged"):
        await on_demand_processing.dpr_processing(dpr_input)


@pytest.mark.parametrize(
    "mocked_stac_catalog_search_inside_collection",
    [["cadip"]],
    indirect=True,
    ids=[""],
)
async def test_on_demand_cadip_staging(
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, ...
    mocked_stac_catalog_search_inside_collection,  # /cadip/search[/...]
    mocked_staging_response,  # /processes/staging/execution, /jobs/{job_id}
):  # pylint: disable=unused-argument
    """Test the on_demand_cadip_staging flow"""
    await setup_worklow_test_env()
    await cadip_flow.on_demand_cadip_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        # values come from the mocked_stac_catalog_search_inside_collection fixture
        cadip_collection_identifier="S1_L1",
        session_identifier="DCS_01_S1A_20200105072204051312_ch1_DSDB_00000.raw",
        catalog_collection_identifier="catalog_collection_identifier",
    )


@pytest.mark.parametrize(
    "mocked_stac_catalog_search_inside_collection",
    [["auxip", "catalog"]],
    indirect=True,
    ids=[""],
)
async def test_on_demand_auxip_staging(
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, ...
    mocked_stac_catalog_search_inside_collection,  # /auxip/search[/...], /catalog/search[/...]
    mocked_staging_response,  # /processes/staging/execution, /jobs/{job_id}
    mocked_stac_catalog_get_collection,  # /catalog/collections/...
):  # pylint: disable=unused-argument
    """Test the on_demand_auxip_staging flow"""
    await setup_worklow_test_env()
    await aux_flow.on_demand_aux_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        start_datetime="2024-05-27T09:44:09.509000Z",
        end_datetime="2024-05-27T09:44:19.509000Z",
        product_type="AUX_PP2",
        # value comes from the mocked_stac_catalog_search_inside_collection fixture
        catalog_collection_identifier="S1_L1",
    )


@pytest.mark.parametrize(
    "mocked_stac_catalog_search_inside_collection",
    [["prip"]],
    indirect=True,
    ids=[""],
)
async def test_on_demand_prip_staging(
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, ...
    mocked_stac_catalog_search_inside_collection,  # /prip/search[/...]
    mocked_staging_response,  # /processes/staging/execution, /jobs/{job_id}
):  # pylint: disable=unused-argument
    """Test the on_demand_prip_staging flow"""
    await setup_worklow_test_env()
    await prip_flow.on_demand_prip_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        start_datetime="2024-05-27T09:44:09.509000Z",
        end_datetime="2024-05-27T09:44:19.509000Z",
        product_type="S2MSI1C",
        prip_collection="prip-collection",
        catalog_collection_identifier="catalog_collection_identifier",
    )


async def test_catalog_search(
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, ...
    mocked_stac_catalog_search_inside_collection,  # /catalog/search[/...]
):  # pylint: disable=unused-argument
    """Test the catalog_search flow"""
    await setup_worklow_test_env()
    await catalog_flow.catalog_search(env=FlowEnvArgs(owner_id=OWNER_ID), catalog_cql2={"filter": {}})


@pytest.mark.asyncio
async def test_publish_skips_when_no_matching_output_collection(
    mocker,
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, ...
):  # pylint: disable=unused-argument
    """Test: no matching output product -> item skipped"""
    await setup_worklow_test_env()
    env = FlowEnvArgs(owner_id=OWNER_ID)
    spy_add_item = mocker.spy(catalog_client.CatalogClient, "add_item")

    catalog_collection_identifier = [
        FlowGeneratedProduct(name="INVALID", product_type="INVALID", collection_name="COLL_GRD"),
    ]

    items = [
        DprProcessedItemMetadata(
            stac_item=Item(
                **{  # type: ignore
                    "id": "item1",
                    "properties": {
                        "product:type": "S1_GRD",
                        "datetime": "2024-01-01T00:00:00Z",
                        "instruments": ["instrument1"],
                    },
                    "datetime": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "geometry": None,
                    "bbox": None,
                },
            ),
            product_type="S1_GRD",
            output_product_id="item1",
        ),
    ]

    with pytest.raises(RuntimeError) as error:
        await catalog_flow.publish.fn(env, catalog_collection_identifier, items)
        spy_add_item.assert_not_called()
    assert "Could not find a collection to publish the stac_item from" in str(error.value.__cause__)
