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
    auxip_flow,
    catalog_flow,
    on_demand_processing,
)
from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnvArgs,
    ProcessingMode,
)
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

MAP_PRODUCT_TO_COLLECTION = [
    {"GRD": ("S1_GRD", "OUTPUT_GRD_COLLECTION")},
    {"NTC": ("S2_NTC", "OUTPUT_NTC_COLLECTION")},
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
            f"jupyter_token={JUPYTERHUB_API_TOKEN}&cluster_label={DASK_CLUSTER_LABEL}&cluster_instance=",
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
    item_collections = [list(col.values())[0][1] for col in MAP_PRODUCT_TO_COLLECTION]
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

    ################
    # Init and run #
    ################

    # Save env vars in prefect secret blocks
    await setup_worklow_test_env({"JUPYTERHUB_API_TOKEN": JUPYTERHUB_API_TOKEN})

    # build realistic input
    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name="mockup",
        processor_version="1.0",
        pipeline="mockup_full",
        dask_cluster_label=DASK_CLUSTER_LABEL,
        input_products=[{"input_name": ("dummy_id", "dummy_collection")}],
        generated_product_to_collection_identifier=MAP_PRODUCT_TO_COLLECTION,  # type: ignore
        auxiliary_product_to_collection_identifier={"*": COLLECTION_ID},
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
    fixed_uuids = [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    ]
    for i, item in enumerate(expected_items.values()):
        item["properties"]["eopf:origin_datetime"] = "2026-01-01T00:00:00Z"
        # The href must include the UUID generated by payload_generator
        product_name = item["id"]
        product_uuid = fixed_uuids[i]
        old_href = item["assets"][product_name]["href"]
        # wanted: s3://.../TEST_FLOW_OUTPUT/UUID/product_name
        base_path = old_href.rsplit("/", 1)[0]
        item["assets"][product_name]["href"] = f"{base_path}/{product_uuid}/{product_name}"

    result_collection_ids = []
    result_items = {}
    for i in range(len(expected_items)):
        _, result_collection_id, result_item = spy_add_item.call_args_list[i][0]
        result_collection_ids.append(result_collection_id)
        result_items[result_item.id] = result_item.to_dict()

    assert sorted(result_collection_ids) == sorted([list(p.values())[0][1] for p in MAP_PRODUCT_TO_COLLECTION])
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
    assert keys == ["processing-unit-list", "auxip-cql2", "auxip-cql2", "dpr-payload-file"]


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
            return ("ADFS_NAME", (False, ItemCollection([it])))

    class ProcessInputAdfsTaskFailMock(Mock):
        """Mock of process_input_adfs to force status=False in the flow."""

        def submit(self, *_, **__):
            """Return a PrefectFutureFailStub."""
            return PrefectFutureFailStub()

    mocker.patch.object(on_demand_processing, "process_input_adfs", ProcessInputAdfsTaskFailMock())

    ################
    # Init and run #
    ################

    await setup_worklow_test_env({"JUPYTERHUB_API_TOKEN": JUPYTERHUB_API_TOKEN})

    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name="mockup",
        processor_version="1.0",
        pipeline="mockup_full",
        dask_cluster_label=DASK_CLUSTER_LABEL,
        input_products=[{"input_name": ("stac_item_id", "collection_name")}],  # Item STAC
        generated_product_to_collection_identifier=[{"output_folder": ("CATALOG_COLLECTION_ID")}],
        auxiliary_product_to_collection_identifier={"*": "CATALOG_COLLECTION_ID"},
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
    await on_demand_processing.on_demand_cadip_staging(
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
    await auxip_flow.on_demand_auxip_staging(
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
    await on_demand_processing.on_demand_prip_staging(
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

    catalog_collection_identifier = [{"INVALID": ("INVALID", "COLL_GRD")}]

    items = [
        {
            "id": "item1",
            "properties": {"product:type": "S1_GRD", "datetime": "2024-01-01T00:00:00Z"},
            "geometry": None,
            "bbox": None,
        },
    ]

    with pytest.raises(RuntimeError) as error:
        await catalog_flow.publish.fn(env, catalog_collection_identifier, items)
        spy_add_item.assert_not_called()
    assert str(error.value.__cause__) == "Product type unknown: S1_GRD"
