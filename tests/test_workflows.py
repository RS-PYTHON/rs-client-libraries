# Copyright 2025 CS Group
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
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import responses
from prefect.blocks.system import Secret
from pydantic import SecretStr
from pystac import Asset, Item, ItemCollection

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_client.rs_client import RsClient
from rs_common import prefect_utils
from rs_workflows import (
    auxip_flow,
    cadip_flow,
    catalog_flow,
    init_pi_db_flow,
    on_demand_processing,
    pi_db_models,
    prip_flow,
    staging_flow,
)
from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnvArgs,
    ProcessingMode,
)
from rs_workflows.pi_db_models import Base
from tests.conftest import COLLECTION_ID, HTTP_OK, MOCKED_RSPY_WEBSITE, OWNER_ID

S3_PAYLOAD = "S3_PAYLOAD"
RSPY_APIKEY = "RSPY_APIKEY"
CONFIG_DIR = Path(__file__).parent / "resources"


def make_mock_processed_item(item_id: str, product_type: str):
    """Create a realistic processed item as returned by the DPR processor."""
    return {
        "stac_discovery": {
            "id": item_id,
            "geometry": {"type": "Polygon", "coordinates": [[[-10, 40], [10, 40], [10, 60], [-10, 60], [-10, 40]]]},
            "bbox": [-10, 40, 10, 60],
            "properties": {
                "datetime": "2024-01-01T12:00:00Z",
                "product:type": product_type,  # ← this must be a real string!
            },
        },
    }


# Realistic processed items returned by run_processor
MOCK_PROCESSED_ITEMS = [
    make_mock_processed_item("S1A_20240101_GRD", "S1_GRD"),
    make_mock_processed_item("S2A_20240101_NTC", "S2_NTC"),
]

#########
# Mocks #
#########


@pytest.fixture(autouse=True)
def mocked_auxip_search(mocked_stac_catalog_search_inside_collection):
    """Mock auxip search response with the same contents as the catalog search."""
    responses.post(
        url=f"{MOCKED_RSPY_WEBSITE}/auxip/search",
        json=mocked_stac_catalog_search_inside_collection,
        status=HTTP_OK,
    )


class MockStr(Mock):
    """Mock str"""

    def split(self, *_, **__):
        """Mock str split"""
        return ["split1", "split2"]


class MockRsClient(Mock):
    """Mock RsClient class"""

    # Mocked properties
    id = "id"
    assets = {"asset1": MockStr(), "asset2": MockStr()}
    properties = {
        "prop1": MockStr(),
        "prop2": MockStr(),
    }

    def search(self, *_, **__):
        """Mock stac search"""
        items = []
        # Return two minimal STAC Items that mimic AUXIP search results.
        for i in ["test1", "test2"]:
            # Each item has:
            #   - properties["product:type"] - read by dpr_processing to label the ADF type
            #   - a single asset with an S3 href - dpr_processing takes the first asset.href
            it = Item(
                id=i,
                properties={"product:type": "AUX_MOCK"},
                geometry={},
                bbox=[],
                datetime=datetime.now(),
            )
            it.add_asset("data", Asset(href=f"s3://mock-bucket/{i}.bin"))
            items.append(it)
        return ItemCollection(items)

    def get_items(self, *_, **__):
        """Mock stac get_items"""
        return self.search()

    def get_collections(self, *_, **__):
        """Mock stac get_collections"""
        return [MockRsClient()] * 2

    def wait_for_job(self, *_, **__):
        """Mock successful job that returns processed items."""
        return MOCK_PROCESSED_ITEMS

    def wait_for_jobs(self, *_, **__):
        """Mock wait_for_jobs"""
        return {"job_status": {"status": "successful"}}

    def get_process(self, *_, **__):
        """Mock DprClient get_process"""
        filename = "tasktable.json"
        with open(CONFIG_DIR / filename, encoding="utf-8") as f:
            return json.load(f)


# ---------- Prefect task mocks used by flow ----------
class PayloadStub:  # pylint: disable=too-few-public-methods
    """Minimal payload object used by the flow tests."""

    def dump(self):
        """Return the minimal structure consumed by yaml.dump()."""
        return {"workflow": [], "io": {"input_products": [], "output_products": []}}


class PrefectFutureStub:  # pylint: disable=too-few-public-methods
    """Mock future that mimics Prefect's Future API for tests."""

    def result(self):
        """Return a PayloadStub instance."""
        return PayloadStub()


class GeneratePayloadTaskMock(Mock):
    """Mock of a Prefect task used to stub generate_payload.submit()."""

    def submit(self, *_, **__):
        """Return a PrefectFutureStub."""
        return PrefectFutureStub()


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
        it.add_asset("data", Asset(href="s3://mock-bucket/unstaged1.bin"))
        return ("ADFS_NAME", (False, ItemCollection([it])))


class ProcessInputAdfsTaskFailMock(Mock):
    """Mock of process_input_adfs to force status=False in the flow."""

    def submit(self, *_, **__):
        """Return a PrefectFutureFailStub."""
        return PrefectFutureFailStub()


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


############
# DPR FLOW #
############


async def mock_s3_download_file(
    s3_path: str,  # pylint: disable=unused-argument
    to_path: str | Path | None,
    **__: dict[str, Any],
) -> Path:
    """Mock the prefect_utils.s3_download_file function"""
    if not to_path:
        return Path()

    # Mock the downloading of S3_PAYLOAD
    with open(to_path, "w", encoding="utf-8") as opened:
        opened.write(
            """
workflow:
- name: workflow_name
  module: workflow_module
  processing_unit: workflow_processing_unit
  outputs:
    out1: output1
    out2: output2
""",
        )
    return Path(to_path)


async def setup_worklow_test_env(env_vars: dict[str, str] | None = None):
    """Set up secret blocks needed for correct execution of workflows in Prefect"""
    # Environment variables for all users. For these test we don't need specific values
    # so it creates an empty secret. See test_prefect_utils.py for a real case example.
    # Use an empty dictionary if input_dict is None
    # Default arguments are evaluated once when the function is defined, not each
    # time the function is called. If env_vars = {} would have been used and modify env_vars in one call,
    # this modified dictionary would persists for subsequent calls, which can lead to bugs.
    # Using env_vars = None and creating a new empty dictionary inside this function avoids this issue.
    env_vars = env_vars if env_vars is not None else {}
    # Serialize dictionary to a JSON string and wrap it in SecretStr
    secret_value = SecretStr(json.dumps(env_vars))

    await Secret(
        value=secret_value,
    ).save(  # type: ignore[arg-type]
        prefect_utils.BLOCK_NAME_ENV_GLOBAL,
        overwrite=True,
    )

    # Create prefect block for current user
    await Secret(
        value={  # type: ignore[arg-type]
            "RSPY_WEBSITE": MOCKED_RSPY_WEBSITE,
            "RSPY_APIKEY": RSPY_APIKEY,
        },
    ).save(
        prefect_utils.format_env_user(prefect_utils.BLOCK_NAME_ENV_USER, OWNER_ID),
        overwrite=True,
    )


#############
# MAIN FLOW #
#############


@pytest.mark.asyncio
@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(prefect_utils, "s3_delete", Mock())
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(RsClient, "get_dpr_client", MockRsClient)
async def test_dpr_processing(
    mocker,
    mock_prefect,
    mock_record_performance_indicators,
    mocked_rspy_landing_pages,
    mocked_stac_catalog_get_collection,
):  # pylint: disable=unused-argument
    """Test the dpr_processing flow"""

    # Spy/patch artifact creation to assert keys
    artifact_mock = AsyncMock()
    mocker.patch.object(on_demand_processing, "acreate_markdown_artifact", artifact_mock)

    # Save env vars in prefect secret blocks
    await setup_worklow_test_env({"JUPYTERHUB_API_TOKEN": "JUPYTERHUB_API_TOKEN"})

    # We'll just check that the prefect tasks and flows were called.
    # We don't check the underlying RsClient functions, this is already done in dedicated pytests.
    spied = {
        mocker.spy(prefect_function, "fn"): call_count  # spy on <flow>.fn or <task>.fn = the underlying python function
        for prefect_function, call_count in {
            auxip_flow.search: 2,
            auxip_flow.search_task: 2,
            cadip_flow.search: 0,
            cadip_flow.search_task: 0,
            staging_flow.staging: 2,
            catalog_flow.publish: 1,
        }.items()
    }
    # mock ADF staging — use real pystac.Item, otherwise the pystac will be unhappy
    # real_adf_item = Item(
    #     id="AUX_TEST_001",
    #     geometry=None,
    #     bbox=None,
    #     datetime=datetime.now(),
    #     properties={"product:type": "AUX_TEST"},
    # )
    # real_adf_item.add_asset("data", Asset(href="s3://bucket/auxfile.bin"))

    # adf_future = MagicMock()
    # adf_future.result.return_value = ("AUX_TEST", (True, ItemCollection([real_adf_item])))

    # mocker.patch(
    #     "rs_workflows.on_demand_processing.process_input_adfs.submit",
    #     return_value=adf_future,
    # )

    # mock generate_payload.submit
    mock_payload = MagicMock()
    mock_payload.dump.return_value = {
        "io": {
            "output_products": [
                {"id": "GRD", "path": "s3://out/grd"},
                {"id": "NTC", "path": "s3://out/ntc"},
            ],
        },
    }

    payload_future = MagicMock()
    payload_future.result.return_value = mock_payload

    mocker.patch(
        "rs_workflows.on_demand_processing.generate_payload.submit",
        return_value=payload_future,
    )

    # mock run_processor.submit → returns processed items
    run_processor_future = MagicMock()
    run_processor_future.result.return_value = MOCK_PROCESSED_ITEMS

    mocker.patch(
        "rs_workflows.dpr_flow.run_processor.submit",
        return_value=run_processor_future,
    )

    # build realistic input
    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name=DprProcessor.MOCKUP,
        processor_version="1.0",
        pipeline="mockup_full",
        dask_cluster_label="cluster_label",
        input_products=[{"input_name": ("dummy_id", "dummy_coll")}],
        generated_product_to_collection_identifier=[
            {"GRD": ("S1_GRD", "OUTPUT_GRD_COLLECTION")},
            {"NTC": ("S2_NTC", "OUTPUT_NTC_COLLECTION")},
        ],
        auxiliary_product_to_collection_identifier={"*": COLLECTION_ID},
        processing_mode=[ProcessingMode.NRT],  # type: ignore[list-item]
        start_datetime=datetime(2023, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        satellite="S1A",
        s3_payload_file="s3://test-bucket/payload.yaml",
    )

    # run the flow
    await on_demand_processing.dpr_processing(dpr_input)

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count

    # --- verify s3_upload_file was called with the expected destination (second arg) ---
    upload_mock = cast(AsyncMock, prefect_utils.s3_upload_file)
    upload_calls = upload_mock.await_args_list
    assert len(upload_calls) == 1
    args = upload_calls[0].args
    assert isinstance(args[0], (str, Path))  # temp file path
    assert args[1] == dpr_input.s3_payload_file  # destination S3 path

    # --- verify s3_delete was called with the payload file ---
    delete_mock = cast(Mock, prefect_utils.s3_delete)
    delete_calls = delete_mock.call_args_list  # pylint: disable=no-member
    assert len(delete_calls) == 1
    args = delete_calls[0].args
    assert args[0] == dpr_input.s3_payload_file  # destination S3 path for payload file

    # Verify the two artifact calls use the correct keys
    keys = [c.kwargs.get("key") for c in artifact_mock.await_args_list]
    assert artifact_mock.await_count == 4
    assert keys == ["processing-unit-list", "auxip-cql2", "auxip-cql2", "dpr-payload-file"]


@patch.dict(os.environ, {}, clear=False)
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(prefect_utils, "s3_delete", Mock())
@patch.object(RsClient, "get_auxip_client", MockRsClient)
@patch.object(RsClient, "get_cadip_client", MockRsClient)
@patch.object(RsClient, "get_catalog_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(RsClient, "get_dpr_client", MockRsClient)
@patch.object(on_demand_processing, "process_input_adfs", ProcessInputAdfsTaskFailMock())
async def test_dpr_processing_raises_on_unstaged_adf(
    mocker,
    mock_prefect,
    mock_record_performance_indicators,
):  # pylint: disable=unused-argument, redefined-outer-name
    """The flow should raise ValueError when an ADF could not be staged (status=False)."""

    await setup_worklow_test_env({"JUPYTERHUB_API_TOKEN": "JUPYTERHUB_API_TOKEN"})

    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name=DprProcessor.MOCKUP,
        processor_version="1.0",
        pipeline="mockup_full",
        dask_cluster_label="cluster_label",
        input_products=[{"input_name": ("stac_item_id", "collection_name")}],  # Item STAC
        generated_product_to_collection_identifier=[{"output_folder": ("CATALOG_COLLECTION_ID")}],
        auxiliary_product_to_collection_identifier={"*": "CATALOG_COLLECTION_ID"},
        processing_mode=["nrt"],  # type: ignore[list-item]
        start_datetime=datetime(2023, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        satellite="S1A",
        s3_payload_file="s3://test-bucket/payload.yaml",
    )
    with pytest.raises(ValueError, match="was not correctly staged"):
        await on_demand_processing.dpr_processing(dpr_input)


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(prefect_utils, "s3_delete", Mock())
@patch.object(RsClient, "get_cadip_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
async def test_on_demand_cadip_staging(mocker, mock_prefect):  # pylint: disable=unused-argument
    """Test the on_demand_cadip_staging flow"""

    await setup_worklow_test_env()

    # We'll just check that the prefect tasks and flows were called.
    # We don't check the underlying RsClient functions, this is already done in dedicated pytests.
    spied = {
        mocker.spy(prefect_function, "fn"): call_count  # spy on <flow>.fn or <task>.fn = the underlying python function
        for prefect_function, call_count in {
            cadip_flow.search: 1,
            cadip_flow.search_task: 1,
            staging_flow.staging: 1,
        }.items()
    }

    # Run the prefect flow
    await on_demand_processing.on_demand_cadip_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        cadip_collection_identifier="cadip_collection_identifier",
        session_identifier="session_identifier",
        catalog_collection_identifier="catalog_collection_identifier",
    )

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(prefect_utils, "s3_delete", Mock())
@patch.object(RsClient, "get_auxip_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(RsClient, "get_catalog_client", MockRsClient)
async def test_on_demand_auxip_staging(mocker, mock_prefect):  # pylint: disable=unused-argument
    """Test the on_demand_auxip_staging flow"""

    await setup_worklow_test_env()

    # We'll just check that the prefect tasks and flows were called.
    # We don't check the underlying RsClient functions, this is already done in dedicated pytests.
    spied = {
        mocker.spy(prefect_function, "fn"): call_count  # spy on <flow>.fn or <task>.fn = the underlying python function
        for prefect_function, call_count in {
            auxip_flow.auxip_staging: 1,
            auxip_flow.search: 1,
            auxip_flow.search_task: 1,
            staging_flow.staging: 1,
        }.items()
    }

    # Run the prefect flow
    await auxip_flow.on_demand_auxip_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        start_datetime="2024-05-27T09:44:09.509000Z",
        end_datetime="2024-05-27T09:44:19.509000Z",
        product_type="AUX_PP2",
        catalog_collection_identifier="catalog_collection_identifier",
    )

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(prefect_utils, "s3_delete", Mock())
@patch.object(RsClient, "get_prip_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
async def test_on_demand_prip_staging(mocker, mock_prefect):  # pylint: disable=unused-argument
    """Test the on_demand_prip_staging flow"""

    await setup_worklow_test_env()

    spied = {
        mocker.spy(prefect_function, "fn"): call_count
        for prefect_function, call_count in {
            prip_flow.search: 1,
            prip_flow.search_task: 1,
            staging_flow.staging: 1,
        }.items()
    }

    # Run the prefect flow
    await on_demand_processing.on_demand_prip_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        start_datetime="2024-05-27T09:44:09.509000Z",
        end_datetime="2024-05-27T09:44:19.509000Z",
        product_type="S2MSI1C",
        prip_collection="prip-collection",
        catalog_collection_identifier="catalog_collection_identifier",
    )

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(RsClient, "get_catalog_client", MockRsClient)
async def test_catalog_search(mocker, mock_prefect):  # pylint: disable=unused-argument
    """Test the catalog_search flow"""

    await setup_worklow_test_env()

    spy_search = mocker.spy(MockRsClient, "search")

    # Run the prefect flow
    await catalog_flow.catalog_search(env=FlowEnvArgs(owner_id=OWNER_ID), catalog_cql2={"filter": {}})

    assert spy_search.call_count == 1
    spy_search.reset_mock()


def test_create_schema(monkeypatch, patch_prefect_logger):  # pylint: disable=unused-argument
    """
    Tests that the `create_schema` task correctly triggers table creation.

    This test verifies:
      - The SQLAlchemy engine is created using the provided database URL.
      - The `Base.metadata.create_all` method is called with the engine,
        ensuring that tables are initialized in the target database.

    Args:
        monkeypatch: Fixture to replace attributes during the test.

    Assertions:
        - `create_engine` is called once with the expected test database URL.
        - `Base.metadata.create_all` is called once with the mock engine.
    """

    mock_create_engine = MagicMock()
    monkeypatch.setattr(init_pi_db_flow, "create_engine", mock_create_engine)

    mock_create_all = MagicMock()
    monkeypatch.setattr(Base.metadata, "create_all", mock_create_all)

    test_db_url = "test_db_url"
    init_pi_db_flow.create_schema.fn(test_db_url)

    mock_create_engine.assert_called_once_with(test_db_url)
    mock_create_all.assert_called_once_with(mock_create_engine.return_value)


def test_insert_pi_categories(monkeypatch):
    """
    Tests that the `insert_pi_categories` task correctly inserts default categories.

    This test verifies:
      - A session is created using the SQLAlchemy engine.
      - If no categories exist, all predefined `PI_CATEGORY_DATA` entries are inserted.
      - Each inserted object has the correct attributes.
      - The session is committed and closed properly.

    Args:
        monkeypatch: Fixture to replace attributes during the test.

    Assertions:
        - `create_engine` is called with the test database URL.
        - `sessionmaker` is initialized with the engine.
        - Each category in `PI_CATEGORY_DATA` is added with correct attributes.
        - `commit` is called once.
        - `close` is called once.
    """

    mock_create_engine = MagicMock()
    monkeypatch.setattr(init_pi_db_flow, "create_engine", mock_create_engine)

    mock_sessionmaker = MagicMock()
    monkeypatch.setattr(init_pi_db_flow, "sessionmaker", mock_sessionmaker)

    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value = mock_session

    mock_query = mock_session.query.return_value
    mock_query.count.return_value = 0

    test_db_url = "test_db_url"
    init_pi_db_flow.insert_pi_categories.fn(test_db_url)

    mock_create_engine.assert_called_once_with(test_db_url)
    mock_sessionmaker.assert_called_once_with(bind=mock_create_engine.return_value)
    mock_session.query.assert_called_once_with(pi_db_models.PiCategory)  # Adjust if PiCategory import changes
    mock_query.count.assert_called_once()

    # check each call argument’s attributes
    for call_args, (mission, name, desc, max_delay) in zip(
        mock_session.add.call_args_list,
        init_pi_db_flow.PI_CATEGORY_DATA,
    ):
        (pi_category_obj,) = call_args.args
        assert pi_category_obj.mission == mission
        assert pi_category_obj.name == name
        assert pi_category_obj.description == desc
        assert pi_category_obj.max_delay_seconds == max_delay
    # check the call count matches
    assert mock_session.add.call_count == len(init_pi_db_flow.PI_CATEGORY_DATA)
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()


@pytest.mark.asyncio
async def test_init_pi_database(monkeypatch, mock_prefect):  # pylint: disable=unused-argument
    """
    End-to-end test of the `init_pi_database` flow.

    This test simulates the full flow execution by:
      - Patching environment variables required to build the database URL.
      - Patching `create_schema` and `insert_pi_categories` tasks with mocks.
      - Patching `get_run_logger` to capture log output.
      - Executing the flow with test `FlowEnvArgs`.

    Args:
        monkeypatch: Fixture to replace attributes during the test.

    Assertions:
        - The logger logs the start and end messages.
        - The constructed database URL matches the expected test values.
        - `create_schema` and `insert_pi_categories` are called once with the expected database URL.
    """

    # Patch environment variables used to build db_url
    mock_environ = {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_HOST": "test_host",
        "POSTGRES_PORT": "5432",
        "POSTGRES_PI_DB": "test_db",
    }
    await setup_worklow_test_env(mock_environ)
    monkeypatch.setattr(os, "environ", mock_environ)

    mock_create_schema = MagicMock()
    monkeypatch.setattr(init_pi_db_flow, "create_schema", mock_create_schema)

    mock_insert_pi_categories = MagicMock()
    monkeypatch.setattr(init_pi_db_flow, "insert_pi_categories", mock_insert_pi_categories)

    # Patch get_run_logger to return our mock logger
    mock_logger = MagicMock(name="mock_logger")
    monkeypatch.setattr(init_pi_db_flow, "get_run_logger", MagicMock(return_value=mock_logger))
    expected_db_url = (
        f"postgresql+psycopg2://{mock_environ['POSTGRES_USER']}:"
        f"{mock_environ['POSTGRES_PASSWORD']}@{mock_environ['POSTGRES_HOST']}:"
        f"{mock_environ['POSTGRES_PORT']}/{mock_environ['POSTGRES_PI_DB']}"
    )

    await init_pi_db_flow.init_pi_database(env=FlowEnvArgs(owner_id=OWNER_ID))

    mock_logger.info.assert_any_call(
        "Starting the initialization of the tables for the performance indicator database...",
    )
    mock_create_schema.assert_called_once_with(expected_db_url)
    mock_insert_pi_categories.assert_called_once_with(expected_db_url)
    mock_logger.info.assert_any_call("The initialization of the tables for the performance indicator database finished")


@pytest.mark.asyncio
async def test_publish_task_success(mocker):
    """Test: publish task adds item to catalog with correct collection and asset"""
    # mock CatalogClient and FlowEnv
    mock_catalog_client = MagicMock()
    mock_rs_client = MagicMock()
    mock_rs_client.get_catalog_client.return_value = mock_catalog_client

    mock_flow_env = MagicMock()
    mock_flow_env.rs_client = mock_rs_client

    mocker.patch("rs_workflows.catalog_flow.FlowEnv", return_value=mock_flow_env)

    # mock prefect logger
    mocker.patch("rs_workflows.catalog_flow.get_run_logger")

    # mock os.path.join
    mocker.patch("os.path.join", side_effect=lambda *parts: "/".join(parts))

    # input data
    env = FlowEnvArgs(owner_id="test-owner")

    catalog_collection_identifier = [
        {"S1_GRD": ("S1_GRD", "OUTPUT_GRD_COLLECTION")},  # ← will match
        {"S2_NTC": ("S2_NTC", "OUTPUT_NTC_COLLECTION")},
    ]

    payload_file = MagicMock()
    payload_file.io.output_products = [
        MagicMock(id="GRD", path="s3://output-bucket/grd-output"),
    ]

    items = [
        {
            "id": "S1A_20240101_GRD",
            "geometry": {"type": "Polygon", "coordinates": [[[-10, 40], [10, 40], [10, 60], [-10, 60], [-10, 40]]]},
            "bbox": [-10, 40, 10, 60],
            "properties": {
                "datetime": "2024-01-01T12:00:00Z",
                "product:type": "S1_GRD",
            },
        },
    ]

    # run the async task
    await catalog_flow.publish.fn(env, catalog_collection_identifier, items)

    # assertions
    mock_catalog_client.add_item.assert_called_once()

    # verify correct collection and item
    collection_id, item = mock_catalog_client.add_item.call_args[0]
    assert collection_id == "OUTPUT_GRD_COLLECTION"
    assert item["id"] == "S1A_20240101_GRD"
    expected_datetime = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert item["properties"]["datetime"] == expected_datetime

    # verify asset, temporarily commented out until asset creation is re-enabled
    # asset_key = "S1A_20240101_GRD.zarr"
    # asset = item.assets[asset_key]
    # assert asset.href == "s3://output-bucket/grd-output/S1A_20240101_GRD.zarr"
    # assert asset.title == asset_key
    # assert asset.media_type == "application/vnd+zarr"
    # assert asset.roles == ["data", "metadata"]


@pytest.mark.asyncio
async def test_publish_multiple_items(mocker):
    """Test: multiple items -> multiple add_item calls"""
    mock_catalog_client = MagicMock()
    mock_flow_env = MagicMock(rs_client=MagicMock(get_catalog_client=lambda: mock_catalog_client))
    mocker.patch("rs_workflows.payload_generator.FlowEnv", return_value=mock_flow_env)
    mocker.patch("rs_workflows.payload_generator.get_run_logger")
    mocker.patch("rs_workflows.catalog_flow.FlowEnv", return_value=mock_flow_env)
    mocker.patch("rs_workflows.catalog_flow.get_run_logger")
    mocker.patch("os.path.join", side_effect=lambda *parts: "/".join(parts))

    payload_file = MagicMock()
    payload_file.io.output_products = [
        MagicMock(id="GRD", path="s3://out/grd"),
        MagicMock(id="NTC", path="s3://out/ntc"),
    ]

    catalog_collection_identifier = [
        {"S1_GRD": ("S1_GRD", "COLL_GRD")},
        {"S2_NTC": ("S2_NTC", "COLL_NTC")},
    ]

    items = [
        {
            "id": "item1",
            "properties": {"product:type": "S1_GRD", "datetime": "2024-01-01T00:00:00Z"},
            "geometry": None,
            "bbox": None,
        },
        {
            "id": "item2",
            "properties": {"product:type": "S2_NTC", "datetime": "2024-01-02T00:00:00Z"},
            "geometry": None,
            "bbox": None,
        },
    ]

    await catalog_flow.publish.fn(FlowEnvArgs(owner_id="test"), catalog_collection_identifier, items)

    assert mock_catalog_client.add_item.call_count == 2
    calls = mock_catalog_client.add_item.mock_calls
    assert calls[0][1][0] == "COLL_GRD"
    assert calls[1][1][0] == "COLL_NTC"


@pytest.mark.asyncio
async def test_publish_skips_when_no_matching_output_collection(mocker):
    """Test: no matching output product -> item skipped (no error)"""
    mock_catalog_client = MagicMock()
    mock_flow_env = MagicMock(rs_client=MagicMock(get_catalog_client=lambda: mock_catalog_client))
    mocker.patch("rs_workflows.payload_generator.FlowEnv", return_value=mock_flow_env)
    mocker.patch("rs_workflows.payload_generator.get_run_logger")
    mocker.patch("rs_workflows.catalog_flow.FlowEnv", return_value=mock_flow_env)
    mocker.patch("rs_workflows.catalog_flow.get_run_logger")
    mocker.patch("os.path.join", side_effect=lambda *parts: "/".join(parts))

    catalog_collection_identifier = [{"INVALID": ("INVALID", "COLL_GRD")}]

    items = [
        {
            "id": "item1",
            "properties": {"product:type": "S1_GRD", "datetime": "2024-01-01T00:00:00Z"},
            "geometry": None,
            "bbox": None,
        },
    ]
    with pytest.raises(RuntimeError):
        await catalog_flow.publish.fn(FlowEnvArgs(owner_id="test"), catalog_collection_identifier, items)

        mock_catalog_client.add_item.assert_not_called()
