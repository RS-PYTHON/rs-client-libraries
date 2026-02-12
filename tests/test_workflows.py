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
import tempfile
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import pytest_responses
import responses
from prefect.blocks.system import Secret
from pydantic import SecretStr
from pystac import Asset, Item, ItemCollection
from starlette import status

from rs_client.ogcapi.dpr_client import DprProcessor
from rs_client.stac import catalog_client
from rs_common import prefect_utils
from rs_workflows import (
    auxip_flow,
    catalog_flow,
    init_pi_db_flow,
    on_demand_processing,
    pi_db_models,
)
from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnvArgs,
    ProcessingMode,
)
from rs_workflows.payload_generator import RSPY_TEMP_BUCKET
from rs_workflows.pi_db_models import Base
from tests.conftest import (
    COLLECTION_ID,
    MOCKED_BUCKET,
    MOCKED_RSPY_WEBSITE,
    OWNER_ID,
    S3_ACCESSKEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRETKEY,
)

CONFIG_DIR = Path(__file__).parent / "resources"


##################
# Mock variables #
##################

RSPY_APIKEY = "RSPY_APIKEY"
JUPYTERHUB_API_TOKEN = "JUPYTERHUB_API_TOKEN"
DASK_CLUSTER_LABEL = "DASK_CLUSTER_LABEL"

# Realistic processed items returned by run_processor
ITEMS = {
    "S1_GRD": Item(
        id="S1A_20240101_GRD",
        properties={"product:type": "S1_GRD", "datetime": "2024-01-01T00:00:00Z"},
        geometry={},
        bbox=[],
        datetime=datetime.now(),
    ),
    "S2_NTC": Item(
        id="S2A_20240101_NTC",
        properties={"product:type": "S2_NTC", "datetime": "2024-01-01T00:00:00Z"},
        geometry={},
        bbox=[],
        datetime=datetime.now(),
    ),
}

MAP_PRODUCT_TO_COLLECTION = [
    {"GRD": ("S1_GRD", "OUTPUT_GRD_COLLECTION")},
    {"NTC": ("S2_NTC", "OUTPUT_NTC_COLLECTION")},
]

##################
# Mock functions #
##################


@pytest.fixture
def mocked_tasktable():
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


#####################
# Utility functions #
#####################


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

    # Remove the existing blocks, if any
    user_block_name = prefect_utils.format_env_user(prefect_utils.BLOCK_NAME_ENV_USER, OWNER_ID)
    with suppress(ValueError):
        await Secret.delete(prefect_utils.BLOCK_NAME_ENV_GLOBAL)
    with suppress(ValueError):
        await Secret.delete(user_block_name)

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
            "S3_ACCESSKEY": S3_ACCESSKEY,
            "S3_SECRETKEY": S3_SECRETKEY,
            "S3_REGION": S3_REGION,
            "S3_ENDPOINT": S3_ENDPOINT,
        },
    ).save(user_block_name, overwrite=True)


#############
# DPR flows #
#############


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mocked_stac_catalog_search_inside_collection",
    [["auxip", "catalog"]],
    indirect=True,
    ids=[""],
)
async def test_dpr_processing(
    mocker,
    mocked_s3,
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, /...
    mocked_stac_catalog_get_collection,  # /catalog/collections[/...]
    mocked_stac_catalog_search_inside_collection,  # /auxip/search[/...], /catalog/search[/...]
    mocked_staging_response,  # /processes/staging/execution, /jobs/{job_id}
    mocked_tasktable,  # /dpr/processes/mockup?...
    mocked_dpr_response,  # /dpr/processes/mockup/execution, /dpr/jobs/{job_id}
):  # pylint: disable=unused-argument
    """Test the dpr_processing flow"""

    #########
    # Mocks #
    #########

    # Spy/patch artifact creation to assert keys
    artifact_mock = AsyncMock()
    mocker.patch.object(on_demand_processing, "acreate_markdown_artifact", artifact_mock)

    # Mock the update_eopf_assets function
    mocker.patch("rs_workflows.dpr_flow.update_eopf_assets", return_value=[ITEMS.values(), ITEMS.keys()])

    # Upload a mock processor log file
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(b"Dummy processor log contents\n")
        tmp.flush()
        mocked_s3.upload_file(tmp.name, MOCKED_BUCKET, "mockup.processor.log")

    # Mock posting of the items in the catalog
    item_collections = [list(col.values())[0][1] for col in MAP_PRODUCT_TO_COLLECTION]
    for collection_id in item_collections:
        responses.post(
            f"{MOCKED_RSPY_WEBSITE}/catalog/collections/{OWNER_ID}:{collection_id}/items",
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
        processor_name=DprProcessor.MOCKUP,
        processor_version="1.0",
        pipeline="mockup_full",
        dask_cluster_label=DASK_CLUSTER_LABEL,
        input_products=[{"input_name": ("dummy_id", "dummy_coll")}],
        generated_product_to_collection_identifier=MAP_PRODUCT_TO_COLLECTION,
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

    # Verify correct collection and item
    assert spy_add_item.call_count == len(ITEMS)
    for i in range(len(ITEMS)):
        _, collection_id, item = spy_add_item.call_args_list[i][0]
        expected_item = list(ITEMS.values())[i]
        assert collection_id == list(MAP_PRODUCT_TO_COLLECTION[i].values())[0][1]
        assert item == expected_item

    # verify asset, temporarily commented out until asset creation is re-enabled
    # asset_key = "S1A_20240101_GRD.zarr"
    # asset = item.assets[asset_key]
    # assert asset.href == f"s3://{MOCKED_BUCKET}/grd-output/S1A_20240101_GRD.zarr"
    # assert asset.title == asset_key
    # assert asset.media_type == "application/vnd+zarr"
    # assert asset.roles == ["data", "metadata"]

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
):
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
        processor_name=DprProcessor.MOCKUP,
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
):
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
):
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
):
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
):
    """Test the catalog_search flow"""
    await setup_worklow_test_env()
    await catalog_flow.catalog_search(env=FlowEnvArgs(owner_id=OWNER_ID), catalog_cql2={"filter": {}})


@pytest.mark.asyncio
async def test_publish_skips_when_no_matching_output_collection(
    mocker,
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, ...
):
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


################
# PI computing #
################


def test_create_schema(monkeypatch):  # pylint: disable=unused-argument
    """
    Tests that the `create_schema` task for the PI computing correctly triggers table creation.

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
async def test_init_pi_database(monkeypatch):  # pylint: disable=unused-argument
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
