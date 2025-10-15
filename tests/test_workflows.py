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
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from prefect.blocks.system import Secret
from pydantic import SecretStr
from pystac import Item

from rs_client.rs_client import RsClient
from rs_common import prefect_utils
from rs_workflows import (
    auxip_flow,
    cadip_flow,
    catalog_flow,
    dpr_flow,
    init_pi_db_flow,
    on_demand_processing,
    pi_db_models,
    prip_flow,
    staging_flow,
)
from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnvArgs,
    ProcessorEnum,
)
from rs_workflows.pi_db_models import Base

OWNER_ID = "OWNER_ID"
S3_PAYLOAD = "S3_PAYLOAD"
RSPY_WEBSITE = "RSPY_WEBSITE"
RSPY_APIKEY = "RSPY_APIKEY"

# Recursive defaultdict, see: https://stackoverflow.com/a/8702435
MOCK_DICT = lambda: defaultdict(MOCK_DICT)  # type: ignore # pylint: disable=unnecessary-lambda-assignment # noqa: E731

#########
# Mocks #
#########


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
        return [
            Item(id="test1", properties={}, geometry={}, bbox=[], datetime=datetime.now()),
            Item(id="test2", properties={}, geometry={}, bbox=[], datetime=datetime.now()),
        ]

    def get_items(self, *_, **__):
        """Mock stac get_items"""
        return [MockRsClient()] * 2

    def get_collections(self, *_, **__):
        """Mock stac get_collections"""
        return [MockRsClient()] * 2

    def wait_for_job(self, *_, **__):
        """Mock DprClient wait_for_job"""
        return [MOCK_DICT()] * 2

    def wait_for_jobs(self, *_, **__):
        """Mock DprClient wait_for_jobs"""
        return {"job_status": {"status": "successful"}}


@pytest.fixture(autouse=True)
def mock_record_performance_indicators(mocker):
    """
    Auto-applied fixture that mocks the Prefect task `record_performance_indicators`
    so that no real DB or side effects are triggered during tests.
    """
    fake_task = MagicMock()
    fake_task.fn = MagicMock()
    mocker.patch("rs_workflows.dpr_flow.record_performance_indicators", fake_task)

    return fake_task


############
# DPR FLOW #
############


async def mock_s3_download_file(
    s3_path: str,
    to_path: str | Path | None,
    **__: dict[str, Any],
) -> Path:
    """Mock the prefect_utils.s3_download_file function"""
    if not to_path:
        return Path()

    # Mock the downloading of S3_PAYLOAD
    if s3_path.startswith(S3_PAYLOAD):
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
            "RSPY_WEBSITE": RSPY_WEBSITE,
            "RSPY_APIKEY": RSPY_APIKEY,
        },
    ).save(
        prefect_utils.format_env_user(prefect_utils.BLOCK_NAME_ENV_USER, OWNER_ID),
        overwrite=True,
    )


#############
# MAIN FLOW #
#############


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(RsClient, "get_auxip_client", MockRsClient)
@patch.object(RsClient, "get_cadip_client", MockRsClient)
@patch.object(RsClient, "get_catalog_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(RsClient, "get_dpr_client", MockRsClient)
@patch.object(catalog_flow, "datetime", Mock())
async def test_dpr_processing(
    mocker,
    mock_prefect,
    mock_record_performance_indicators,
):  # pylint: disable=unused-argument, redefined-outer-name
    """Test the dpr_processing flow"""

    # Save env vars in prefect secret blocks
    await setup_worklow_test_env({"JUPYTERHUB_API_TOKEN": "JUPYTERHUB_API_TOKEN"})

    # We'll just check that the prefect tasks and flows were called.
    # We don't check the underlying RsClient functions, this is already done in dedicated pytests.
    spied = {
        mocker.spy(prefect_function, "fn"): call_count  # spy on <flow>.fn or <task>.fn = the underlying python function
        for prefect_function, call_count in {
            auxip_flow.search: 1,
            auxip_flow.search_task: 1,
            cadip_flow.search: 1,
            cadip_flow.search_task: 1,
            dpr_flow.read_payload_values: 1,
            dpr_flow.write_payload: 1,
            dpr_flow.run_processor: 1,
            staging_flow.staging_task_auxip: 1,
            staging_flow.staging_task_cadip: 1,
            staging_flow.staging: 2,
            catalog_flow.publish: 1,
        }.items()
    }

    # Run the prefect flow
    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name=ProcessorEnum.S1L0,
        processor_version="1.0",
        pipeline="s1_l0_full",
        dask_cluster_label="cluster_label",
        input_products={},  # Item STAC
        generated_product_to_collection_identifier={"*": "CATALOG_COLLECTION_ID"},
        auxiliary_product_to_collection_identifier={"*": "CATALOG_COLLECTION_ID"},
        processing_mode=["nrt"],  # type: ignore[list-item]
        start_datetime=None,
        end_datetime=None,
        satellite="S3A",
    )
    await on_demand_processing.dpr_processing(dpr_input)

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(RsClient, "get_cadip_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(catalog_flow, "datetime", Mock())
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
            staging_flow.staging_task_cadip: 1,
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
@patch.object(RsClient, "get_auxip_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(RsClient, "get_catalog_client", MockRsClient)
@patch.object(catalog_flow, "datetime", Mock())
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
            catalog_flow.catalog_search: 1,
            catalog_flow.catalog_search_task: 1,
            staging_flow.staging_task_auxip: 1,
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
