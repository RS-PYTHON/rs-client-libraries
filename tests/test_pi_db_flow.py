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

"""Test the Prefect workflows for the initialization of the PI computing database"""

import os
from unittest.mock import MagicMock

import pytest

from rs_workflows import (
    init_pi_db_flow,
    pi_db_models,
)
from rs_workflows.flow_utils import (
    FlowEnvArgs,
)
from rs_workflows.pi_db_models import Base
from tests.conftest import OWNER_ID
from tests.test_workflows import setup_worklow_test_env


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
