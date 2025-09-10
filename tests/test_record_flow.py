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
"""Module for testing <<record flow run>>"""


import sys
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, MetaData, String, Table
from sqlalchemy.sql.dml import Insert, Update

import rs_workflows.record_performance as record_flow_module

# pylint: disable = redefined-outer-name, unused-argument, assignment-from-none


@pytest.fixture
def mock_db_env(monkeypatch, mocker):
    """
    Pytest fixture to mock environment variables required for database connections.

    Sets the following environment variables:
        - POSTGRES_USER
        - POSTGRES_PASSWORD
        - POSTGRES_HOST
        - POSTGRES_PORT
        - POSTGRES_PI_DB

    Args:
        monkeypatch (pytest.MonkeyPatch): pytest fixture used to temporarily set environment variables.

    This fixture ensures that tests depending on database connection settings
    can run without relying on real environment variables.
    """
    # Patch env vars
    env_global = {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_HOST": "test_host",
        "POSTGRES_PORT": "5432",
        "POSTGRES_PI_DB": "test_db",
    }
    for key, value in env_global.items():
        monkeypatch.setenv(key, value)

    # Patch logger
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=MagicMock())

    # Patch DB + engine
    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_performance.create_engine", return_value=mock_engine)
    mock_session = MagicMock()
    mock_session.close = MagicMock()
    mocker.patch("rs_workflows.record_performance.sessionmaker", return_value=lambda **_: mock_session)

    # Patch table
    metadata = MetaData()
    mock_table = Table(
        "product_realised",
        metadata,
        Column("id", String, primary_key=True),
        Column("flow_run_id", String),
        Column("eopf_type", String),
    )
    mocker.patch("rs_workflows.record_performance.Table", return_value=mock_table)

    # Patch helper
    mocker.patch("rs_workflows.record_performance.get_pi_category_id", return_value="mock-cat")

    return mock_session, mock_table


def make_mock_session(mocker):
    """
    Create and patch a mock SQLAlchemy session for testing.

    This function creates a MagicMock to simulate a database session,
    patches `sessionmaker` in `rs_workflows.record_performance` to return
    this mock session, and ensures that the session has a `.close()` method
    for proper cleanup in the tested code.

    Args:
        mocker: pytest-mock fixture used to patch `sessionmaker`.

    Returns:
        MagicMock: The mocked database session object.
    """
    mock_session = MagicMock()
    mock_session.close = MagicMock()
    mocker.patch("rs_workflows.record_performance.sessionmaker", return_value=lambda: mock_session)
    return mock_session


def test_record_flow_run_inserts_new_entry(mock_db_env, mocker):
    """It should insert a new row when no record exists."""
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=MagicMock())

    # Fake flow_run context
    mock_flow_run = MagicMock()
    mock_flow_run.id = "fake-id"
    mock_flow_run.parent_flow_run_id = "parent-id"
    mocker.patch.object(record_flow_module.runtime, "flow_run", mock_flow_run)

    mocker.patch("rs_workflows.record_performance.version", return_value="2025.0.0")
    mocker.patch("rs_workflows.record_performance.sys", new=sys)

    # Mock db + engine
    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_performance.create_engine", return_value=mock_engine)
    mock_session = make_mock_session(mocker)

    # Table
    metadata = MetaData()
    mock_table = Table("flow_run", metadata, Column("id", String), Column("prefect_flow_id", String))
    mocker.patch("rs_workflows.record_performance.Table", return_value=mock_table)

    # No existing record
    mock_session.execute.return_value.fetchone.return_value = None
    mock_session.execute.return_value.scalar.return_value = 999

    result = record_flow_module.record_flow_run(start_date="2025-01-01", stop_date="2025-01-02", status="OK")

    # Verify it was insert
    stmt = mock_session.execute.call_args[0][0]
    assert isinstance(stmt, Insert)
    assert stmt.table.name == "flow_run"
    assert result == 999
    mock_session.commit.assert_called_once()


def test_record_flow_run_updates_existing_entry(mock_db_env, mocker):
    """
    Test that record_flow_run updates an existing row if a record already exists for the flow run.

    This ensures that:
    1. When a record for the current flow run already exists, an UPDATE statement is executed.
    2. The correct table ('flow_run') is targeted for the update.
    3. The function returns the existing record's ID.
    4. The database session commit is called exactly once to persist changes.
    5. The flow run context, logger, and versioning are correctly mocked for the test environment.

    Args:
        mock_db_env: A fixture providing a mocked database session and engine.
        mocker: Pytest-mock fixture used for patching modules, functions, and objects.
    """
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=MagicMock())

    # Fake flow_run context
    mock_flow_run = MagicMock()
    mock_flow_run.id = "fake-id"
    mock_flow_run.parent_flow_run_id = "parent-id"
    mocker.patch.object(record_flow_module.runtime, "flow_run", mock_flow_run)

    mocker.patch("rs_workflows.record_performance.version", return_value="2025.0.0")
    mocker.patch("rs_workflows.record_performance.sys", new=sys)

    # Mock db + engine
    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_performance.create_engine", return_value=mock_engine)
    mock_session = make_mock_session(mocker)

    # Table
    metadata = MetaData()
    mock_table = Table("flow_run", metadata, Column("id", String), Column("prefect_flow_id", String))
    mocker.patch("rs_workflows.record_performance.Table", return_value=mock_table)

    # Existing record found
    mock_session.execute.return_value.fetchone.return_value = ["existing-id"]

    result = record_flow_module.record_flow_run(stop_date="2025-01-03", status="NOK")

    stmt = mock_session.execute.call_args[0][0]
    assert isinstance(stmt, Update)
    assert stmt.table.name == "flow_run"  # type: ignore
    assert result == "existing-id"
    mock_session.commit.assert_called_once()


def test_record_product_realised_no_items(mock_db_env):
    """
    Test that record_product_realised does nothing when no STAC items are provided.

    This ensures that:
    1. If the stac_items list is empty, no database operations are performed.
    2. No INSERT or UPDATE statements are executed.
    3. The database session commit is not called.
    4. The function returns None.

    Args:
        mock_db_env: A fixture providing a mocked database session and engine.
    """
    mock_session, _ = mock_db_env
    result = record_flow_module.record_product_realised("flow-1", [])
    assert result is None
    mock_session.execute.assert_not_called()
    mock_session.commit.assert_not_called()


def test_record_product_realised_insert(mock_db_env):
    """
    Test that record_product_realised inserts a new record into the product_realised table
    when no existing record is found for the given flow_run_id.

    This ensures that:
    1. When no record exists, an INSERT statement is executed.
    2. The correct table ('product_realised') is targeted for the insert.
    3. The database session commit is called to persist the new record.

    Args:
        mock_db_env: A fixture providing a mocked database session and engine.
    """
    mock_session, _ = mock_db_env
    # No existing record
    mock_session.execute.return_value.fetchone.return_value = None

    stac_items = [
        {
            "stac_discovery": {
                "properties": {"eopf:type": "S1_GRD", "datetime": "2025-01-01T00:00:00Z"},
            },
        },
    ]

    record_flow_module.record_product_realised("flow-1", stac_items)

    stmt = mock_session.execute.call_args[0][0]
    assert isinstance(stmt, Insert)
    assert stmt.table.name == "product_realised"
    mock_session.commit.assert_called()


def test_record_product_realised_update(mock_db_env):
    """
    Test that record_product_realised updates an existing record in the product_realised table.

    This ensures that:
    1. When a record already exists for the given flow_run_id, an UPDATE statement is executed.
    2. The correct table ('product_realised') is targeted for the update.
    3. The database session commit is called to persist changes.

    Args:
        mock_db_env: A fixture providing a mocked database session and engine.
    """
    mock_session, _ = mock_db_env
    # Simulate existing record
    mock_session.execute.return_value.fetchone.return_value = ["existing-id"]

    stac_items = [
        {
            "stac_discovery": {
                "properties": {"eopf:type": "S1_GRD", "datetime": "2025-01-01T00:00:00Z"},
            },
        },
    ]

    record_flow_module.record_product_realised("flow-1", stac_items)

    stmt = mock_session.execute.call_args[0][0]
    assert isinstance(stmt, Update)
    assert stmt.table.name == "product_realised"  # type: ignore
    mock_session.commit.assert_called()


def test_record_product_realised_keyerror_triggers_rollback(mock_db_env):
    """
    Test that record_product_realised triggers a KeyError and rolls back the DB session
    when a required key is missing in the STAC item.

    This ensures that:
    1. A KeyError is raised if 'properties' or other expected keys are missing in the STAC item.
    2. The database session rollback is called exactly once.
    3. The database session is properly closed in the finally block.

    Args:
        mock_db_env: A fixture providing a mocked database session and engine.
    """
    mock_session, _ = mock_db_env
    # stac_discovery missing "properties"
    stac_items = [{"stac_discovery": {}}]  # type: ignore

    with pytest.raises(KeyError):
        record_flow_module.record_product_realised("flow-1", stac_items)

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()
