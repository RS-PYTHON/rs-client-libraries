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
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, Integer, MetaData, Select, String, Table
from sqlalchemy.sql.dml import Insert, Update

import rs_workflows.record_performance as record_flow_module
from rs_workflows.record_performance import extract_min_datetime

# pylint: disable = redefined-outer-name, unused-argument, assignment-from-none


@pytest.fixture
def mock_db_env(monkeypatch, mocker):
    """
    Pytest fixture to mock environment variables, DB session, engine, and tables.
    Works for:
        - product_realised
        - pi_category
        - flow_run
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

    mock_logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=mock_logger)
    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_performance.create_engine", return_value=mock_engine)
    mock_session = MagicMock()
    mock_session.close = MagicMock()
    mocker.patch("rs_workflows.record_performance.sessionmaker", return_value=lambda **_: mock_session)

    metadata = MetaData()

    mock_product_table = Table(
        "product_realised",
        metadata,
        Column("id", String, primary_key=True),
        Column("flow_run_id", String),
        Column("eopf_type", String),
    )

    mock_product_expected_table = Table(
        "product_expected",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("flow_run_id", String),
        Column("pi_category_id", Integer),
        Column("eopf_type", String),
        Column("sensing_start_datetime", String),
        Column("min_count", Integer),
        Column("max_count", Integer),
    )

    def table_side_effect(name, metadata, **kwargs):
        if name == "product_realised":
            return mock_product_table
        if name == "pi_category":
            return Table(
                "pi_category",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("mission", String),
                Column("name", String),
            )
        if name == "flow_run":
            return Table(
                "flow_run",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("prefect_flow_id", String),
            )
        if name == "product_expected":
            return mock_product_expected_table
        raise ValueError(f"Unmocked table: {name}")

    mocker.patch("rs_workflows.record_performance.Table", side_effect=table_side_effect)

    return mock_session, mock_product_table


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

    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_performance.create_engine", return_value=mock_engine)
    mock_session = make_mock_session(mocker)

    metadata = MetaData()
    mock_table = Table("flow_run", metadata, Column("id", String), Column("prefect_flow_id", String))
    mocker.patch("rs_workflows.record_performance.Table", return_value=mock_table)

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

    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_performance.create_engine", return_value=mock_engine)
    mock_session = make_mock_session(mocker)

    metadata = MetaData()
    mock_table = Table("flow_run", metadata, Column("id", String), Column("prefect_flow_id", String))
    mocker.patch("rs_workflows.record_performance.Table", return_value=mock_table)

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
    # ?
    mock_session.execute.return_value.fetchone.return_value = None

    stac_items = [
        {
            "stac_discovery": {
                "properties": {"product:type": "S1_GRD", "datetime": "2025-01-01T00:00:00Z"},
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
    # zSimulate existing record
    mock_session.execute.return_value.fetchone.return_value = ["existing-id"]

    stac_items = [
        {
            "stac_discovery": {
                "properties": {"product:type": "S1_GRD", "datetime": "2025-01-01T00:00:00Z"},
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
    # stac_discovery not in "properties"
    stac_items = [{"stac_discovery": {}}]  # type: ignore

    with pytest.raises(KeyError):
        record_flow_module.record_product_realised("flow-1", stac_items)

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_get_pi_category_id_found(mock_db_env):
    """
    Test that get_pi_category_id returns the correct ID when a matching record exists in the DB.
    """
    mock_session, _ = mock_db_env
    mock_session.execute.return_value.fetchone.return_value = [5]

    result = record_flow_module.get_pi_category_id("S01SIWOCN")

    stmt = mock_session.execute.call_args[0][0]
    assert isinstance(stmt, Select)
    assert result == 5
    mock_session.close.assert_called_once()


def test_get_pi_category_id_not_found(mock_db_env):
    """
    Test that get_pi_category_id returns None when no record is found for the given eopf_type.
    """
    mock_session, _ = mock_db_env
    mock_session.execute.return_value.fetchone.return_value = None

    result = record_flow_module.get_pi_category_id("S01SIWOCN")

    assert result is None
    mock_session.close.assert_called_once()


def test_get_pi_category_id_invalid_type(mock_db_env):
    """
    Test that get_pi_category_id returns None when the eopf_type does not match any mapping.
    """
    mock_session, _ = mock_db_env

    result = record_flow_module.get_pi_category_id("UNKNOWN_TYPE")

    assert result is None
    mock_session.close.assert_called_once()


def test_get_pi_category_id_db_exception(mock_db_env):
    """
    Test that get_pi_category_id propagates exceptions from the database and closes the session.
    """
    mock_session, _ = mock_db_env
    mock_session.execute.side_effect = Exception("DB error")

    with pytest.raises(Exception, match="DB error"):
        record_flow_module.get_pi_category_id("S01SIWOCN")

    mock_session.close.assert_called_once()


def test_get_flow_run_id_found(mock_db_env):
    """
    Test that get_flow_run_id returns the correct ID when a matching record exists in the DB.
    """
    mock_session, _ = mock_db_env
    mock_session.execute.return_value.fetchone.return_value = [42]

    result = record_flow_module.get_flow_run_id("fake-prefect-id")

    stmt = mock_session.execute.call_args[0][0]
    assert isinstance(stmt, Select)
    assert result == 42
    mock_session.close.assert_called_once()


def test_get_flow_run_id_not_found(mock_db_env):
    """
    Test that get_flow_run_id returns None when no record is found for the given prefect_flow_id.
    """
    mock_session, _ = mock_db_env
    mock_session.execute.return_value.fetchone.return_value = None

    result = record_flow_module.get_flow_run_id("fake-prefect-id")

    assert result is None
    mock_session.close.assert_called_once()


def test_get_flow_run_id_db_exception(mock_db_env):
    """
    Test that get_flow_run_id propagates exceptions from the database and closes the session.
    """
    mock_session, _ = mock_db_env
    mock_session.execute.side_effect = Exception("DB error")

    with pytest.raises(Exception, match="DB error"):
        record_flow_module.get_flow_run_id("fake-prefect-id")

    mock_session.close.assert_called_once()


@pytest.mark.parametrize(
    "input_item, expected_str, datetime_format",
    [
        ("S1A_OPER_MPL_ORBSCT_20200829T150704_99999999T999999_0025", "20200829T150704", "%Y%m%dT%H%M%S"),
        ("S1A_20200105072204051312", "20200105072204051312", "%Y%m%d%H%M%S%f"),
    ],
)
def test_min_valid_datetime_formats(input_item, expected_str, datetime_format):
    """Test for the minimum datetime from an input_item"""
    expected = datetime.strptime(expected_str, datetime_format)
    assert extract_min_datetime([input_item]) == expected


def test_record_product_expected_insert(mock_db_env, mocker):
    """
    Test that product_expected inserts new records into the product_expected table
    when no existing records are found.
    """
    mock_session, _ = mock_db_env

    # Simulate: no record exists for this flow_run_id + eopf_type
    mock_session.execute.return_value.fetchone.return_value = None

    flow_run_id = "test-flow-id"
    dpr_processor_name = "s3_l0"
    payload = {
        "workflow": [
            {
                "inputs": {"AUX1": "S1A_OPER_AUX_RESORB_OPOD_20251008T123456_V20200123T071044_20200123T102814"},
                "outputs": {"out1": "S03DORDOP", "out2": "S03MWRL0_"},
            },
        ],
    }

    # Patch dependencies
    mocker.patch.object(record_flow_module, "extract_min_datetime", return_value="2025-10-08T12:34:56")
    mocker.patch.object(record_flow_module, "get_pi_category_id", return_value=99)

    # Call the function
    record_flow_module.record_product_expected(flow_run_id, dpr_processor_name, payload)

    # Grab all insert calls
    execute_calls = mock_session.execute.call_args_list
    insert_calls = [call for call in execute_calls if isinstance(call[0][0], Insert)]

    assert len(insert_calls) == 2, "Expected 2 INSERT calls for 2 eopf_type outputs"

    for call in insert_calls:
        insert_stmt = call[0][0]
        assert insert_stmt.table.name == "product_expected"

    assert mock_session.commit.call_count == 2


def test_record_product_expected_rollback_on_keyerror(mocker, mock_db_env):
    """
    Test that product_expected rolls back and logs error if an unknown eopf_type causes KeyError.
    """
    mock_session, _ = mock_db_env

    mock_logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=mock_logger)

    mocker.patch("rs_workflows.record_performance.extract_min_datetime", return_value="2025-01-01T00:00:00")
    mocker.patch("rs_workflows.record_performance.get_pi_category_id", return_value=1)

    # Provide a payload with an unknown eopf_type
    payload = {
        "workflow": [
            {"inputs": {"AUX1": "S1A_OPER_AUX_OBMEMC_PDMC_20210211T000000"}, "outputs": {"out1": "UNKNOWN_EOPF_TYPE"}},
        ],
    }

    with pytest.raises(KeyError):
        record_flow_module.record_product_expected("flow-error", "s3_l0", payload)

    mock_session.rollback.assert_called_once()

    assert any(
        "EOPF type 'UNKNOWN_EOPF_TYPE' not found in eopf_type_lookup" in str(call.args[0])
        for call in mock_logger.error.call_args_list
    )

    mock_session.close.assert_called_once()
