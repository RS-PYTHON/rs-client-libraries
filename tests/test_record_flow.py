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
from sqlalchemy import Boolean, Column, Integer, MetaData, Select, String, Table
from sqlalchemy.sql.dml import Insert, Update

import rs_workflows.record_performance as record_flow_module

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


@pytest.fixture
def mock_tables(monkeypatch):
    """Create real SQLAlchemy Table objects for testing validate_products."""
    metadata = MetaData()

    product_expected = Table(
        "product_expected",
        metadata,
        Column("flow_run_id", String),
        Column("eopf_type", String),
        Column("min_count", Integer),
        Column("max_count", Integer),
    )

    product_realised = Table(
        "product_realised",
        metadata,
        Column("flow_run_id", String),
        Column("eopf_type", String),
        Column("unexpected", Boolean, default=False),
    )

    product_missing = Table(
        "product_missing",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("flow_run_id", String),
        Column("eopf_type", String),
        Column("count", Integer),
    )

    def table_side_effect(name, metadata_obj, **kwargs):
        mapping = {
            "product_expected": product_expected,
            "product_realised": product_realised,
            "product_missing": product_missing,
        }
        if name in mapping:
            return mapping[name]
        raise ValueError(f"Unexpected table name: {name}")

    monkeypatch.setattr("rs_workflows.record_performance.Table", table_side_effect)

    return product_expected, product_realised, product_missing


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


def test_inserts_missing_products(mock_db_env, mock_tables, mocker):
    """Should insert into product_missing when realised < min_count and not already recorded."""
    mock_session, _ = mock_db_env
    _, _, _ = mock_tables
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)

    flow_id = "FLOW123"

    # Make a generic result mock
    result = MagicMock()
    result.fetchall.return_value = [("TYPE1", 5, 10)]
    result.scalar.return_value = 3
    result.fetchone.return_value = None
    result.rowcount = 1

    # Always return the same mock result for any execute()
    mock_session.execute.return_value = result

    record_flow_module.validate_products(flow_id)

    logger.warning.assert_any_call("Missing products for TYPE1: inserted 2 into product_missing")
    mock_session.commit.assert_called_once()


def test_skips_when_missing_already_recorded(mock_db_env, mock_tables, mocker):
    """Should skip insert when missing record already exists."""
    mock_session, _ = mock_db_env
    _, _, _ = mock_tables
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)

    flow_id = "FLOW123"

    # Define reusable mock result objects
    res_expected = MagicMock()
    res_expected.fetchall.return_value = [("TYPE1", 5, 10)]

    res_realised = MagicMock()
    res_realised.scalar.return_value = 3

    res_exists_missing = MagicMock()
    res_exists_missing.fetchone.return_value = (1,)  # means already exists

    res_realised_types = MagicMock()
    res_realised_types.fetchall.return_value = [("TYPE1",)]

    # define a function instead of list for side_effect
    def execute_side_effect(*args, **kwargs):
        sql = str(args[0])
        if "product_expected" in sql:
            return res_expected
        if "count" in sql:  # select(func.count())
            return res_realised
        if "product_missing" in sql:
            return res_exists_missing
        if "distinct" in sql:  # realised types
            return res_realised_types

        return MagicMock()  # generic fallback

    mock_session.execute.side_effect = execute_side_effect

    record_flow_module.validate_products(flow_id)

    logger.info.assert_any_call("Missing products for TYPE1 already recorded, skipping insert")
    mock_session.commit.assert_called_once()


def test_marks_too_many_as_unexpected(mock_db_env, mock_tables, mocker):
    """Should mark product_realised.unexpected=True when realised > max_count."""
    mock_session, _ = mock_db_env
    _, _, _ = mock_tables
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)
    flow_id = "FLOW123"

    res_expected = MagicMock()
    res_expected.fetchall.return_value = [("TYPE1", 1, 2)]

    res_realised_count = MagicMock()
    res_realised_count.scalar.return_value = 5  # too many

    res_realised_types = MagicMock()
    res_realised_types.fetchall.return_value = [("TYPE1",)]

    res_update = MagicMock()
    res_update.rowcount = 3

    def execute_side_effect(*args, **kwargs):
        sql = str(args[0]).lower()
        if "product_expected" in sql:
            return res_expected
        if "count" in sql:
            return res_realised_count
        if "distinct" in sql:
            return res_realised_types
        if "update" in sql:
            return res_update

        generic = MagicMock()
        generic.fetchall.return_value = []
        generic.fetchone.return_value = None
        generic.scalar.return_value = 0
        generic.rowcount = 0
        return generic

    mock_session.execute.side_effect = execute_side_effect

    record_flow_module.validate_products(flow_id)

    logger.error.assert_any_call("Too many products for TYPE1: marked all as unexpected")
    mock_session.commit.assert_called_once()


def test_skips_when_too_many_already_marked(mock_db_env, mock_tables, mocker):
    """Should skip update when already marked unexpected."""
    mock_session, _ = mock_db_env
    _, _, _ = mock_tables
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)
    flow_id = "FLOW123"

    res_expected = MagicMock()
    res_expected.fetchall.return_value = [("TYPE1", 1, 2)]

    res_realised_count = MagicMock()
    res_realised_count.scalar.return_value = 5  # too many

    res_realised_types = MagicMock()
    res_realised_types.fetchall.return_value = [("TYPE1",)]

    res_update = MagicMock()
    res_update.rowcount = 0  # already marked

    def execute_side_effect(*args, **kwargs):
        sql = str(args[0]).lower()
        if "product_expected" in sql:
            return res_expected
        if "count" in sql:
            return res_realised_count
        if "distinct" in sql:
            return res_realised_types
        if "update" in sql:
            return res_update

        generic = MagicMock()
        generic.fetchall.return_value = []
        generic.fetchone.return_value = None
        generic.scalar.return_value = 0
        generic.rowcount = 0
        return generic

    mock_session.execute.side_effect = execute_side_effect

    record_flow_module.validate_products(flow_id)

    logger.info.assert_any_call("Too many products for TYPE1 already marked, skipping update")
    mock_session.commit.assert_called_once()


def test_marks_extra_type_as_unexpected(mock_db_env, mock_tables, mocker):
    """Should mark realised eopf_type as unexpected if not in expected."""
    mock_session, _ = mock_db_env
    _, _, _ = mock_tables
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)

    flow_id = "FLOW123"

    # Mock results
    res_expected = MagicMock()
    res_expected.fetchall.return_value = [("TYPE1", 1, 2)]  # expected rows

    res_realised_count = MagicMock()
    res_realised_count.scalar.return_value = 2  # OK, within min-max

    res_realised_types = MagicMock()
    res_realised_types.fetchall.return_value = [("TYPE1",), ("EXTRA",)]  # extra type

    res_update = MagicMock()
    res_update.rowcount = 3  # simulate marking unexpected

    def execute_side_effect(*args, **kwargs):
        sql = str(args[0]).lower()
        if "product_expected" in sql:
            return res_expected
        if "count" in sql:
            return res_realised_count
        if "distinct" in sql:
            return res_realised_types
        if "update" in sql:  # only updates trigger rowcount
            return res_update

        return MagicMock(rowcount=0)

    mock_session.execute.side_effect = execute_side_effect

    record_flow_module.validate_products(flow_id)

    logger.error.assert_any_call("Unexpected product type EXTRA: marked all as unexpected")
    mock_session.commit.assert_called_once()


def test_rollback_on_exception(mock_db_env, mock_tables, mocker):
    """Should rollback and log error if exception occurs."""
    mock_session, _ = mock_db_env
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)

    mock_session.execute.side_effect = Exception("err!")

    with pytest.raises(Exception):
        record_flow_module.validate_products("FLOW123")

    mock_session.rollback.assert_called_once()
    logger.error.assert_any_call("Error in validate_products: err!")
    mock_session.close.assert_called_once()
