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
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    Select,
    String,
    Table,
)
from sqlalchemy.sql.dml import Insert, Update

import rs_workflows.record_performance as record_flow_module
from rs_workflows.record_performance import (
    extract_min_datetime,
    update_timeliness_fields,
)

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


@pytest.fixture
def mock_tables(monkeypatch):
    """Create real SQLAlchemy Table objects for testing validate_products."""
    metadata = MetaData()

    product_expected = Table(
        "product_expected",
        metadata,
        Column("id", Integer, primary_key=True),
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
        Column("pi_category_id", Integer),
        Column("unexpected", Boolean, default=False),
        Column("sensing_start_datetime", DateTime, nullable=True),
    )

    product_missing = Table(
        "product_missing",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("flow_run_id", String),
        Column("pi_category_id", Integer, nullable=True),
        Column("eopf_type", String),
        Column("sensing_start_datetime", DateTime, nullable=True),
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

    result = record_flow_module.record_flow_run.fn(start_date="2025-01-01", stop_date="2025-01-02", status="OK")

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

    result = record_flow_module.record_flow_run.fn(stop_date="2025-01-03", status="NOK")

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
    result = record_flow_module.record_product_realised.fn("flow-1", [])
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
            "properties": {"product:type": "S1_GRD", "datetime": "2025-01-01T00:00:00Z"},
        },
    ]

    record_flow_module.record_product_realised.fn("flow-1", stac_items)

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
    stac_items = [{}]  # type: ignore

    with pytest.raises(KeyError):
        record_flow_module.record_product_realised.fn("flow-1", stac_items)

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


def test_record_product_expected_insert_data(mock_db_env, mocker):
    """
    Test that product_expected inserts new records into the product_expected table
    when no existing records are found.
    """
    mock_session, _ = mock_db_env

    # Simulate: no record exists for this flow_run_id + eopf_type
    mock_session.execute.return_value.fetchone.return_value = None

    flow_run_id = "test-flow-id"
    dpr_processor_name = "s3_l0"

    # Mock PayloadSchema
    mock_input_step = MagicMock()
    mock_input_step.inputs = {"AUX1": "S1A_OPER_AUX_RESORB_OPOD_20251008T123456_V20200123T071044_20200123T102814"}
    mock_input_step.outputs = {"out1": "S03DORDOP", "out2": "S03MWRL0_"}

    payload = MagicMock()
    payload.workflow = [mock_input_step]

    # Patch dependencies
    mocker.patch.object(record_flow_module, "extract_min_datetime", return_value="2025-10-08T12:34:56")
    mocker.patch.object(record_flow_module, "get_pi_category_id", return_value=99)

    # Call the function
    record_flow_module.record_product_expected.fn(flow_run_id, dpr_processor_name, payload, ["S03DORDOP", "S03MWRL0_"])

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
    mock_input_step = MagicMock()
    mock_input_step.inputs = {"AUX1": "S1A_OPER_AUX_OBMEMC_PDMC_20210211T000000"}
    mock_input_step.outputs = {"out1": "UNKNOWN_EOPF_TYPE"}

    payload = MagicMock()
    payload.workflow = [mock_input_step]

    with pytest.raises(KeyError):
        record_flow_module.record_product_expected.fn("flow-error", "s3_l0", payload, ["UNKNOWN_EOPF_TYPE"])

    mock_session.rollback.assert_called_once()

    assert any(
        "EOPF type 'UNKNOWN_EOPF_TYPE' not found in eopf_type_lookup" in str(call.args[0])
        for call in mock_logger.error.call_args_list
    )

    mock_session.close.assert_called_once()


def test_inserts_missing_products(mock_db_env, mock_tables, mocker):
    """Should insert into product_missing when realised < min_count and not already recorded."""
    mock_session, _ = mock_db_env
    _, _, _ = mock_tables
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)

    flow_id = "FLOW123"

    # Mock values to simulate DB calls
    expected_result = MagicMock()
    expected_result.fetchall.return_value = [("TYPE1", 5, 10)]  # expected_rows
    expected_result.scalar.return_value = 3  # realised_count (less than min_count)
    expected_result.fetchone.return_value = None  # no existing record in product_missing
    expected_result.rowcount = 1

    # When we call select(pi_category_id, sensing_start_datetime), we need to return something specific
    realised_info_result = MagicMock()
    realised_info_result.fetchone.return_value = (42, "2022-11-01 09:24:39.695148")

    # Patch execute() to return different mocks depending on query
    def execute_side_effect(statement, *args, **kwargs):
        sql_str = str(statement)
        if "FROM product_expected" in sql_str:
            return expected_result
        if "count(" in sql_str:
            return expected_result
        if "FROM product_missing" in sql_str:
            return expected_result
        if "FROM product_realised" in sql_str and "pi_category_id" in sql_str:
            return realised_info_result
        return expected_result

    mock_session.execute.side_effect = execute_side_effect

    record_flow_module.validate_products.fn(flow_id)

    # Assert that we logged the correct warning with the new fields
    logger.warning.assert_any_call(
        "Missing products for TYPE1: inserted 2 into product_missing "
        "(pi_category_id=42, sensing_start_datetime=2022-11-01 09:24:39.695148)",
    )

    mock_session.commit.assert_called_once()


def test_inserts_missing_products_else_branch(mock_db_env, mock_tables, mocker):
    """Test behaviour when no realised_info is found (fetchone() returns None)."""
    mock_session, _ = mock_db_env
    _, _, _ = mock_tables
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)

    flow_id = "FLOW123"

    # Mock values to simulate DB calls
    expected_result = MagicMock()
    expected_result.fetchall.return_value = [("TYPE1", 5, 10)]  # expected_rows
    expected_result.scalar.return_value = 3  # realised_count (less than min_count)
    expected_result.fetchone.return_value = None  # no existing record in product_missing
    expected_result.rowcount = 1

    # Patch execute() to return different mocks depending on query
    def execute_side_effect(statement, *args, **kwargs):
        sql_str = str(statement)
        if "FROM product_expected" in sql_str:
            return expected_result
        if "count(" in sql_str:
            return expected_result
        if "FROM product_missing" in sql_str:
            return expected_result
        if "FROM product_realised" in sql_str and "pi_category_id" in sql_str:
            # This is the else branch case: no realised info found
            realised_info_result = MagicMock()
            realised_info_result.fetchone.return_value = None
            return realised_info_result
        return expected_result

    mock_session.execute.side_effect = execute_side_effect

    record_flow_module.validate_products.fn(flow_id)

    # Assert warning about missing realised info (else branch)
    logger.warning.assert_any_call("No realised info found for TYPE1, leaving category and start_datetime as NULL")

    # Assuming commit still happens:
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

    record_flow_module.validate_products.fn(flow_id)

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

    record_flow_module.validate_products.fn(flow_id)

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

    record_flow_module.validate_products.fn(flow_id)

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

    record_flow_module.validate_products.fn(flow_id)

    logger.error.assert_any_call("Unexpected product type EXTRA: marked all as unexpected")
    mock_session.commit.assert_called_once()


def test_rollback_on_exception(mock_db_env, mock_tables, mocker):
    """Should rollback and log error if exception occurs."""
    mock_session, _ = mock_db_env
    logger = MagicMock()
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=logger)

    mock_session.execute.side_effect = Exception("err!")

    with pytest.raises(Exception):
        record_flow_module.validate_products.fn("FLOW123")

    mock_session.rollback.assert_called_once()
    logger.error.assert_any_call("Error in validate_products: err!")
    mock_session.close.assert_called_once()


def test_no_products_found(mocker, mock_db_env):
    """Should log and return if no product_realised records exist for the flow_run_id."""

    mock_session, _ = mock_db_env
    mock_logger = MagicMock()

    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=mock_logger)
    flow_run_id = "FLOW999"

    # Mock empty DB result
    mock_session.execute.return_value.fetchall.return_value = []

    update_timeliness_fields.fn(flow_run_id=flow_run_id)

    mock_logger.info.assert_any_call("No records provided — skipping updating the timeliness in product_realised.")
    mock_session.execute.assert_called_once()


def test_update_timeliness_fields_exception(mocker, mock_db_env):
    """Should rollback and re-raise if an exception occurs."""

    mock_session, _ = mock_db_env
    mock_logger = MagicMock()
    flow_run_id = "FLOWEXC"

    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=mock_logger)

    # Make execute raise an exception
    mock_session.execute.side_effect = Exception("DB Error")

    # Act & Assert
    with pytest.raises(Exception, match="DB Error"):
        update_timeliness_fields.fn(flow_run_id=flow_run_id)

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()
    mock_logger.error.assert_any_call("Unexpected error in update_timeliness_fields: DB Error")


def test_update_timeliness_fields(mocker, mock_db_env):
    """Tests that product_realised on_time_X_day fields are updated correctly."""
    mock_session, _ = mock_db_env
    mock_logger = MagicMock()
    flow_run_id = "FLOW123"

    # Patch logger
    mocker.patch("rs_workflows.record_performance.get_run_logger", return_value=mock_logger)

    metadata = MetaData()
    product_realised = Table(
        "product_realised",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("flow_run_id", String),
        Column("pi_category_id", Integer),
        Column("catalog_stored_datetime", DateTime),
        Column("origin_date", DateTime),
        Column("on_time_0_day", Boolean),
        Column("on_time_1_day", Boolean),
    )

    pi_category = Table(
        "pi_category",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("max_delay_seconds", Integer),
    )

    mocker.patch(
        "rs_workflows.record_performance.Table",
        side_effect=[pi_category, product_realised],
    )

    # --- Fake product record ---
    fake_product = MagicMock()
    fake_product.id = 1
    fake_product.pi_category_id = 12
    fake_product.catalog_stored_datetime = datetime(2025, 1, 2, 1)
    fake_product.origin_date = datetime(2025, 1, 1)

    # Mock execute() results
    mock_products_result = MagicMock()
    mock_products_result.fetchall.return_value = [fake_product]

    mock_max_delay_result = MagicMock()
    mock_max_delay_result.scalar.return_value = 24 * 3600

    # Side effect for execute() calls
    mock_session.execute.side_effect = [
        mock_products_result,  # select(product_realised)
        mock_max_delay_result,  # select(pi_category.c.max_delay_seconds)
        None,
    ]

    update_timeliness_fields.fn(flow_run_id=flow_run_id)

    mock_session.commit.assert_called_once()
    mock_logger.info.assert_any_call(f"Updated timeliness fields for flow_run_id={flow_run_id}")
    expected_values = {
        "on_time_0_day": False,
        "on_time_1_day": True,
        "on_time_2_day": True,
        "on_time_3_day": True,
        "on_time_7_day": True,
    }

    # Check the actual update values from the Update call
    update_call = None
    for call in mock_session.execute.call_args_list:
        arg0 = call.args[0]
        if (
            isinstance(arg0, Update)
            and arg0._values is not None  # pylint: disable=W0212
            and "on_time_0_day" in arg0._values  # pylint: disable=W0212
        ):
            update_call = arg0
            break

    assert update_call is not None, "Expected an Update call with on_time_0_day"

    for col, expected in expected_values.items():
        assert update_call._values is not None  # pylint: disable=W0212
        actual = update_call._values[col].value  # pylint: disable=W0212
        assert actual is expected, f"Expected {col}={expected}, got {actual}"
