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


from unittest.mock import MagicMock

from sqlalchemy import Column, MetaData, String, Table

import rs_workflows.record_flow_run as record_flow_module


def test_record_flow_run_inserts_new_entry(monkeypatch, mocker):
    """Test that record_flow_run inserts a new entry when no existing record is found"""
    # Environment variables
    env_global = {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_HOST": "test_host",
        "POSTGRES_PORT": "5432",
        "POSTGRES_PI_DB": "test_db",
    }
    for key, value in env_global.items():
        monkeypatch.setenv(key, value)

    mocker.patch("rs_workflows.record_flow_run.get_run_logger", return_value=MagicMock())
    mocker.patch("rs_workflows.record_flow_run.select", return_value=MagicMock())

    # Patch runtime.flow_run
    mock_flow_run = MagicMock()
    mock_flow_run.id = "fake-id"
    mock_flow_run.parent_flow_run_id = "parent-id"
    mock_flow_run.parameters = {
        "flow_run_type": "systematic",
        "mission": "sentinel-1",
    }
    mocker.patch.object(record_flow_module.runtime, "flow_run", mock_flow_run)

    # Patch version("dask")
    mocker.patch("rs_workflows.record_flow_run.version", return_value="2025.0.0")

    # Patch SQLAlchemy engine
    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_flow_run.create_engine", return_value=mock_engine)

    # Mock session object (with .close())
    mock_session = MagicMock()
    mock_session.close = MagicMock()

    # Fake sessionmaker that always returns mock_session
    def fake_sessionmaker(**kwargs):  # pylint: disable = unused-argument
        return lambda: mock_session

    mocker.patch("rs_workflows.record_flow_run.sessionmaker", side_effect=fake_sessionmaker)

    # Patch flow_run Table
    mock_table = MagicMock()
    mocker.patch("rs_workflows.record_flow_run.Table", return_value=mock_table)

    # Simulate no existing record
    mock_session.execute.return_value.fetchone.return_value = None

    record_flow_module.record_flow_run.fn(
        start_date="2025-01-01",
        stop_date="2025-01-02",
        status="OK",
    )

    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()
    mock_session.execute.assert_called()  # Insert should have been attempted


def test_record_flow_run_updates_existing_entry(monkeypatch, mocker):
    """Test that record_flow_run updates an existing entry instead of inserting"""
    env_global = {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_HOST": "test_host",
        "POSTGRES_PORT": "5432",
        "POSTGRES_PI_DB": "test_db",
    }
    for key, value in env_global.items():
        monkeypatch.setenv(key, value)

    mocker.patch("rs_workflows.record_flow_run.get_run_logger", return_value=MagicMock())
    mocker.patch("rs_workflows.record_flow_run.select", return_value=MagicMock())

    # Patch runtime.flow_run
    mock_flow_run = MagicMock()
    mock_flow_run.id = "fake-id"
    mock_flow_run.parent_flow_run_id = "parent-id"
    mock_flow_run.parameters = {"flow_run_type": "systematic", "mission": "sentinel-1"}
    mocker.patch.object(record_flow_module.runtime, "flow_run", mock_flow_run)

    # Patch version("dask")
    mocker.patch("rs_workflows.record_flow_run.version", return_value="2025.0.0")

    # Patch SQLAlchemy engine + session
    mock_engine = MagicMock()
    mocker.patch("rs_workflows.record_flow_run.create_engine", return_value=mock_engine)

    mock_session = MagicMock()
    mock_session.close = MagicMock()

    def fake_sessionmaker(**kwargs):  # pylint: disable = unused-argument
        return lambda: mock_session

    mocker.patch("rs_workflows.record_flow_run.sessionmaker", side_effect=fake_sessionmaker)

    # Build a real-ish SQLAlchemy Table for mocking
    metadata = MetaData()
    mock_table = Table(
        "flow_run",
        metadata,
        Column("id", String, primary_key=True),
        Column("prefect_flow_id", String),
        Column("dpr_processing_start_datetime", String),
        Column("dpr_processing_stop_datetime", String),
        Column("dpr_processing_status", String),
    )
    mocker.patch("rs_workflows.record_flow_run.Table", return_value=mock_table)

    # Simulate that a record already exists
    mock_session.execute.return_value.fetchone.return_value = {"id": 123}

    # Call the task function
    record_flow_module.record_flow_run.fn(
        stop_date="2025-01-03",
        status="NOK",
    )

    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()
    mock_session.execute.assert_called()  # update should have been executed
