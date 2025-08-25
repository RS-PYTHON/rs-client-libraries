import pytest
from unittest.mock import MagicMock
import rs_workflows.record_flow_run as record_flow_module

def test_record_flow_run_inserts_new_entry(monkeypatch, mocker):
    # Environment variables for all users
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
    def fake_sessionmaker(**kwargs):
        return lambda: mock_session

    mocker.patch("rs_workflows.record_flow_run.sessionmaker", side_effect=fake_sessionmaker)

    # Patch flow_run Table
    mock_table = MagicMock()
    mocker.patch("rs_workflows.record_flow_run.Table", return_value=mock_table)

    mock_session.execute.return_value.fetchone.return_value = None
    record_flow_module.record_flow_run.fn(
        start_date="2025-01-01",
        stop_date="2025-01-02",
        status="OK",
    )
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()