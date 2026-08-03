# Copyright 2023-2026 Airbus, CS Group
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

"""Tests the code from the script configure_s3_processing_result_storage"""

import pytest
import prefect.variables as pv

from scripts.configure_s3_processing_result_storage import (
    PREFECT_VAR_NAME,
    get_storage_path,
)


class DummyVariableResult:
    """Simple stand-in for Prefect's Variable.get return object."""

    def __init__(self, payload):
        self._payload = payload

    def get(self, key, default=None):
        """Return payload[key] if present, else default."""
        return self._payload.get(key, default)


def test_returns_absolute_path_stripped_trailing_slash(monkeypatch):
    """Should return absolute_path without a trailing '/'."""
    storage_configuration = [
        {"kind": "shared_disk", "name": "x", "absolute_path": "/mnt/shared/"}
    ]

    def fake_variable_get(var_name):
        """Mock Prefect Variable.get to return storage_configuration."""
        assert var_name == PREFECT_VAR_NAME
        return DummyVariableResult({"storage_configuration": storage_configuration})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))
    assert get_storage_path() == "/mnt/shared"


def test_ignores_non_shared_disk_entries(monkeypatch):
    """Should ignore entries whose kind != 'shared_disk'."""
    storage_configuration = [
        {"kind": "other", "name": "x", "absolute_path": "/mnt/ignore/"},
        {
            "kind": "shared_disk",
            "name": "good",
            "absolute_path": "/mnt/good/",
            "opening_mode": "CREATE_OVERWRITE",
        },
    ]

    def fake_variable_get(_var_name):
        """Return storage_configuration containing both shared and non-shared entries."""
        return DummyVariableResult({"storage_configuration": storage_configuration})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))
    assert get_storage_path() == "/mnt/good"


def test_requires_absolute_path_and_name(monkeypatch):
    """Should skip entries missing name and/or absolute_path."""
    storage_configuration = [
        {"kind": "shared_disk", "name": "", "absolute_path": "/mnt/nok/"},
        {"kind": "shared_disk", "name": "b", "absolute_path": ""},
        {"kind": "shared_disk", "name": "ok", "absolute_path": "/mnt/ok/"},
    ]

    def fake_variable_get(_var_name):
        """Return storage_configuration with invalid entries then one valid one."""
        return DummyVariableResult({"storage_configuration": storage_configuration})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))
    assert get_storage_path() == "/mnt/ok"


def test_opening_mode_must_be_create_overwrite_when_set(monkeypatch):
    """Should only accept entry if opening_mode is CREATE_OVERWRITE when present."""
    storage_configuration = [
        {
            "kind": "shared_disk",
            "name": "ro",
            "absolute_path": "/mnt/ro/",
            "opening_mode": "READ_ONLY",
        },
        {
            "kind": "shared_disk",
            "name": "ok",
            "absolute_path": "/mnt/ok/",
            "opening_mode": "CREATE_OVERWRITE",
        },
    ]

    def fake_variable_get(_var_name):
        """Return storage_configuration with a read-only and a writable entry."""
        return DummyVariableResult({"storage_configuration": storage_configuration})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))
    assert get_storage_path() == "/mnt/ok"


def test_allows_create_overwrite_case_insensitive(monkeypatch):
    """Should treat opening_mode case-insensitively."""
    storage_configuration = [
        {
            "kind": "shared_disk",
            "name": "ok",
            "absolute_path": "/mnt/ok/",
            "opening_mode": "create_overwrite",
        }
    ]

    def fake_variable_get(_var_name):
        """Return a storage configuration using lowercase opening_mode."""
        return DummyVariableResult({"storage_configuration": storage_configuration})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))
    assert get_storage_path() == "/mnt/ok"


def test_returns_first_matching_entry(monkeypatch):
    """Should return the first matching shared_disk entry that satisfies rules."""
    storage_configuration = [
        {
            "kind": "shared_disk",
            "name": "first",
            "absolute_path": "/mnt/first/",
            "opening_mode": "CREATE_OVERWRITE",
        },
        {
            "kind": "shared_disk",
            "name": "second",
            "absolute_path": "/mnt/second/",
            "opening_mode": "CREATE_OVERWRITE",
        },
    ]

    def fake_variable_get(_var_name):
        """Return a list where the first entry should win."""
        return DummyVariableResult({"storage_configuration": storage_configuration})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))
    assert get_storage_path() == "/mnt/first"


def test_raises_when_prefect_variable_cannot_be_loaded(monkeypatch):
    """Should raise RuntimeError when Variable.get throws."""
    def fake_variable_get(_var_name):
        """Simulate Prefect variable loading failure."""
        raise RuntimeError("boom")

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))

    with pytest.raises(RuntimeError) as excinfo:
        get_storage_path()

    assert "Unable to load Prefect variable" in str(excinfo.value)
    assert PREFECT_VAR_NAME in str(excinfo.value)


def test_raises_when_storage_configuration_missing_or_not_list(monkeypatch):
    """Should raise RuntimeError when storage_configuration is missing or not a list."""
    def fake_missing(_var_name):
        """Return payload without storage_configuration."""
        return DummyVariableResult({})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_missing))
    with pytest.raises(RuntimeError) as excinfo1:
        get_storage_path()
    assert "storage_configuration is not a list" in str(excinfo1.value)

    def fake_wrong_type(_var_name):
        """Return storage_configuration with wrong type."""
        return DummyVariableResult({"storage_configuration": "nope"})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_wrong_type))
    with pytest.raises(RuntimeError) as excinfo2:
        get_storage_path()
    assert "storage_configuration is not a list" in str(excinfo2.value)


def test_raises_when_no_matching_entry_found(monkeypatch):
    """Should raise RuntimeError when no writable shared_disk entry is found."""
    storage_configuration = [
        {
            "kind": "shared_disk",
            "name": "ro",
            "absolute_path": "/mnt/ro/",
            "opening_mode": "READ_ONLY",
        },
        {"kind": "other", "name": "x", "absolute_path": "/mnt/x/"},
    ]

    def fake_variable_get(_var_name):
        """Return storage_configuration where no entry matches writable criteria."""
        return DummyVariableResult({"storage_configuration": storage_configuration})

    monkeypatch.setattr(pv.Variable, "get", staticmethod(fake_variable_get))

    with pytest.raises(RuntimeError) as excinfo:
        get_storage_path()

    assert "Failed to get the shared mounted path from the Prefect values" in str(
        excinfo.value
    )
