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

"""Unit tests for AUXIP workflow helpers and flows."""

# pylint: disable=redefined-outer-name,unused-argument

from contextlib import nullcontext
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from pystac import Item, ItemCollection

from rs_workflows import auxip_flow
from rs_workflows.flow_utils import FlowEnvArgs


@pytest.fixture
def mock_auxip_logger(monkeypatch, mocker):
    """Replace Prefect run logger with a plain mock."""
    logger = mocker.Mock()
    monkeypatch.setattr(auxip_flow, "get_run_logger", lambda: logger)
    return logger


@pytest.mark.asyncio
async def test_search_returns_found_items(monkeypatch, mocker, mock_auxip_logger):
    """Test AUXIP search delegates to Auxip client and returns the results."""
    found = ItemCollection([])
    env = FlowEnvArgs(owner_id="me")
    auxip_client = mocker.Mock()
    auxip_client.search.return_value = found
    flow_env = mocker.Mock()
    flow_env.start_span.return_value = nullcontext()
    flow_env.rs_client.get_auxip_client.return_value = auxip_client
    monkeypatch.setattr(auxip_flow, "FlowEnv", lambda env: flow_env)

    result = await auxip_flow.search.fn(env, {"filter": {"foo": "bar"}, "limit": 3, "sortby": []})

    assert result is found
    auxip_client.search.assert_called_once_with(method="POST", stac_filter={"foo": "bar"}, max_items=3, sortby=[])


@pytest.mark.asyncio
async def test_search_raises_when_empty_and_error_if_empty(monkeypatch, mocker, mock_auxip_logger):
    """Test AUXIP search raises when no items are found and error_if_empty is set."""
    env = FlowEnvArgs(owner_id="me")
    auxip_client = mocker.Mock()
    auxip_client.search.return_value = ItemCollection([])
    flow_env = mocker.Mock()
    flow_env.start_span.return_value = nullcontext()
    flow_env.rs_client.get_auxip_client.return_value = auxip_client
    monkeypatch.setattr(auxip_flow, "FlowEnv", lambda env: flow_env)

    with pytest.raises(ValueError, match="No Auxip product found"):
        await auxip_flow.search.fn(env, {"filter": {"foo": "bar"}}, error_if_empty=True)


class _SubmitResult:  # pylint: disable=too-few-public-methods
    """Small future-like stub returning a precomputed result."""

    def __init__(self, result):
        self._result = result

    def result(self):
        """Return the stored result."""
        return self._result


class _TaskStub:  # pylint: disable=too-few-public-methods
    """Minimal Prefect task stub implementing with_options/submit."""

    def __init__(self, result):
        self._result = result
        self.with_options_calls = []
        self.submit_calls = []

    def with_options(self, **kwargs):
        """Store timeout options and return self."""
        self.with_options_calls.append(kwargs)
        return self

    def submit(self, *args, **kwargs):
        """Store submit calls and return a future-like stub."""
        self.submit_calls.append((args, kwargs))
        return _SubmitResult(self._result)


@pytest.mark.asyncio
async def test_auxip_staging_returns_early_when_search_is_empty(monkeypatch, mocker, mock_auxip_logger):
    """Test AUXIP staging exits early when search returns no items."""
    env = FlowEnvArgs(owner_id="me")
    flow_env = mocker.Mock()
    flow_env.start_span.return_value = nullcontext()
    flow_env.serialize.return_value = {"owner_id": "me"}
    monkeypatch.setattr(auxip_flow, "FlowEnv", lambda env: flow_env)
    search_stub = _TaskStub(ItemCollection([]))
    monkeypatch.setattr(auxip_flow, "search_task", search_stub)

    result = await auxip_flow.auxip_staging.fn(env, {"filter": {}}, "collection")

    assert result == (True, None)


@pytest.mark.asyncio
async def test_auxip_staging_success(monkeypatch, mocker, mock_auxip_logger):
    """Test AUXIP staging returns staged catalog items and creates artifact on success."""
    env = FlowEnvArgs(owner_id="me")
    item_datetime = datetime.now(UTC)
    source_item = Item(id="item-1", geometry=None, bbox=None, datetime=item_datetime, properties={})
    catalog_item = Item(id="item-1", geometry=None, bbox=None, datetime=item_datetime, properties={})
    flow_env = mocker.Mock()
    flow_env.start_span.return_value = nullcontext()
    flow_env.serialize.return_value = {"owner_id": "me"}
    catalog_client = mocker.Mock()
    catalog_client.get_items.return_value = [catalog_item]
    flow_env.rs_client.get_catalog_client.return_value = catalog_client
    monkeypatch.setattr(auxip_flow, "FlowEnv", lambda env: flow_env)
    monkeypatch.setattr(auxip_flow, "search_task", _TaskStub(ItemCollection([source_item])))
    monkeypatch.setattr(
        auxip_flow,
        "staging_task",
        _TaskStub({"job-1": {"status": "successful", "jobID": "1", "message": "ok"}}),
    )
    artifact_mock = AsyncMock()
    monkeypatch.setattr(auxip_flow, "acreate_markdown_artifact", artifact_mock)

    status_ok, items = await auxip_flow.auxip_staging.fn(env, {"filter": {}}, "collection")

    assert status_ok is True
    assert items is not None
    assert [item.id for item in items] == ["item-1"]
    artifact_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_auxip_staging_failure_skips_artifact(monkeypatch, mocker, mock_auxip_logger):
    """Test AUXIP staging returns False when one staging job failed."""
    env = FlowEnvArgs(owner_id="me")
    item_datetime = datetime.now(UTC)
    source_item = Item(id="item-1", geometry=None, bbox=None, datetime=item_datetime, properties={})
    catalog_item = Item(id="item-1", geometry=None, bbox=None, datetime=item_datetime, properties={})
    flow_env = mocker.Mock()
    flow_env.start_span.return_value = nullcontext()
    flow_env.serialize.return_value = {"owner_id": "me"}
    catalog_client = mocker.Mock()
    catalog_client.get_items.return_value = [catalog_item]
    flow_env.rs_client.get_catalog_client.return_value = catalog_client
    monkeypatch.setattr(auxip_flow, "FlowEnv", lambda env: flow_env)
    monkeypatch.setattr(auxip_flow, "search_task", _TaskStub(ItemCollection([source_item])))
    monkeypatch.setattr(
        auxip_flow,
        "staging_task",
        _TaskStub({"job-1": {"status": "failed", "jobID": "1", "message": "boom"}}),
    )
    artifact_mock = AsyncMock()
    monkeypatch.setattr(auxip_flow, "acreate_markdown_artifact", artifact_mock)

    status_ok, items = await auxip_flow.auxip_staging.fn(env, {"filter": {}}, "collection")

    assert status_ok is False
    assert items is not None
    artifact_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_demand_auxip_staging_builds_valcover_filter(monkeypatch):
    """Test on-demand AUXIP staging builds a ValCover filter and forwards it."""
    env = FlowEnvArgs(owner_id="me")
    create_filter_mock = MagicMock(return_value={"op": "and"})
    staged_items = ItemCollection([])
    staging_mock = AsyncMock(return_value=(True, staged_items))
    auxip_staging_task = MagicMock()
    auxip_staging_task.fn = staging_mock
    monkeypatch.setattr(auxip_flow, "create_valcover_filter", create_filter_mock)
    monkeypatch.setattr(auxip_flow, "auxip_staging", auxip_staging_task)

    result = await auxip_flow.on_demand_auxip_staging.fn(
        env,
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        "AUX_TEST",
        "collection",
    )

    assert result == (True, staged_items)
    create_filter_mock.assert_called_once_with("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "AUX_TEST")
    staging_mock.assert_awaited_once_with(
        env=env,
        cql2_filter={"filter": {"op": "and"}},
        catalog_collection_identifier="collection",
    )


@pytest.mark.asyncio
async def test_task_wrappers_delegate_to_underlying_flows(monkeypatch):
    """Test task wrappers delegate to the corresponding flow functions."""
    search_mock = AsyncMock(return_value="search-result")
    staging_mock = AsyncMock(return_value=("ok", None))
    unzip_mock = AsyncMock(return_value="item")
    monkeypatch.setattr(auxip_flow.search, "fn", search_mock)
    monkeypatch.setattr(auxip_flow.auxip_staging, "fn", staging_mock)
    monkeypatch.setattr(auxip_flow.asset_unzip_decompress, "fn", unzip_mock)

    assert await auxip_flow.search_task.fn(1, a=2) == "search-result"
    assert await auxip_flow.auxip_staging_task.fn(3, b=4) == ("ok", None)
    assert await auxip_flow.auxip_unzip_decompress_task.fn(5, c=6) == "item"
