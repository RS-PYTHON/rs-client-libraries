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

"""Unit tests for EarthDataHub workflow helpers and flows."""

from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest
from pystac import ItemCollection

from rs_workflows import earthdatahub_flow
from rs_workflows.flow_utils import FlowEnvArgs


@pytest.mark.asyncio
async def test_search_returns_found_items(monkeypatch, mocker):
    """Test EarthDataHub search delegates to the EarthDataHub client and returns the results."""
    found = ItemCollection([])
    env = FlowEnvArgs(owner_id="me")
    earthdatahub_client = mocker.Mock()
    earthdatahub_client.search.return_value = found
    flow_env = mocker.Mock()
    flow_env.start_span.return_value = nullcontext()
    flow_env.rs_client.get_earthdatahub_client.return_value = earthdatahub_client
    monkeypatch.setattr(earthdatahub_flow.stac, "get_run_logger", mocker.Mock(return_value=mocker.Mock()))
    monkeypatch.setattr(earthdatahub_flow.stac, "FlowEnv", lambda env: flow_env)

    result = await earthdatahub_flow.search.fn(env, {"filter": {"foo": "bar"}, "limit": 3, "sortby": []})

    assert result is found
    earthdatahub_client.search.assert_called_once_with(
        method="POST",
        stac_filter={"foo": "bar"},
        max_items=3,
        collections=None,
        sortby=[],
        timestamp=None,
    )


@pytest.mark.asyncio
async def test_task_wrapper_delegates_to_search(monkeypatch):
    """Test task wrapper delegates to the EarthDataHub search flow."""
    search_mock = AsyncMock(return_value="search-result")
    monkeypatch.setattr(earthdatahub_flow.search, "fn", search_mock)

    assert await earthdatahub_flow.earthdatahub_search_task.fn(1, a=2) == "search-result"
    search_mock.assert_awaited_once_with(1, a=2)
