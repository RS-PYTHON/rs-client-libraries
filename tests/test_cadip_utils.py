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

"""Unit tests for rs_workflows.utils.cadip.is_evicted and get_cadip_station."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from rs_workflows.utils import cadip


def _item_with_eviction_asset(eviction_datetime: str | None) -> MagicMock:
    item = MagicMock()
    if eviction_datetime is None:
        item.assets = {"asset1": MagicMock(extra_fields={})}
    else:
        item.assets = {"asset1": MagicMock(extra_fields={"eviction_datetime": eviction_datetime})}
    return item


# --------------------------------------------------------------------------- #
# is_evicted
# --------------------------------------------------------------------------- #
def test_is_evicted_false_when_no_eviction_datetime_asset():
    """An item whose assets carry no 'eviction_datetime' is not evicted."""
    item = _item_with_eviction_asset(None)
    evicted, eviction_date = cadip.is_evicted(item)
    assert evicted is False
    assert eviction_date is None


def test_is_evicted_true_when_eviction_date_in_past():
    """An 'eviction_datetime' in the past marks the item as evicted."""
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    item = _item_with_eviction_asset(past)
    evicted, eviction_date = cadip.is_evicted(item)
    assert evicted is True
    assert eviction_date is not None


def test_is_evicted_false_when_eviction_date_in_future():
    """An 'eviction_datetime' in the future does not yet mark the item as evicted."""
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat().replace("+00:00", "Z")
    item = _item_with_eviction_asset(future)
    evicted, eviction_date = cadip.is_evicted(item)
    assert evicted is False
    assert eviction_date is not None


# --------------------------------------------------------------------------- #
# get_cadip_station
# --------------------------------------------------------------------------- #
def _patch_get_cadip_station(
    mocker,
    *,
    item_count=1,
    item=None,
    evicted=(False, None),
    published=(True, datetime.now(timezone.utc)),
):
    mocker.patch.object(cadip, "get_run_logger", return_value=MagicMock())
    item_col = MagicMock()
    item_col.__len__.return_value = item_count
    item_col.__getitem__.return_value = item
    flow_env = MagicMock()
    flow_env.rs_client.get_cadip_client.return_value.search.return_value = item_col
    mocker.patch.object(cadip, "is_evicted", return_value=evicted)
    mocker.patch.object(cadip, "is_published", return_value=published)
    return flow_env


async def test_get_cadip_station_returns_none_when_session_not_found(mocker):
    """When the search does not return exactly one item, the station is not found."""
    flow_env = _patch_get_cadip_station(mocker, item_count=0)
    result = await cadip.get_cadip_station.fn(flow_env, "SESSION_1", ["s1_sgs"])
    assert result is None


async def test_get_cadip_station_returns_none_when_evicted(mocker):
    """An evicted session is not returned as available, even if published."""
    item = MagicMock()
    flow_env = _patch_get_cadip_station(mocker, item=item, evicted=(True, datetime.now(timezone.utc)))
    result = await cadip.get_cadip_station.fn(flow_env, "SESSION_1", ["s1_sgs"])
    assert result is None


async def test_get_cadip_station_returns_none_when_not_published(mocker):
    """A found, non-evicted session that is not published is not returned as available."""
    item = MagicMock()
    flow_env = _patch_get_cadip_station(mocker, item=item, published=(False, None))
    result = await cadip.get_cadip_station.fn(flow_env, "SESSION_1", ["s1_sgs"])
    assert result is None


async def test_get_cadip_station_returns_none_when_no_collection_link(mocker):
    """A published session without a 'collection' link cannot resolve a station name."""
    item = MagicMock()
    item.links = []
    flow_env = _patch_get_cadip_station(mocker, item=item, published=(True, datetime.now(timezone.utc)))
    result = await cadip.get_cadip_station.fn(flow_env, "SESSION_1", ["s1_sgs"])
    assert result is None


async def test_get_cadip_station_returns_station_name_when_published(mocker):
    """A published, non-evicted session resolves to the station name from its collection link."""
    item = MagicMock()
    collection_link = MagicMock(rel="collection", href="https://cadip.example/collections/s1_sgs/")
    other_link = MagicMock(rel="self", href="https://cadip.example/items/SESSION_1")
    item.links = [other_link, collection_link]
    flow_env = _patch_get_cadip_station(mocker, item=item, published=(True, datetime.now(timezone.utc)))
    result = await cadip.get_cadip_station.fn(flow_env, "SESSION_1", ["s1_sgs"])
    assert result == "s1_sgs"
