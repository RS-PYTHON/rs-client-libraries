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

"""Unit tests for rs_workflows.utils.catalog.is_unpublished and is_published."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from rs_workflows.utils.catalog import is_published, is_unpublished


def _item_with_properties(properties: dict) -> MagicMock:
    item = MagicMock()
    item.properties = properties
    return item


# --------------------------------------------------------------------------- #
# is_unpublished
# --------------------------------------------------------------------------- #
def test_is_unpublished_false_when_no_unpublished_property():
    """An item without an 'unpublished' property is not unpublished."""
    item = _item_with_properties({})
    unpublished, unpublished_date = is_unpublished(item)
    assert unpublished is False
    assert unpublished_date is None


def test_is_unpublished_true_when_date_in_past():
    """An 'unpublished' date in the past marks the item as unpublished."""
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    item = _item_with_properties({"unpublished": past})
    unpublished, unpublished_date = is_unpublished(item)
    assert unpublished is True
    assert unpublished_date is not None


def test_is_unpublished_false_when_date_in_future():
    """An 'unpublished' date in the future does not yet mark the item as unpublished."""
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat().replace("+00:00", "Z")
    item = _item_with_properties({"unpublished": future})
    unpublished, unpublished_date = is_unpublished(item)
    assert unpublished is False
    assert unpublished_date is not None


# --------------------------------------------------------------------------- #
# is_published
# --------------------------------------------------------------------------- #
def test_is_published_false_when_no_published_property():
    """An item without a 'published' property is not published."""
    item = _item_with_properties({})
    published, published_date = is_published(item)
    assert published is False
    assert published_date is None


def test_is_published_true_when_date_in_past():
    """A 'published' date in the past marks the item as published."""
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    item = _item_with_properties({"published": past})
    published, published_date = is_published(item)
    assert published is True
    assert published_date is not None


def test_is_published_false_when_date_in_future():
    """A 'published' date in the future does not yet mark the item as published."""
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat().replace("+00:00", "Z")
    item = _item_with_properties({"published": future})
    published, published_date = is_published(item)
    assert published is False
    assert published_date is not None
