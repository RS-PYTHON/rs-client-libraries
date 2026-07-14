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

"""Unit tests for rs_workflows/operation/clean_old_prefect_workers_flow.py"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses as responses_lib

from rs_workflows.operation.clean_old_prefect_workers_flow import (
    cleanup_offline_workers,
)

# The URL we register with `responses` and that the flow must call.
MOCKED_API_URL = "http://prefect-server:4200/api"

# Module path of the PREFECT_API_URL setting object used by the flow.
FLOW_MODULE = "rs_workflows.operation.clean_old_prefect_workers_flow"


def _make_worker(name: str, status: str = "OFFLINE", heartbeat: str | None = None) -> dict:
    """Build a minimal worker dict as returned by the Prefect API."""
    worker: dict = {"name": name, "status": status}
    if heartbeat is not None:
        worker["last_heartbeat_time"] = heartbeat
    return worker


def _old_heartbeat(days: int = 2) -> str:
    """Return a timestamp older than *days* days ago."""
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _recent_heartbeat(minutes: int = 30) -> str:
    """Return a timestamp that is still within the last day."""
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


@pytest.fixture(autouse=True)
def _patch_prefect_api_url():
    """
    Patch PREFECT_API_URL *inside the flow module* so that PREFECT_API_URL.value()
    always returns MOCKED_API_URL, regardless of what Prefect's test harness sets.
    Setting the env var is not enough because the harness overrides it at runtime.
    """
    mock_setting = MagicMock()
    mock_setting.value.return_value = MOCKED_API_URL
    with patch(f"{FLOW_MODULE}.PREFECT_API_URL", mock_setting):
        yield


# Tests


@responses_lib.activate
def test_cleanup_no_pools():
    """When work_pools_csv is empty (or only whitespace/commas), return early without any HTTP call."""
    cleanup_offline_workers(work_pools_csv="", max_age_days=1)
    # No HTTP calls should have been made
    assert len(responses_lib.calls) == 0


@responses_lib.activate
def test_cleanup_pool_not_found():
    """A 404 response for a pool should log a warning and skip to the next pool."""
    pool = "nonexistent-pool"
    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        status=404,
        json={"detail": "Not found"},
    )

    # Should not raise
    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    assert len(responses_lib.calls) == 1


@responses_lib.activate
def test_cleanup_api_error_raises():
    """A non-404, non-2xx response must raise an HTTPError."""
    pool = "bad-pool"
    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        status=500,
        json={"detail": "Internal server error"},
    )

    with pytest.raises(requests.HTTPError):
        cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)


@responses_lib.activate
def test_cleanup_skips_online_workers():
    """Workers whose status is not OFFLINE must never be queued for deletion."""
    pool = "my-pool"
    online_worker = _make_worker("worker-online", status="ONLINE", heartbeat=_old_heartbeat(5))
    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=[online_worker],
    )

    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    # Only the filter POST should have been called; no DELETE
    assert len(responses_lib.calls) == 1
    assert "DELETE" not in [c.request.method for c in responses_lib.calls]


@responses_lib.activate
def test_cleanup_skips_recently_offline_workers():
    """OFFLINE workers whose last heartbeat is within max_age_days must not be deleted."""
    pool = "my-pool"
    recent_worker = _make_worker("worker-recent", status="OFFLINE", heartbeat=_recent_heartbeat(60))
    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=[recent_worker],
    )

    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    assert len(responses_lib.calls) == 1
    assert "DELETE" not in [c.request.method for c in responses_lib.calls]


@responses_lib.activate
def test_cleanup_deletes_old_offline_worker():
    """An OFFLINE worker older than max_age_days must be deleted."""
    pool = "my-pool"
    worker_name = "old-worker"
    old_worker = _make_worker(worker_name, status="OFFLINE", heartbeat=_old_heartbeat(3))

    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=[old_worker],
    )
    responses_lib.delete(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/{worker_name}",
        status=204,
    )

    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    methods = [c.request.method for c in responses_lib.calls]
    assert "POST" in methods
    assert "DELETE" in methods
    assert len(responses_lib.calls) == 2


@responses_lib.activate
def test_cleanup_worker_without_heartbeat_is_deleted():
    """
    An OFFLINE worker with no last_heartbeat_time is treated as old enough
    and must be deleted.
    """
    pool = "my-pool"
    worker_name = "no-heartbeat-worker"
    worker = _make_worker(worker_name, status="OFFLINE", heartbeat=None)

    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=[worker],
    )
    responses_lib.delete(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/{worker_name}",
        status=204,
    )

    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    methods = [c.request.method for c in responses_lib.calls]
    assert "DELETE" in methods


@responses_lib.activate
def test_cleanup_delete_failure_raises():
    """If the DELETE call returns a non-2xx response, an HTTPError must be raised."""
    pool = "my-pool"
    worker_name = "broken-worker"
    old_worker = _make_worker(worker_name, status="OFFLINE", heartbeat=_old_heartbeat(5))

    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=[old_worker],
    )
    responses_lib.delete(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/{worker_name}",
        status=500,
        json={"detail": "delete failed"},
    )

    with pytest.raises(requests.HTTPError):
        cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)


@responses_lib.activate
def test_cleanup_multiple_pools():
    """
    When multiple pools are given, each pool is processed independently.
    Pools that do not exist (404) are skipped; valid pools have workers deleted.
    """
    missing_pool = "missing-pool"
    active_pool = "active-pool"
    worker_name = "stale-worker"
    work_pools_csv = f"{missing_pool},{active_pool}"

    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{missing_pool}/workers/filter",
        status=404,
        json={"detail": "Not found"},
    )
    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{active_pool}/workers/filter",
        json=[_make_worker(worker_name, status="OFFLINE", heartbeat=_old_heartbeat(2))],
    )
    responses_lib.delete(
        url=f"{MOCKED_API_URL}/work_pools/{active_pool}/workers/{worker_name}",
        status=204,
    )

    cleanup_offline_workers(work_pools_csv=work_pools_csv, max_age_days=1)

    methods = [c.request.method for c in responses_lib.calls]
    assert methods.count("POST") == 2
    assert methods.count("DELETE") == 1


@responses_lib.activate
def test_cleanup_pagination():
    """
    When the first page is full (len == page_size == 200), a second page is fetched.
    Only old OFFLINE workers across both pages are deleted.
    """
    pool = "paged-pool"
    page_size = 200

    # First page: 200 workers, all recent (should not be deleted)
    page1 = [_make_worker(f"recent-{i}", status="OFFLINE", heartbeat=_recent_heartbeat(10)) for i in range(page_size)]
    # Second page: 1 old offline worker (should be deleted)
    old_name = "old-worker-page2"
    page2 = [_make_worker(old_name, status="OFFLINE", heartbeat=_old_heartbeat(3))]

    # Register two POST responses (pages)
    responses_lib.add(
        responses_lib.POST,
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=page1,
    )
    responses_lib.add(
        responses_lib.POST,
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=page2,
    )
    responses_lib.delete(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/{old_name}",
        status=204,
    )

    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    post_calls = [c for c in responses_lib.calls if c.request.method == "POST"]
    delete_calls = [c for c in responses_lib.calls if c.request.method == "DELETE"]

    assert len(post_calls) == 2, "Expected two paginated filter requests"
    assert len(delete_calls) == 1, "Expected exactly one deletion"
    delete_url = delete_calls[0].request.url
    assert delete_url is not None
    assert old_name in delete_url


@responses_lib.activate
def test_cleanup_worker_name_url_encoded():
    """Worker names with special characters must be URL-encoded in the DELETE request."""
    pool = "my-pool"
    # Slash in name requires percent-encoding: %2F
    worker_name = "namespace/worker"
    encoded_name = "namespace%2Fworker"
    old_worker = _make_worker(worker_name, status="OFFLINE", heartbeat=_old_heartbeat(3))

    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=[old_worker],
    )
    responses_lib.delete(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/{encoded_name}",
        status=204,
    )

    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    delete_calls = [c for c in responses_lib.calls if c.request.method == "DELETE"]
    assert len(delete_calls) == 1
    delete_url = delete_calls[0].request.url
    assert delete_url is not None
    assert encoded_name in delete_url


@responses_lib.activate
def test_cleanup_mixed_workers_in_pool():
    """
    A pool with a mix of ONLINE, recent OFFLINE, and old OFFLINE workers:
    only the old OFFLINE ones should be deleted.
    """
    pool = "mixed-pool"
    old_name = "to-delete"
    workers = [
        _make_worker("online-worker", status="ONLINE", heartbeat=_old_heartbeat(5)),
        _make_worker("recent-offline", status="OFFLINE", heartbeat=_recent_heartbeat(30)),
        _make_worker(old_name, status="OFFLINE", heartbeat=_old_heartbeat(4)),
    ]

    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=workers,
    )
    responses_lib.delete(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/{old_name}",
        status=204,
    )

    cleanup_offline_workers(work_pools_csv=pool, max_age_days=1)

    delete_calls = [c for c in responses_lib.calls if c.request.method == "DELETE"]
    assert len(delete_calls) == 1
    delete_url = delete_calls[0].request.url
    assert delete_url is not None
    assert old_name in delete_url


@responses_lib.activate
def test_cleanup_csv_with_whitespace_and_extra_commas():
    """Leading/trailing whitespace and extra commas in work_pools_csv must be handled gracefully."""
    pool = "trimmed-pool"
    responses_lib.post(
        url=f"{MOCKED_API_URL}/work_pools/{pool}/workers/filter",
        json=[],
    )

    # Comma-padded CSV with spaces around pool name
    cleanup_offline_workers(work_pools_csv=f"  ,  {pool}  ,  ", max_age_days=1)

    assert len(responses_lib.calls) == 1
