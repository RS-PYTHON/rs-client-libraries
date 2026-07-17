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

"""Utilities for working with Prefect variables."""

from collections.abc import Awaitable
from typing import Any, cast

from prefect import get_run_logger
from prefect.variables import Variable


def _deep_merge(value: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge dictionary updates without mutating the inputs."""
    merged = value.copy()
    for key, update in updates.items():
        current = merged.get(key)
        merged[key] = _deep_merge(current, update) if isinstance(current, dict) and isinstance(update, dict) else update
    return merged


def _contains_updates(value: dict[str, Any], updates: dict[str, Any]) -> bool:
    """Return whether all nested updates are present in a dictionary."""
    for key, expected in updates.items():
        actual = value.get(key)
        if isinstance(expected, dict):
            if not isinstance(actual, dict) or not _contains_updates(actual, expected):
                return False
        elif actual != expected:
            return False
    return True


async def update_prefect_variable(variable_name: str, updates: dict[str, Any]) -> dict[str, Any]:
    """Merge updates into a Prefect variable and verify that they were persisted."""
    logger = get_run_logger()
    logger.info("Reading current Prefect variable %s", variable_name)

    raw_value = await cast(Awaitable[Any], Variable.get(variable_name, default={}))
    logger.info(
        "Read Prefect variable %s: type=%s, keys=%s",
        variable_name,
        type(raw_value).__name__,
        sorted(raw_value) if isinstance(raw_value, dict) else [],
    )

    value = _deep_merge(raw_value if isinstance(raw_value, dict) else {}, updates)
    logger.info("Updating Prefect variable %s with keys=%s", variable_name, sorted(updates))
    await cast(Awaitable[Any], Variable.set(variable_name, value, overwrite=True))

    saved_value = await cast(Awaitable[Any], Variable.get(variable_name, default={}))
    if not isinstance(saved_value, dict):
        raise RuntimeError(
            f"Prefect variable {variable_name!r} was not updated: expected a dictionary, "
            f"got {type(saved_value).__name__}"
        )

    if not _contains_updates(saved_value, updates):
        raise RuntimeError(f"Prefect variable {variable_name!r} was not updated: expected nested updates {updates!r}")

    logger.info("Verified Prefect variable %s update for keys=%s", variable_name, sorted(updates))
    return saved_value
