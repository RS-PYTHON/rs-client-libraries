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

"""Unit tests for Prefect variable utilities."""

from unittest.mock import AsyncMock, MagicMock

from rs_workflows.utils import prefect as prefect_utils


async def test_update_prefect_variable_merges_and_verifies(mocker):
    """Existing fields are preserved while requested fields are updated."""
    mocker.patch.object(prefect_utils, "get_run_logger", return_value=MagicMock())
    stored_value = {"processor_name": "S3-L0", "finished": "old"}

    async def get_variable(*_args, **_kwargs):
        return stored_value.copy()

    async def set_variable(_name, value, **_kwargs):
        stored_value.clear()
        stored_value.update(value)

    variable_get = mocker.patch.object(
        prefect_utils.Variable,
        "get",
        new=AsyncMock(side_effect=get_variable),
    )
    variable_set = mocker.patch.object(
        prefect_utils.Variable,
        "set",
        new=AsyncMock(side_effect=set_variable),
    )

    result = await prefect_utils.update_prefect_variable(
        "s3-l0-default-setting",
        {"finished": "2026-07-17T12:00:00.000Z"},
    )

    assert variable_get.await_count == 2
    variable_set.assert_awaited_once_with(
        "s3-l0-default-setting",
        {
            "processor_name": "S3-L0",
            "finished": "2026-07-17T12:00:00.000Z",
        },
        overwrite=True,
    )
    assert result == stored_value
