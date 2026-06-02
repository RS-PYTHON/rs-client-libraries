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

"""Convert a set of ADF data."""

from datetime import datetime

from dateutil.rrule import HOURLY, rrule
from prefect import flow, get_run_logger
from prefect.input import select
from prefect.variables import get_variable, list_variables


def filter_variables(pattern: str = "s1"):
    """Filter Prefect Variable."""
    all_vars = list_variables()
    return [var.name for var in all_vars if var.name.startswith(pattern)]


filtered_vars = filter_variables("AA")


@flow(name="convert-adf-group")
async def convert_adf_data(
    period_start_datetime: datetime,
    period_end_datetime: datetime,
    adf_group_name: str = select(filter_variables()),
) -> None:
    """
    Convert a set of ADF data.
     - adf_group_name: name of the ADF group to convert.
     - period_start_datetime: start datetime of the period to convert.
     - period_end_datetime: end datetime of the period to convert.

     The part of the period in the past will be treated immediately.
     The part of the period in the future will be scheduled.

    """

    logger = get_run_logger()
    schedule_rule = rrule(freq=HOURLY, interval=2, dtstart=period_start_datetime, until=period_end_datetime)
    for dt in schedule_rule:
        logger.info(dt.strftime("%Y-%m-%d %H:%M"))
