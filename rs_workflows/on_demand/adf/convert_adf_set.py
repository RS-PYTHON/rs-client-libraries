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
from prefect import flow, task


@flow(name="convert-adf-group")
async def convert_adf_data(adf_group_name: str, period_start_datetime: datetime, period_end_datetime: datetime) -> None:
    """
    Convert a set of ADF data.
     - adf_group_name: name of the ADF group to convert.
     - period_start_datetime: start datetime of the period to convert.
     - period_end_datetime: end datetime of the period to convert.

     The part of the period in the past will be treated immediately.
     The part of the period in the future will be scheduled.

    """

    schedule_rule = rrule(freq=HOURLY, interval=2, dtstart=period_start_datetime, until=period_end_datetime)
    for dt in schedule_rule:
        print(dt.strftime("%Y-%m-%d %H:%M"))
