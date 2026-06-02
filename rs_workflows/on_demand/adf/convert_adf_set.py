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

import json
from datetime import datetime

from dateutil.rrule import HOURLY, rrule
from prefect import flow, get_run_logger, task
from prefect.variables import get_variable


@flow(name="convert-adf-group")
async def convert_adf_data(period_start_datetime: datetime, period_end_datetime: datetime, adf_group_name: str) -> None:
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

    # read the Prefect Variable and extract data
    prefect_var = get_variable("adf_group_name")
    data = json.loads(prefect_var)
    satellite = data.get("satellite")
    aux_to_be_generated = data.get("aux-to-be-generated", [])

    logger.info(f"Satellite: {satellite}")
    for item in aux_to_be_generated:
        logger.info(
            f"Scheduling conversion for aux type: {item.product_type} with conversion rule: {item.cql2_query_name} every {item.period_in_hours} hours",
        )


@task(name="convert-adf-single-type")
async def convert_adf_single_type(
    product_type: str,
    start: datetime,
    stop: datetime,
) -> None:
    """
    Convert a single ADF data.
     - product_type: type of the product to convert.
     - start: start datetime of the period to convert.
     - stop: end datetime of the period to convert.

    """
    logger = get_run_logger()
    logger.info(
        f"Converting ADF data for product type {product_type} from {start.strftime('%Y-%m-%d %H:%M')} to {stop.strftime('%Y-%m-%d %H:%M')}",
    )
