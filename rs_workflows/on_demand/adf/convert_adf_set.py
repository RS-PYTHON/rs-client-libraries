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

import asyncio
import json
import logging
from collections.abc import Awaitable
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from prefect import flow, get_run_logger, task
from prefect.runner.storage import GitRepository
from prefect.runtime import flow_run
from prefect.variables import Variable

from rs_workflows.flow_utils import AuxiliaryProductMapping, FlowEnvArgs

CQL2_FILTERS_PATH: str = "./config/cql2_filters.json"
PREFECT_WORKPOOL: str = "on-demand-k8s-pool-prefect"
GITHUB_URL: str = "https://github.com/RS-PYTHON/rs-client-libraries.git"
GITHUB_BRANCH: str = "rspy-1074/create-flow-convert-adf-group"


@flow(name="convert-adf-group")
async def convert_adf_data(
    owner_identifier: str,
    period_start_datetime: datetime,
    period_end_datetime: datetime,
    adf_group_name: str,
) -> None:
    """
    Convert a set of ADF (Auxiliary Data Files) data for a specified group and time period.

    Args:
        period_start_datetime: Start datetime of the period to convert (UTC).
        period_end_datetime: End datetime of the period to convert (UTC).
        adf_group_name: Name of the ADF group (Prefect Variable) containing the configuration.

    Behavior:
        - The part of the period in the past is processed immediately.
        - The part of the period in the future is scheduled for later execution.

    Raises:
        ValueError: If `period_start_datetime` is not before `period_end_datetime` or if the Prefect Variable format is invalid.
        FileNotFoundError: If the Prefect Variable does not exist.
    """

    logger = get_run_logger()
    # logger.level = logging.DEBUG
    # logger.propagate = True
    logger.setLevel(logging.DEBUG)

    # Check input chronology
    if period_start_datetime >= period_end_datetime:
        raise ValueError(
            f"❌ period_start_datetime should be before period_end_datetime ( here {period_start_datetime} >= {period_end_datetime})",
        )

    # Read the Prefect Variable and extract list of aux to manage
    raw_data = await cast(Awaitable[Any], Variable.get(adf_group_name))
    if raw_data is None:
        raise FileExistsError(f"❌ Prefect variable '{adf_group_name}' does not exist.")
    if not isinstance(raw_data, dict):
        raise ValueError(f"❌ Prefect variable '{adf_group_name}' has got an invalid format.")
    settings: dict[str, Any] = raw_data
    satellite = settings.get("satellite", "")
    aux_to_be_generated = settings.get("aux-to-be-generated", [])

    # Split the problem in two : past and future period.
    now_utc = datetime.now(timezone.utc)

    # 1) Let's start with future period
    if period_end_datetime > now_utc:
        schedule_start = now_utc if period_start_datetime < now_utc else period_start_datetime
        logger.info(f"Scheduling ADF conversion for the period [{schedule_start} - {period_end_datetime}]")
        for item in aux_to_be_generated:
            await schedule_adf_conversion.with_options(name=f"schedule {item['product_type']}  ")(
                owner_identifier,
                item["product_type"],
                item["cql2_query_name"],
                item.get("dTa", 0),
                item.get("dTb", 0),
                item["period_in_hours"],
                schedule_start,
                period_end_datetime,
            )
        else:
            logger.info("No future period to schedule. All AUX data retrieval is for past dates.")

    # 2) Continue with transformation on the past
    if period_start_datetime < now_utc:
        retrieve_past_start = period_start_datetime
        retrieve_past_end = now_utc if period_end_datetime > now_utc else period_end_datetime

        logger.info(f"Convert ADF for the period [{retrieve_past_start} - {retrieve_past_end}]")

        tasks = [
            past_adf_conversion.with_options(name=f"convert {item['product_type']}  ")(
                owner_identifier,
                item["product_type"],
                item["cql2_query_name"],
                item.get("dTa", 0),
                item.get("dTb", 0),
                item["period_in_hours"],
                retrieve_past_start,
                retrieve_past_end,
            )
            for item in aux_to_be_generated
        ]
        await asyncio.gather(*tasks)
    else:
        logger.info("No AUX data to retrieve in the past. Flows have been scheduled to retrieved them later on.")


class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def substitute_values(obj, values):
    if isinstance(obj, dict):
        return {k: substitute_values(v, values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [substitute_values(v, values) for v in obj]
    if isinstance(obj, str):
        return obj.format_map(SafeDict(values))
    return obj


def compute_filter(cql2_query_name: str, dTa: int, dTb: int) -> dict:

    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    try:
        # Read the file and load its content into a variable
        with open(CQL2_FILTERS_PATH, encoding="utf-8") as file:
            cql2_json = json.load(file)  # json_data is a Python dictionary

    except FileNotFoundError:
        logger.error(f"❌ Error: The file '{CQL2_FILTERS_PATH}' does not exist.")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error: The file '{CQL2_FILTERS_PATH}' is not valid JSON. Details: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error while reading file '{CQL2_FILTERS_PATH}' : {e}")

    logger.debug(f"Read CQL2 filter content from file '{CQL2_FILTERS_PATH}' : {cql2_json}")

    # Find the filter
    filter_temp = next(entry for entry in cql2_json if entry["name"] == cql2_query_name)
    filter_json = {"filter": filter_temp["stac"]["filter"]}

    return substitute_values(filter_json, {"dTa": dTa, "dTb": dTb})


@task(name="conversion from the past")
async def past_adf_conversion(
    owner_identifier: str,
    product_type: str,
    cql2_query_name: str,
    dTa: int,
    dTb: int,
    period_in_hours: int,
    period_start: datetime,
    period_end: datetime,
) -> None:
    """ """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    logger.info("Computing cql2_filter without start_datetime and end_datetime...")
    cql2_filter_without_date = compute_filter(cql2_query_name, dTa, dTb)

    # Scheduling according to the period_in_hours
    if period_in_hours == 0:
        # special case where a single run is requested
        logger.info(
            f"Run the conversion task with time range [{period_start}-{period_end}].",
        )
        cql2_filter = substitute_values(
            cql2_filter_without_date,
            {"start_datetime": period_start, "end_datetime": period_end},
        )
        logger.debug(f"Associated cql2 filter is: {cql2_filter}")
        await fake_adf_conversion.with_options(name=f"[{period_start}-{period_end}]")()

    else:
        start = period_start
        duration: timedelta = timedelta(hours=int(period_in_hours))
        while start <= period_end:
            stop = min(start + duration, period_end)
            logger.info(
                f"Run the conversion task time range [{start}-{stop}].",
            )
            cql2_filter = substitute_values(cql2_filter_without_date, {"start_datetime": start, "end_datetime": stop})
            logger.debug(f"Associated cql2 filter is: {cql2_filter}")
            await fake_adf_conversion.with_options(name=f"[{start}-{stop}]")()
            start += duration


@task(name="fake conversion in the past")
async def fake_adf_conversion():
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)
    logger.info(" single conversion task ...")
    await asyncio.sleep(5)


@task(name="schedule conversion")
async def schedule_adf_conversion(
    owner_identifier: str,
    product_type: str,
    cql2_query_name: str,
    dTa: int,
    dTb: int,
    period_in_hours: int,
    period_start: datetime,
    period_end: datetime,
) -> None:
    """
    Convert a single ADF data.
     - product_type: type of the product to convert.
     - start: start datetime of the period to convert.
     - stop: end datetime of the period to convert.

    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    logger.info("Computing cql2_filter without start_datetime and end_datetime...")
    cql2_filter_without_date = compute_filter(cql2_query_name, dTa, dTb)

    # Scheduling according to the period_in_hours
    if period_in_hours == 0:
        # special case where a single run is requested
        logger.info(
            f"Schedule the flow conversion to start at {period_start} for a time range [{period_start}-{period_end}].",
        )
        logger.debug(f"Associated cql2 filter is: {cql2_filter_without_date}")
        rule: str = (
            f"DTSTART:{period_start.strftime("%Y%m%dT%H%M%SZ")}\nFREQ=HOURLY;UNTIL={period_start.strftime("%Y%m%dT%H%M%SZ")}"
        )

        await schedule_conversion_flow(
            owner_identifier,
            rule,
            cql2_filter_without_date,
            product_type,
            period_end - period_start,
        )

    else:
        period_corrected: timedelta = min(period_end - period_start, timedelta(minutes=period_in_hours))
        logger.debug(f"period_corrected = {period_corrected}")
        start_rule: datetime = period_start + period_corrected
        stop_rule: datetime = period_end
        rule: str = (
            f"DTSTART:{start_rule.strftime("%Y%m%dT%H%M%SZ")}\nFREQ=MINUTELY;INTERVAL={period_in_hours};UNTIL={stop_rule.strftime("%Y%m%dT%H%M%SZ")}"
        )
        logger.debug(f"rule = {rule}")

        logger.info(
            f"Schedule the flow conversion to start at {start_rule} for a time range [{period_start}-{period_end}].",
        )
        logger.debug(f"Associated cql2 filter is: {cql2_filter_without_date}")
        await schedule_conversion_flow(owner_identifier, rule, cql2_filter_without_date, product_type, period_corrected)


async def schedule_conversion_flow(
    owner_identifier: str,
    rule: str,
    cql2_filter_without_date: dict,
    product_type: str,
    period: timedelta,
) -> None:

    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    flow_obj = await flow.from_source(
        source=GitRepository(url=GITHUB_URL, branch=GITHUB_BRANCH),
        entrypoint="rs_workflows/on_demand/adf/convert_adf_set.py:adf_conversion_scheduled",
    )

    encoded_period: str = str(int(period.total_seconds()))
    await flow_obj.deploy(
        name=f"Convert ADF {product_type}",
        work_pool_name=PREFECT_WORKPOOL,
        rrule=rule,
        tags=["auxip", "conversion"],
        parameters={
            "env": FlowEnvArgs(owner_id=owner_identifier),
            "adf_type": product_type,
            "auxiliary_product_to_collection_identifier": [],
            "cql2_filter_without_date": cql2_filter_without_date,
            "period": encoded_period,
        },
    )


@flow(name="convert-adf-scheduled")
async def adf_conversion_scheduled(
    env: FlowEnvArgs,
    adf_type: str,
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping],
    cql2_filter_without_date: dict,
    period: str,
) -> None:
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    decoded_period: timedelta = timedelta(seconds=int(period))

    logger.info(f"Starting the conversion for {adf_type}")
    start: datetime = flow_run.scheduled_start_time - decoded_period
    stop: datetime = flow_run.scheduled_start_time
    cql2_filter = substitute_values(cql2_filter_without_date, {"start_datetime": start, "end_datetime": stop})
    logger.debug(f"The CQL2 used for the conversion is {cql2_filter}")

    ## CALL the conversion task with env, auxilliary_product_to_collection_identifier and cql2_filter
