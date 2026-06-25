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
import os
from collections.abc import Awaitable
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from prefect import flow, get_run_logger, runtime, task
from prefect.client.orchestration import get_client
from prefect.runner.storage import GitRepository
from prefect.runtime import flow_run
from prefect.variables import Variable

from rs_common.utils import strftime_millis
from rs_workflows.adf_flow import adf_conversion_task, substitute_values
from rs_workflows.flow_utils import AdfProcessIn, AuxiliaryProductMapping, FlowEnvArgs

script_dir = os.path.dirname(os.path.abspath(__file__))
CQL2_FILTERS_PATH = os.path.join(script_dir, "config", "cql2-queries.json")
FLOW_TO_BE_SCHEDULED: str = "rs_workflows/on_demand/adf/convert_adf_set.py:adf_conversion_scheduled"


@flow(name="convert-adf-group")
async def convert_adf_group(
    period_start_datetime: datetime,
    period_end_datetime: datetime,
    adf_group_name: str = "convert-aux-s3-olci-l1",
    owner_identifier: str = "copernicus",
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
        ValueError: If `period_start_datetime` is not before `period_end_datetime`
        or if the Prefect Variable format is invalid.
        FileNotFoundError: If the Prefect Variable does not exist.
    """

    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    # Check input chronology
    if period_start_datetime >= period_end_datetime:
        raise ValueError(
            "❌ period_start_datetime should be before period_end_datetime",
            f" ( here {period_start_datetime} >= {period_end_datetime})",
        )

    # Read the Prefect Variable and extract list of aux to manage
    raw_data = await cast(Awaitable[Any], Variable.get(adf_group_name))
    if raw_data is None:
        raise FileExistsError(f"❌ Prefect variable '{adf_group_name}' does not exist.")
    if not isinstance(raw_data, dict):
        raise ValueError(f"❌ Prefect variable '{adf_group_name}' has got an invalid format.")
    settings: dict[str, Any] = raw_data
    aux_to_be_generated: list = settings.get("aux-to-be-generated", [])
    logger.debug(f"aux_to_be_generated = {aux_to_be_generated}")
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping] = settings.get(
        "auxiliary-product-to-collection-identifier",
        [],
    )
    logger.debug(f"auxiliary_product_to_collection_identifier = {auxiliary_product_to_collection_identifier}")

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
                auxiliary_product_to_collection_identifier,
            )
    else:
        logger.info("No AUX data to be retrieved for the future period. No flow will be scheduled.")

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
                auxiliary_product_to_collection_identifier,
            )
            for item in aux_to_be_generated
        ]
        await asyncio.gather(*tasks)
    else:
        logger.info("No AUX data to retrieve in the past.")


def compute_cql2(cql2_query_name: str, dta: int, dtb: int) -> dict:
    """Compute the CQL2 filter content by reading the configuration file and substituting the values."""
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
    except OSError as e:
        logger.error(f"❌ OS error while reading file '{CQL2_FILTERS_PATH}': {e}")

    logger.debug(f"Read CQL2 filter content from file '{CQL2_FILTERS_PATH}' : {cql2_json}")

    # Find the filter
    cql2_temp = next(entry for entry in cql2_json if entry["name"] == cql2_query_name)
    if cql2_temp is None:
        raise RuntimeError(f"❌ cql2 query '{cql2_query_name}' not found on configuration.")
    cql2_json = {
        "filter": cql2_temp["stac"]["filter"],
        "sortby": cql2_temp["stac"]["sortby"],
        "limit": cql2_temp["stac"]["limit"],
    }

    return substitute_values(cql2_json, {"dTa": dta, "dTb": dtb})


@task(name="conversion from the past")
async def past_adf_conversion(
    owner_identifier: str,
    product_type: str,
    cql2_query_name: str,
    dta: int,
    dtb: int,
    period_in_hours: int,
    period_start: datetime,
    period_end: datetime,
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping],
) -> None:
    """
    Convert ADF data for a period in the past by splitting it into
    sub-periods of length `period_in_hours` and running the conversion flow on each of them.
    If `period_in_hours` is equal to 0, then the conversion is run on the whole period at once.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    logger.info("Computing cql2_filter without start_datetime and end_datetime...")
    cql2_filter_without_date = compute_cql2(cql2_query_name, dta, dtb)

    # Scheduling according to the period_in_hours
    flow_parameters: AdfProcessIn
    if period_in_hours == 0:
        # special case where a single run is requested
        logger.info(
            f"Run the conversion task with time range [{period_start}-{period_end}].",
        )
        cql2_filter = substitute_values(
            cql2_filter_without_date,
            {
                "start_datetime": strftime_millis(period_start),
                "end_datetime": strftime_millis(period_end),
            },
        )
        logger.debug(f"Associated cql2 filter is: {cql2_filter}")
        flow_parameters = AdfProcessIn(
            env=FlowEnvArgs(owner_id=owner_identifier),
            adf_type=product_type,
            auxiliary_product_to_collection_identifier=auxiliary_product_to_collection_identifier,
            cql2_filter=cql2_filter,
        )
        await adf_conversion_task.with_options(
            name=f"convert {product_type} on the period [{period_start}-{period_end}]",
        )(flow_parameters)

    else:
        start = period_start
        duration: timedelta = timedelta(hours=int(period_in_hours))
        while start <= period_end:
            stop = min(start + duration, period_end)
            logger.info(
                f"Run the conversion task time range [{start}-{stop}].",
            )
            cql2_filter = substitute_values(
                cql2_filter_without_date,
                {
                    "start_datetime": strftime_millis(start),
                    "end_datetime": strftime_millis(stop),
                },
            )
            logger.debug(f"( past ) Associated cql2 filter is: {cql2_filter}")
            flow_parameters = AdfProcessIn(
                env=FlowEnvArgs(owner_id=owner_identifier),
                adf_type=product_type,
                auxiliary_product_to_collection_identifier=auxiliary_product_to_collection_identifier,
                cql2_filter=cql2_filter,
            )
            await adf_conversion_task.with_options(name=f"convert {product_type} on the period [{start}-{stop}]")(
                flow_parameters,
            )
            start += duration


@task(name="schedule conversion")
async def schedule_adf_conversion(
    owner_identifier: str,
    product_type: str,
    cql2_query_name: str,
    dta: int,
    dtb: int,
    period_in_hours: int,
    period_start: datetime,
    period_end: datetime,
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping],
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
    cql2_filter_without_date = compute_cql2(cql2_query_name, dta, dtb)

    # Scheduling according to the period_in_hours
    rule: str
    if period_in_hours == 0:
        # special case where a single run is requested
        logger.info(
            f"Schedule the flow conversion to start at {period_start} for a time range [{period_start}-{period_end}].",
        )
        logger.debug(f"Associated cql2 filter is: {cql2_filter_without_date}")
        rule = (
            f"DTSTART:{period_start.strftime("%Y%m%dT%H%M%SZ")}\n"
            f"FREQ=HOURLY;UNTIL={period_start.strftime("%Y%m%dT%H%M%SZ")}"
        )

        await schedule_conversion_flow(
            owner_identifier,
            rule,
            cql2_filter_without_date,
            product_type,
            period_end - period_start,
            auxiliary_product_to_collection_identifier,
        )

    else:
        period_corrected: timedelta = min(period_end - period_start, timedelta(hours=period_in_hours))
        logger.debug(f"period_corrected = {period_corrected}")
        rule = (
            f"DTSTART:{period_start.strftime("%Y%m%dT%H%M%SZ")}\n"
            f"FREQ=HOURLY;INTERVAL={period_in_hours};UNTIL={period_end.strftime("%Y%m%dT%H%M%SZ")}"
        )
        logger.debug(f"rule = {rule}")

        logger.info(
            f"Schedule the flow conversion to start at {period_start} for a time range [{period_start}-{period_end}].",
        )
        logger.debug(f"Associated cql2 filter is: {cql2_filter_without_date}")
        await schedule_conversion_flow(
            owner_identifier,
            rule,
            cql2_filter_without_date,
            product_type,
            period_corrected,
            auxiliary_product_to_collection_identifier,
        )


async def schedule_conversion_flow(
    owner_identifier: str,
    rule: str,
    cql2_filter_without_date: dict,
    product_type: str,
    period: timedelta,
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping],
) -> None:
    """Schedule the conversion flow with the given parameters and scheduling rule."""

    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    # Retrieve the name of the workpool, GitHub URL and Branch
    work_pool_name: str | None = None
    github_repository: str | None = None
    github_branch: str | None = None
    async with get_client() as client:
        deployment = await client.read_deployment(runtime.deployment.id)
        work_pool_name = deployment.work_pool_name
        pull_steps = deployment.pull_steps or []

        for step in pull_steps:
            for step_name, step_config in step.items():
                if "git_clone" in step_name:
                    github_repository = step_config.get("repository")
                    github_branch = step_config.get("branch", "develop")  # "develop" by default
        logger.info(
            f"Work pool name: {work_pool_name}, GitHub repository: {github_repository}, GitHub branch: {github_branch}",
        )

    flow_obj = await cast(
        Awaitable[Any],
        flow.from_source(
            source=GitRepository(url=github_repository, branch=github_branch),
            entrypoint=FLOW_TO_BE_SCHEDULED,
        ),
    )

    encoded_period: str = str(int(period.total_seconds()))
    await flow_obj.deploy(
        name=f"Convert ADF {product_type}",
        work_pool_name=work_pool_name,
        rrule=rule,
        tags=["auxip", "conversion"],
        parameters={
            "env": FlowEnvArgs(owner_id=owner_identifier),
            "adf_type": product_type,
            "cql2_filter_without_date": cql2_filter_without_date,
            "period": encoded_period,
            "auxiliary_product_to_collection_identifier": auxiliary_product_to_collection_identifier,
        },
    )


@flow(name="convert-adf-scheduled")
async def adf_conversion_scheduled(
    env: FlowEnvArgs,
    adf_type: str,
    cql2_filter_without_date: dict,
    period: str,
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping],
) -> None:
    """
    Flow to convert ADF data for a scheduled period. The period is defined by the scheduling rule of the flow and
    the `period` parameter, which defines the length of the period to convert starting from the flow run start time.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    decoded_period: timedelta = timedelta(seconds=int(period))

    logger.info(f"Starting the conversion for {adf_type}")
    start: datetime = flow_run.scheduled_start_time - decoded_period
    stop: datetime = flow_run.scheduled_start_time
    cql2_filter = substitute_values(
        cql2_filter_without_date,
        {
            "start_datetime": strftime_millis(start),
            "end_datetime": strftime_millis(stop),
        },
    )
    logger.debug(f"The CQL2 used for the conversion is {cql2_filter}")

    flow_parameters: AdfProcessIn = AdfProcessIn(
        env=env,
        adf_type=adf_type,
        auxiliary_product_to_collection_identifier=auxiliary_product_to_collection_identifier,
        cql2_filter=cql2_filter,
    )
    await adf_conversion_task.with_options(name=f"convert {adf_type} on the period [{start}-{stop}]")(flow_parameters)
