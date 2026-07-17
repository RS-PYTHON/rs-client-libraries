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

"""sentinel 3 Level-0 processing."""

from collections.abc import Awaitable
from datetime import datetime, timezone
from typing import Any, cast

from prefect import flow, get_run_logger, task
from prefect.variables import Variable

from rs_workflows.flow_utils import (
    FlowInputProduct,
)
from rs_workflows.on_demand.common.l0_last_steps import process_l0_last_steps
from rs_workflows.on_demand.common.types import Level0FlowParams

S3_L0_DEFAULT_SETTING = "s3-l0-default-setting"


@flow(name="process-s3-l0")
async def process_s3l0(
    session: str,
    flow_params: Level0FlowParams | None = None,
    verbose: bool = False,
):
    """
    Sentinel-3 L0 processing.
    The session should have been staged before.
    """

    # Resolve values from the s3-l0-default-setting Prefect variable before
    # using them to build the DPR input. Explicit flow parameters still win.
    resolved_flow_params = await (flow_params or Level0FlowParams()).resolve("3")

    input_products = [
        FlowInputProduct(
            name="S3ACADUS",
            item_id=session,
            collection_name=resolved_flow_params.session_collection,
        ),
    ]

    await process_l0_last_steps(
        mission="3",
        session=session,
        flow_params=resolved_flow_params,
        input_products=input_products,
        verbose=verbose,
    )


@task(name="process-s3-l0")
async def process_s3l0_task(*args, **kwargs) -> None:
    """See: dpr_processing"""
    logger = get_run_logger()
    logger.info("Starting S3 L0 task; Prefect variable to update: %s", S3_L0_DEFAULT_SETTING)

    await process_s3l0.fn(*args, **kwargs)
    logger.info("S3 L0 processing completed; reading current Prefect variable")

    raw_settings = await cast(Awaitable[Any], Variable.get(S3_L0_DEFAULT_SETTING, default={}))
    logger.info(
        "Read Prefect variable %s: type=%s, keys=%s",
        S3_L0_DEFAULT_SETTING,
        type(raw_settings).__name__,
        sorted(raw_settings) if isinstance(raw_settings, dict) else [],
    )
    settings = raw_settings.copy() if isinstance(raw_settings, dict) else {}
    finished = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    settings["finished"] = finished
    logger.info("Writing finished=%s to Prefect variable %s", finished, S3_L0_DEFAULT_SETTING)
    await cast(
        Awaitable[Any],
        Variable.set(S3_L0_DEFAULT_SETTING, settings, overwrite=True),
    )

    saved_settings = await cast(Awaitable[Any], Variable.get(S3_L0_DEFAULT_SETTING, default={}))
    saved_finished = saved_settings.get("finished") if isinstance(saved_settings, dict) else None
    if saved_finished != finished:
        raise RuntimeError(
            f"Prefect variable {S3_L0_DEFAULT_SETTING!r} was not updated: "
            f"expected finished={finished!r}, got {saved_finished!r}"
        )
    logger.info("Verified Prefect variable %s: finished=%s", S3_L0_DEFAULT_SETTING, saved_finished)
