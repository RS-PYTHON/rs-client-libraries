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

"""common Level-0 processing."""

from datetime import datetime
from typing import Any

from pystac import Item

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
    WorkflowType,
)
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dpr import (
    ProcessingId,
    ProcessorSettings,
    call_dpr_flow,
    load_processor_settings,
)
from rs_workflows.utils.prefect import get_logger

# Level-0 processing of each mission, and name of its session input product in the task table
L0_PROCESSINGS: dict[str, tuple[ProcessingId, str]] = {
    "1": (ProcessingId.S1_L0, "S1CADUS"),
    "3": (ProcessingId.S3_L0, "S3ACADUS"),
}


def get_session_collections(settings: ProcessorSettings) -> list[str]:
    """Return the catalog collections of the staged sessions, mandatory in the settings of a Level-0 processing."""
    if not settings.input_collections:
        raise ValueError("❌ The settings of the Level-0 processing must define the 'input_collections'")
    return settings.input_collections


async def process_staged_session(
    mission: str,
    session: str,
    env_args: FlowEnvArgs,
    force_systematic: bool,
    verbose: bool,
) -> list[dict[str, Any]] | None:
    """
    Level-0 processing of a session already staged in the catalog: read the settings of the processing of the
    mission, then run the final processing steps.

    Returns:
        The STAC items of the generated products, or None if the session is not in the catalog.
    """
    settings = await load_processor_settings(L0_PROCESSINGS[mission][0])
    return await process_l0_last_steps(
        mission=mission,
        session=session,
        env_args=env_args,
        settings=settings,
        workflow=WorkflowType.of_run(force_systematic),
        verbose=verbose,
    )


async def process_l0_last_steps(
    mission: str,
    session: str,
    env_args: FlowEnvArgs,
    settings: ProcessorSettings,
    workflow: WorkflowType,
    verbose: bool,
) -> list[dict[str, Any]] | None:
    """
    Final processing steps that are common to all missions.

    Returns:
        The STAC items of the generated products, or None if the session is not in the catalog.

    Raises:
        ValueError: If the session has no valid 'datetime' property.
    """
    logger = get_logger()
    logger.info(f"Starting L0 last steps: mission={mission!r}, session={session!r}, verbose={verbose!r}")
    session_collections = get_session_collections(settings)

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, f"sentinel{mission}-level0-processing"):
        logger.info(f"Looking up catalog session: session={session!r}, collections={session_collections!r}")
        item_session: Item | None = await get_single_catalog_item(flow_env, session, session_collections)
        if not item_session:
            logger.error("❌ Session %r was not found; DPR processing cannot be launched.", session)
            return None
        logger.info(f"✅ The session {session} has been found in the catalog.")
        logger.info("Catalog item id=%r, properties=%r", item_session.id, item_session.properties)

        # Session datetime date
        published = item_session.properties.get("datetime")
        if not isinstance(published, str):
            raise ValueError("Missing or invalid 'datetime' property in item_session")
        session_datetime = datetime.fromisoformat(published)

        input_products = [
            FlowInputProduct(
                name=L0_PROCESSINGS[mission][1],
                item_id=session,
                collection_name=str(item_session.collection_id),
            ),
        ]
        l0_result = await call_dpr_flow(
            env_args,
            settings=settings,
            input_products=input_products,
            external_variables={
                "start_datetime": session_datetime,
                "end_datetime": session_datetime,
                "satellite": f"sentinel-{mission}{session[2].lower()}",
            },
            workflow=workflow,
        )
        logger.info(f"L0 processing completed for mission={mission!r}, session={session!r}: {len(l0_result)} products")
        return l0_result
