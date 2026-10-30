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

import re
from typing import Any

from prefect import flow
from pystac import Item

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
)
from rs_workflows.on_demand.common.l0_last_steps import (
    L0_PROCESSINGS,
    get_session_collections,
)
from rs_workflows.on_demand.common.staging import stage_session_common
from rs_workflows.on_demand.sentinel1.s1_l0 import process_s1l0
from rs_workflows.on_demand.sentinel3.s3_l0 import process_s3l0
from rs_workflows.utils.cadip import get_cadip_station
from rs_workflows.utils.catalog import (
    get_single_catalog_item,
    is_published,
    is_unpublished,
)
from rs_workflows.utils.dpr import load_processor_settings
from rs_workflows.utils.prefect import get_logger

# Level-0 flow of each mission
L0_FLOWS = {"1": process_s1l0, "3": process_s3l0}


@flow(name="process-l0")
async def process_l0(
    session: str,
    env_args: FlowEnvArgs,
) -> list[dict[str, Any]] | None:
    """
    This is the generic l0 processing flow, started by the systematic processing after the staging of a session.
    It performs common L0 task like retrieving session from catalog an staging it from cadip if needed.
    Then it runs the Level-0 flow of the mission (`process-s1-l0` or `process-s3-l0`) as a sub-flow, with the
    settings of the `s<mission>l0-default-setting` Prefect variable.

    Args:
        session: Identifier of the session.
        env_args: Flow environment arguments. Its `process` field sets `force_systematic` (run the processing as a
            systematic one, so that the automations start the next processing of the chain from the events of the
            generated products) and `verbose` (not used yet).
    """
    logger = get_logger()
    logger.info(f"Mode verbose is set to {env_args.process.verbose}")

    # Check session name format
    pattern = re.compile(r"^S[123]._")
    if not pattern.match(session):
        logger.error("❌ Bad Sentinel-1,2,3 session name.")
        raise ValueError(f"Invalid session name: '{session}'")

    # We detect the mission
    mission: str = session[1]
    if mission not in L0_FLOWS:
        raise ValueError(f"❌ No Level-0 processing for Sentinel-{mission}")
    logger.info(f"✅ Sentinel-{mission} session name is correct.")

    # Read the settings and check the Dask clusters before searching the session
    settings = await load_processor_settings(L0_PROCESSINGS[mission][0])
    session_collections = get_session_collections(settings)

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, "level0-processing"):
        found = False

        # Try to retrieve the session on the collection
        item_session: Item | None = await get_single_catalog_item(flow_env, session, session_collections)

        # If the session is not on the rs-catalog, we will try to stage it
        if item_session:
            found = True
            unpublished, unpublished_date = is_unpublished(item_session)
            if unpublished:
                logger.error(f"❌ The session '{session}' has been unpublished on {unpublished_date}")
                raise ValueError(f"'{session}' has been unpublished on {unpublished_date}")
            published, _ = is_published(item_session)
            if not published:
                logger.error(f"❌ The session '{session}' has not been published yet")
                raise ValueError(f"'{session}' has not been published")
        else:
            logger.info(f"📥 Try to stage session {session} from {mission} stations :{settings.cadip_collections}")
            station = await get_cadip_station(
                flow_env,
                session,
                settings.cadip_collections,
            )
            if station is not None:
                found = await stage_session_common(flow_env, station, session)

        # The session is staged at this step.
        # We can run the flow of the mission, that reads its settings again as it can also be run alone
        logger.info(f"🚧 We start Sentinel-{mission} processing.")
        if not found:
            return None
        return await L0_FLOWS[mission](
            session=session,
            env_args=env_args,
        )
