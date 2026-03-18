# Copyright 2023-2026 Airbus
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
from prefect import flow, get_run_logger
from pystac import Item

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
)
from rs_workflows.on_demand.common.staging import stage_session_common
from rs_workflows.on_demand.common.types import DEFAULT_PREFECT_CONFIGURATION
from rs_workflows.on_demand.common.types import Level0FlowParams
from rs_workflows.on_demand.sentinel1.s1_l0 import process_s1l0
from rs_workflows.on_demand.sentinel3.s3_l0 import process_s3l0
from rs_workflows.utils.cadip import get_cadip_station
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dask import is_dask_cluster_running


@flow(name="process level-0")
async def process_l0(
    session: str,
    flow_params: Level0FlowParams | None = None,
    verbose: bool = False,
) -> None:
    """
    This is the generic l0 processing flow.
    It will call process_s1l0, process_s2l0 or process_s3l0

    Only session parameter is mandatory.
    All other parameters can get their default values from Prefect variable.

    """
    logger = get_run_logger()
    logger.info(f"Mode verbose is set to {verbose}")

    # Check session name format
    pattern = re.compile(r"^S[123]._")
    if not pattern.match(session):
        logger.error("❌ Bad Sentinel-1,2,3 session name.")
        raise ValueError(f"Invalid session name: '{session}'")

    # We detect the mission
    mission: str = session[2]
    logger.info(f"✔️ Sentinel-{mission} session name is correct.")

    # Override of some parameters with default configuration
    flow_params = flow_params or Level0FlowParams()
    p = await flow_params.resolve(mission, level="0")

    flow_env = FlowEnv(FlowEnvArgs(owner_id=p.owner_identifier))
    with flow_env.start_span(__name__, "level0-processing"):
        # Check that the chosen dask_cluster_label is deployed
        if await is_dask_cluster_running(p.dask_cluster_label) == False:
            raise ValueError(f"❌ '{p.dask_cluster_label}' is unknown or not ready.")

        # Try to retrieve the session on the collection
        item_session: Item = await get_single_catalog_item(flow_env, session, [p.session_collection])

        # If the session is not on the rs-catalog, we will try to stage it
        if item_session is None:
            logger.info(f"Try to stage session  {session} from {mission} stations :{p.cadip_collections}")
            station = await get_cadip_station(
                flow_env,
                session,
                p.cadip_collections,
            )
            if station is not None:
                await stage_session_common(flow_env, station, session)

        # The session is stagged at this step.
        # We can call the flow
        logger.info(f"We start Sentinel-{mission} processing.")
        if item_session:
            match mission:
                case 1:
                    await process_s1l0(session, p, verbose)
                case 3:
                    await process_s3l0(session, p, verbose)