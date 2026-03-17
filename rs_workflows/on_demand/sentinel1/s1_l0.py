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

"""sentinel 1 Level-0 processing."""

import re
from datetime import datetime, timedelta
from enum import Enum

from prefect import flow, get_run_logger, task
from pystac import Item

from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnv,
    FlowEnvArgs,
    GeneratedProduct,
    InputProduct,
)
from rs_workflows.on_demand.stage_last_sessions import stage_session_common
from rs_workflows.utils.cadip import get_cadip_station
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dask import is_dask_cluster_running
from rs_workflows.utils.dpr import call_dpr_flow, read_prefect_variable


class Collection(str, Enum):
    S1_SESSION = "s01-cadip-session"


DEFAULT_CONFIGURATION: str = "s1-l0-default-setting"


@flow(name="process a sentinel-1 sessions")
async def s1l0_processing(
    session: str,
    owner_identifier: str = "copernicus",
    dask_cluster_label: str = "",
    verbose: bool = False,
):
    logger = get_run_logger()
    logger.info(f"Mode verbose is set to {verbose}")

    # Check S1 session name format
    pattern = re.compile(r"^S1._")
    if not pattern.match(session):
        logger.error("❌ Bad Sentinel-1 session name.")
        raise ValueError(f"Invalid session name: '{session}'")
    logger.info("✔️ Sentinel-1 session name is correct.")

    # Retrieve dask cluster label
    if dask_cluster_label == "":
        settings: dict = await read_prefect_variable(DEFAULT_CONFIGURATION)
        dask_cluster_label = settings["dask_cluster_name"]

    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    with flow_env.start_span(__name__, "sentinel1-level0"):
        # Check that the chosen dask_cluster_label is deployed
        if await is_dask_cluster_running(dask_cluster_label) == False:
            raise ValueError(f"❌ '{dask_cluster_label}' is unknown or not ready.")

        # Try to retrieve the session on the collection
        item_session: Item = await get_single_catalog_item(flow_env, session, [Collection.S1_SESSION.value])

        # If the session is not on the rs-catalog, we will try to stage it
        if item_session is None:
            logger.info("Try to stage it from all S1 stations.")
            station = await get_cadip_station(
                flow_env,
                session,
                ["s1_ins", "s1_kse", "s1_mps", "s1_mti", "s1_nsg", "s1_sgs"],
            )
            if station is not None:
                await stage_session_common(flow_env, station, session)
                item_session = await get_single_catalog_item(flow_env, session, [Collection.S1_SESSION.value])

    # Prepare the input for the Sentinel-1
    # The satellite name can be retrieved from the 3 first caracters of the session name
    satellite_identifier = session[:3].upper()
    end_datetime = datetime.fromisoformat(item_session.properties.get("published"))
    start_datetime = end_datetime - timedelta(hours=12)

    input_products: list[InputProduct] = [
        InputProduct(
            name="S1CADUS",
            cadip_session=item_session.id,
            collection_name=Collection.S1_SESSION.value,
        ),
    ]

    await call_dpr_flow(
        owner_id=owner_identifier,
        prefect_settings=DEFAULT_CONFIGURATION,
        dask_cluster_label=dask_cluster_label,
        input_products=input_products,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        satellite_identifier=satellite_identifier,
    )
