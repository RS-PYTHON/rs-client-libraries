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
from rs_workflows.on_demand.common.staging import stage_session_common
from rs_workflows.utils.cadip import get_cadip_station
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dask import is_dask_cluster_running
from rs_workflows.utils.dpr import call_dpr_flow, read_prefect_variable
from typing import Optional

from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnvArgs,
    GeneratedProduct,
    InputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_client.ogcapi.dpr_client import DprPipeline
from prefect.variables import Variable

DEFAULT_CONFIGURATION_TEMPLATE = "s{mission}-l0-default-setting"


@flow(name="process level-0")
async def process_l0(
    session: str,
    owner_identifier: str="",    
    dask_cluster_label: str="",
    session_collection: str="",
    processor_name: str="",
    processor_version: str="",
    pipeline: Optional[DprPipeline]=None,
    unit: str="",
    priority: Optional[Priority] = None,
    processing_mode: list[ProcessingMode]= [],
    workflow: Optional[WorkflowType] = None,
    generated_product_to_collection_identifier:list[GeneratedProduct] = [],
    auxiliary_product_to_collection_identifier:list[AuxiliaryProductMapping] = [],
    verbose: bool = False) -> None:
    """
    This is the generic l0 processing flow.
    It will call process_s1l0, process_s2l0 or process_s3l0
    
    Only session parameter is mandatory.
    All other parameters can get their default values from Prefect variable.
    
    Args:
        session (str): _description_
        owner_identifier (str, optional): _description_. Defaults to "".
        dask_cluster_label (str, optional): _description_. Defaults to "".
        processor_name (str, optional): _description_. Defaults to "".
        processor_version (str, optional): _description_. Defaults to "".
        pipeline (Optional[DprPipeline], optional): _description_. Defaults to None.
        unit (str, optional): _description_. Defaults to "".
        priority (Optional[Priority], optional): _description_. Defaults to None.
        processing_mode (list[ProcessingMode], optional): _description_. Defaults to [].
        workflow (Optional[WorkflowType], optional): _description_. Defaults to None.
        generated_product_to_collection_identifier (list[GeneratedProduct], optional): _description_. Defaults to [].
        auxiliary_product_to_collection_identifier (list[AuxiliaryProductMapping], optional): _description_. Defaults to [].
    """
    logger = get_run_logger()
    logger.info(f"Mode verbose is set to {verbose}")

    # Check session name format
    pattern = re.compile(r"^S[123]._")
    if not pattern.match(session):
        logger.error("❌ Bad Sentinel-1,2,3 session name.")
        raise ValueError(f"Invalid session name: '{session}'")
    
    # We detect the mission
    mission:str = session[2]
    logger.info(f"✔️ Sentinel-{mission} session name is correct.")
    
    # We override some parameters with default configuration
    settings: dict = await Variable.get(DEFAULT_CONFIGURATION_TEMPLATE.format(mission=mission))
    owner_identifier = owner_identifier or settings["owner_identifier"]
    dask_cluster_label = dask_cluster_label or settings["dask_cluster_name"]
    session_collection = session_collection or settings["session_collection"]
    cadip_collections:list[str] = settings["cadip_collections"]
    
    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    with flow_env.start_span(__name__, "start-level0-processing"):
        # Check that the chosen dask_cluster_label is deployed
        if await is_dask_cluster_running(dask_cluster_label) == False:
            raise ValueError(f"❌ '{dask_cluster_label}' is unknown or not ready.")
        
        # Try to retrieve the session on the collection
        item_session: Item = await get_single_catalog_item(flow_env, session, [session_collection])
        
        # If the session is not on the rs-catalog, we will try to stage it
        if item_session is None:
            logger.info(f"Try to stage session  {session} from {mission} stations :{cadip_collections}")
            station = await get_cadip_station(
                flow_env,
                session,
                cadip_collections,
            )
            if station is not None:
                await stage_session_common(flow_env, station, session)
                item_session = await get_single_catalog_item(flow_env, session, [session])

        #XX
        if item_session:
            xx