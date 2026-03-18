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
    FlowEnv,
    FlowEnvArgs,
    GeneratedProduct
)
from rs_workflows.on_demand.common.staging import stage_session_common
from rs_workflows.utils.cadip import get_cadip_station
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dask import is_dask_cluster_running
from rs_workflows.on_demand.common.var import DEFAULT_PREFECT_CONFIGURATION

from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    FlowEnvArgs,
    GeneratedProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_client.ogcapi.dpr_client import DprPipeline
from prefect.variables import Variable

from pydantic import BaseModel, Field
from typing import Optional, List
from prefect.variables import Variable
from rs_workflows.on_demand.sentinel1 import process_s1l0

class Level0FlowParams(BaseModel):
    owner_identifier: str = ""
    dask_cluster_label: str = ""
    session_collection: str = ""
    processor_name: str = ""
    processor_version: str = ""
    pipeline: Optional[DprPipeline] = None
    unit: str = ""
    priority: Optional[Priority] = None
    processing_mode: List[ProcessingMode] = Field(default_factory=list)
    workflow: Optional[WorkflowType] = None
    generated_product_to_collection_identifier: List[GeneratedProduct] = Field(default_factory=list)
    auxiliary_product_to_collection_identifier: List[AuxiliaryProductMapping] = Field(default_factory=list)
    cadip_collections: List[str] = Field(default_factory=list)  # ajouté car tu l'utilises


    async def resolve(self, mission: str, level: str = "0") -> "Level0FlowParams":
        settings = await Variable.get(
            DEFAULT_PREFECT_CONFIGURATION.format(mission=mission, level=level)
        )
        return Level0FlowParams(
            owner_identifier=self.owner_identifier or settings.get("owner_identifier", ""),
            dask_cluster_label=self.dask_cluster_label or settings.get("dask_cluster_name",""),
            session_collection=self.session_collection or settings.get("session_collection",""),
            processor_name=self.processor_name or settings.get("processor_name", ""),
            processor_version=self.processor_version or settings.get("processor_version", ""),
            pipeline=self.pipeline,
            unit=self.unit or settings.get("unit", ""),
            priority=self.priority or settings.get("priority"),
            processing_mode=self.processing_mode or settings.get("processing_mode", []),
            workflow=self.workflow or settings.get("workflow"),
            generated_product_to_collection_identifier=(
                self.generated_product_to_collection_identifier
                or settings.get("generated_product_to_collection_identifier", [])
            ),
            auxiliary_product_to_collection_identifier=(
                self.auxiliary_product_to_collection_identifier
                or settings.get("auxiliary_product_to_collection_identifier", [])
            ),
            cadip_collections=settings["cadip_collections"],
        )



@flow(name="process level-0")
async def process_l0(
    session: str,
    flow_params : Optional[Level0FlowParams] = None,
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
    mission:str = session[2]
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
