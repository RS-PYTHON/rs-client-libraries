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

from prefect.variables import Variable
from prefect import flow, get_run_logger
from pydantic import BaseModel, Field
from pystac import Item

from rs_client.ogcapi.dpr_client import DprPipeline
from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    Priority,
    ProcessingMode,
    WorkflowType,
    GeneratedProduct,
 
)


from rs_workflows.on_demand.sentinel1.s1_l0 import process_s1l0
from rs_workflows.on_demand.sentinel3.s3_l0 import process_s3l0

from rs_workflows.on_demand.common.staging import stage_session_common
from .types import (
    DEFAULT_PREFECT_CONFIGURATION,
    Level0FlowParams,
)
from rs_workflows.utils.cadip import get_cadip_station
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dask import is_dask_cluster_running
from typing import Optional, List


class Level0FlowParams2(BaseModel):
    owner_identifier: str = Field(
        default="",
        title="Owner Identifier",
        description="Identifier of the data owner used for processing and configuration."
    )

    dask_cluster_label: str = Field(
        default="",
        title="Dask Cluster Label",
        description="Name of the Dask cluster used for distributed execution."
    )

    session_collection: str = Field(
        default="",
        title="Session Collection",
        description="CADIP collection name containing the Sentinel session."
    )

    processor_name: str = Field(
        default="",
        title="Processor Name",
        description="Name of the processor used for Level-0 processing."
    )

    processor_version: str = Field(
        default="",
        title="Processor Version",
        description="Version of the processor used for Level-0 processing."
    )

    pipeline: Optional[DprPipeline] = Field(
        default=None,
        title="Pipeline",
        description="DPR pipeline to use for processing."
    )

    unit: str = Field(
        default="",
        title="Unit",
        description="Processing unit or internal identifier."
    )

    priority: Optional[Priority] = Field(
        default=None,
        title="Priority",
        description="Processing priority (low, normal, high)."
    )

    processing_mode: List[ProcessingMode] = Field(
        default_factory=list,
        title="Processing Mode",
        description="List of processing modes to apply."
    )

    workflow: Optional[WorkflowType] = Field(
        default=None,
        title="Workflow Type",
        description="Workflow type to execute (on-demand, scheduled, etc.)."
    )

    generated_product_to_collection_identifier: List[GeneratedProduct] = Field(
        default_factory=list,
        title="Generated Product Mapping",
        description="List of generated products and their target collections."
    )

    #auxiliary_product_to_collection_identifier: List[AuxiliaryProductMapping] = Field(
    #    default_factory=list,
    #    title="Auxiliary Product Mapping",
    #    description="List of auxiliary products and their target collections."
    #)

    cadip_collections: List[str] = Field(
        default_factory=list,
        title="CADIP Collections",
        description="List of CADIP collections to query for session retrieval."
    )

    async def resolve(self, mission: str, level: str = "0") -> "Level0FlowParams":
        var_name = DEFAULT_PREFECT_CONFIGURATION.format(mission=mission, level=level)
        settings: dict = await Variable.get(var_name)

        if settings is None:
            raise ValueError(f"❌ Prefect variable '{var_name}' not found")

        return Level0FlowParams(
            owner_identifier=self.owner_identifier or settings.get("owner_identifier", ""),
            dask_cluster_label=self.dask_cluster_label or settings.get("dask_cluster_name", ""),
            session_collection=self.session_collection or settings.get("session_collection", ""),
            processor_name=self.processor_name or settings.get("processor", {}).get("name", ""),
            processor_version=self.processor_version or settings.get("processor", {}).get("version", ""),
            pipeline=self.pipeline or settings.get("pipeline", None),
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
            cadip_collections=settings.get("cadip_collections", []),
        )



@flow(name="test param")
async def test_param(
    session: str,
    flow_params: Level0FlowParams,
    verbose2: bool = False,
):
    logger = get_run_logger()
    logger.info("test")


@flow(name="process level-0")
async def process_l0(
    session: str,
    flow_params: Level0FlowParams,
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
    mission: str = session[1]
    logger.info(f"✔️ Sentinel-{mission} session name is correct.")

    # Override of some parameters with default configuration
    p = await flow_params.resolve(mission, level="1")

    flow_env = FlowEnv(FlowEnvArgs(owner_id=p.owner_identifier))
    with flow_env.start_span(__name__, "level0-processing"):
        found = False

        # Check that the chosen dask_cluster_label is deployed
        if await is_dask_cluster_running(p.dask_cluster_label) == False:
            raise ValueError(f"❌ '{p.dask_cluster_label}' is unknown or not ready.")

        # Try to retrieve the session on the collection
        item_session: Item = await get_single_catalog_item(flow_env, session, [p.session_collection])

        # If the session is not on the rs-catalog, we will try to stage it
        if item_session:
            found = True
        else:
            logger.info(f"Try to stage session  {session} from {mission} stations :{p.cadip_collections}")
            station = await get_cadip_station(
                flow_env,
                session,
                p.cadip_collections,
            )
            if station is not None:
                found = await stage_session_common(flow_env, station, session)

        # The session is stagged at this step.
        # We can call the flow
        logger.info(f"We start Sentinel-{mission} processing.")
        if found:
            match int(mission):
                case 1:
                    await process_s1l0(session, p, verbose)
                case 3:
                    await process_s3l0(session, p, verbose)
