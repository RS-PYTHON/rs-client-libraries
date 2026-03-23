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
from datetime import datetime
from prefect import flow, get_run_logger, task
from pystac import Item

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
)
from rs_workflows.on_demand.common.types import (
    DEFAULT_PREFECT_CONFIGURATION,
    Level0FlowParams,
)

@flow(name="process sentinel-3 level-0")
async def process_s3l0(session: str, flow_params: Level0FlowParams, verbose: bool = False):
    logger = get_run_logger()
    logger.info(f"Mode verbose is set to {verbose}")
    
    # Override of some parameters with default configuration
    flow_params = flow_params or Level0FlowParams()
    p = await flow_params.resolve(mission="3", level="0")
        
    flow_env = FlowEnv(FlowEnvArgs(owner_id=p.owner_identifier))
    with flow_env.start_span(__name__, "sentinel3-level0-processing"):
        item_session:Item = await get_single_catalog_item(flow_env, session, [p.session_collection])

        if item_session:
            # Prepare the input for the Sentinel-1
            # The satellite name can be retrieved from the 3 first caracters of the session name
            satellite_identifier = f"sentinel-3{session[:3].lower()}"
            end_datetime = datetime.fromisoformat(item_session.properties.get("published"))
            start_datetime = end_datetime
            input_products: list[FlowInputProduct] = [
                FlowInputProduct(
                    name="S3ACADUS",
                    cadip_session=item_session.id,
                    collection_name=p.session_collection,
                ),
            ]
            await call_dpr_flow(
                FlowEnvArgs(owner_id=p.owner_identifier),
                input_products=input_products,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                satellite_identifier=satellite_identifier,
                dask_cluster_label=p.dask_cluster_label,
                processor_name=p.processor_name,                
                processor_version=p.processor_version,
                pipeline=p.pipeline,
                unit=p.unit,
                priority=p.priority,
                processing_mode=p.processing_mode,
                workflow=p.workflow,            
                generated_product_to_collection_identifier=p.generated_product_to_collection_identifier,
                auxiliary_product_to_collection_identifier=p.auxiliary_product_to_collection_identifier,
            )
        else:
            logger.error(f"❌ The processing cannot be launched.")

@task(name="process sentinel-3 level-0")
async def process_s3l0_task(*args, **kwargs) -> None:
    """See: dpr_processing"""
    return await process_s3l0.fn(*args, **kwargs)
    

