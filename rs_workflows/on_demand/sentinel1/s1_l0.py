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
from rs_workflows.on_demand.common.staging import stage_session_common
from rs_workflows.utils.cadip import get_cadip_station
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dask import is_dask_cluster_running
from rs_workflows.utils.dpr import call_dpr_flow
from prefect.variables import Variable


class Collection(str, Enum):
    S1_SESSION = "s01-cadip-session"


DEFAULT_CONFIGURATION: str = "s1-l0-default-setting"


@task(name="process sentinel-1 level-0")
async def process_s1l0(
    session: str,
    owner_identifier: str = "copernicus",
    dask_cluster_label: str = "",
    verbose: bool = False,
):

    # Retrieve dask cluster label

    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    with flow_env.start_span(__name__, "sentinel1-level0"):
        

        
        

    # Prepare the input for the Sentinel-1
    # The satellite name can be retrieved from the 3 first caracters of the session name
    satellite_identifier = session[:3].upper()
    end_datetime = datetime.fromisoformat(item_session.properties.get("published"))
    start_datetime = end_datetime
    input_products: list[InputProduct] = [
        InputProduct(
            name="S1CADUS",
            cadip_session=item_session.id,
            collection_name=Collection.S1_SESSION.value,
        ),
    ]

    await call_dpr_flow(
        FlowEnvArgs(owner_id=owner_identifier),
        prefect_settings=DEFAULT_CONFIGURATION,
        dask_cluster_label=dask_cluster_label,
        input_products=input_products,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        satellite_identifier=satellite_identifier,
    )
