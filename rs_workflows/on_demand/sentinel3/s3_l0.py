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

"""sentinel 3 Level-0 processing."""

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



