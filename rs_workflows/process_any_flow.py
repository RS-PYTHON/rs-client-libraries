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

"""Process systematic and ondemand flow implementation"""

import datetime

from prefect import flow, task
from pystac import ItemCollection

from rs_client.stac.prip_client import PripClient
from rs_common.utils import create_valcover_filter
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, RetryConfig
from rs_workflows.staging_flow import staging_task
from rs_workflows.utils.prefect import get_logger
from typing import Any
from enum import Enum
from prefect.variables import Variable
import json
from pathlib import Path
from jsonschema import validate, ValidationError



from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnvArgs,
    FlowGeneratedProduct,
    FlowInputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)

class ProcessingId(str, Enum):
    """
    Processing Identifier : usefull to determine configuration file pattern.
    """
    S3_L0 = "s3l0"
    S3_OLCI_L1 = "s3l1olci"
    S3_OLCI_L2 = "s3l2olci"
    

    

@flow(name="process-any")
async def process_any(
    env: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    processing_id: ProcessingId,
    workflow: WorkflowType = WorkflowType.ON_DEMAND,
):
    
    logger = get_logger()

    ##################################################
    # Check default settings from Prefect variable
    ##################################################
    default_settings_var_name:str = f'{processing_id}-default-setting'
    logger.info(f"Read prefect variable named '{default_settings_var_name}'")

    default_settings = Variable.get(default_settings_var_name)
    if default_settings is None:
        raise RuntimeError(f"❌ Prefect variable {default_settings_var_name!r} is missing")

    # If Prefect variable return JSON (str), we transform it on dic
    if isinstance(default_settings, str):
        try:
            default_settings = json.loads(default_settings)
        except json.JSONDecodeError as err:
            raise ValueError(f"❌ Prefect variable {default_settings_var_name!r} contains invalid JSON") from err

    # Check JSON schema
    schema = Path("./schemas/processor_default_settings.schema.json")
    if not schema.is_file():
        raise FileNotFoundError(f"❌ Schema file not found: {schema.resolve()}")
    with open(schema, "r", encoding="utf-8") as f:
        default_settings_schema = json.load(f)

    try:
        validate(instance=default_settings, schema=default_settings_schema)
    except ValidationError as err:
        raise ValueError(
            f"❌ Invalid JSON schema for var set in Prefect variable {default_settings_var_name!r}:\n{err.message}"
        ) from err