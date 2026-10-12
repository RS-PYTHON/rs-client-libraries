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
from rs_workflows.utils.dask import is_dask_cluster_running

import json
from pathlib import Path
from jsonschema import validate, ValidationError
from rs_workflows.utils.dpr import dpr_processing_task, call_dpr_flow


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

DASK_CLUSTER_STAGING = "dask-staging"

class ProcessingId(str, Enum):
    """
    Processing Identifier : usefull to determine configuration file pattern.
    """
    S3_L0 = "s3l0"
    S3_OLCI_L1 = "s3l1olci"
    S3_OLCI_L2 = "s3l2olci"
    

    

@flow(name="process-any")
async def process_any(
    env_args: FlowEnvArgs,
    input_products: list[FlowInputProduct],
    external_variables: dict[str, Any],
    processing_id: ProcessingId,
    workflow: WorkflowType = WorkflowType.ON_DEMAND,
):
    
    logger = get_logger()

    ########################################################################
    # Check Prefect variable {processing_id.value}-default-setting format
    #########################################################################
    default_settings_var_name:str = f'{processing_id.value}-default-setting'
    logger.info(f"Read prefect variable named '{default_settings_var_name}'")

    default_settings:dict = await Variable.get(default_settings_var_name)
    if default_settings is None:
        raise RuntimeError(f"❌ Prefect variable {default_settings_var_name!r} is missing")

    # Check JSON schema
    schema = Path("rs_workflows/schemas/processor_default_settings.schema.json")
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

    flow_env = FlowEnv(env_args)
    with flow_env.start_span(__name__, f"process-any-{processing_id}"):    
        ##################################################
        # Check cluster dasks
        ##################################################
        processing_dask_cluster = default_settings["dask_cluster_name"]
        if await is_dask_cluster_running(processing_dask_cluster) is False:
            raise ValueError(f"❌ '{processing_dask_cluster}' is unknown or not ready.")
        if await is_dask_cluster_running(DASK_CLUSTER_STAGING) is False:
            raise ValueError(f"❌ 'DASK_CLUSTER_STAGING' is unknown or not ready.")
        


        ##################################################
        # Create all input for DPR processing
        ##################################################
        published_items : list[dict[str, Any]] = await call_dpr_flow(
            env=env_args,
            input_products=input_products,
            external_variables=external_variables,
            dask_cluster_label=processing_dask_cluster,
            processor_name=default_settings["processor"]["name"],
            processor_version=default_settings["processor"]["version"],
            pipeline=default_settings.get("pipeline", None),
            unit=default_settings.get("unit", None),
            priority=default_settings.get("priority", Priority.LOW),
            processing_mode=default_settings.get("processing_mode", [ProcessingMode.ALWAYS]),
            workflow=workflow,
            generated_product_to_collection_identifier=default_settings.get("generated_product_to_collection_identifier", []),
            auxiliary_product_to_collection_identifier = default_settings.get("auxiliary_product_to_collection_identifier", []),
        )

        # TEMP : il faudra mettre générer un évènement pour chacun de ces items.
        for i, item in enumerate(published_items):
            print(f"--- Item {i + 1} ---")
            print(json.dumps(item, indent=2, ensure_ascii=False))