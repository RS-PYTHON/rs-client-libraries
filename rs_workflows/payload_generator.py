# Copyright 2025 CS Group
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

"""."""


import yaml
from prefect import get_run_logger, task
from pydantic import BaseModel
from pystac import ItemCollection
from typing import Dict
from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_client.stac.catalog_client import CatalogClient
from typing import Dict
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.record_performance import record_performance_indicators
from rs_workflows.payload_template import (
    PayloadSchema, 
    GeneralConfiguration,
    ExternalModule, 
    Breakpoints, 
    WorkflowStep,
    IOConfig, 
    InputProduct, 
    OutputProduct, 
    AdfConfig,
    DaskContext, 
    EOQCConfig, 
    StoreParams,
)

def build_workflow_step(unit):
    # get inputs
    input_products = [Dict[str, str]]
    if "input_products" in unit:
        for input_product in unit["input_products"]:
            if isinstance(input_product, dict) and \
                "origin" in input_product and \
                "name" in input_product:
                if "pipeline" in input_product["origin"]:
                    input_products.append({input_product["name"] : input_product["name"]})
                else:
                    input_products.append({input_product["name"] : input_product["origin"]})
    # get adfs
    adfs = [Dict[str, str]]
    if "input_adfs" in unit:
        for input_adf in unit["input_adfs"]:
            if isinstance(input_adf, dict) and "name" in input_adf:
                adfs.append({"dem": input_adf["name"]})          
    # get outputs
    
    # get parameters
          
    try:        
        return WorkflowStep(
            name = unit["name"],
            active = True,
            validate_output = False,
            module = unit["module"],
            processing_unit = unit["name"],            
            inputs = input_products if input_products else None,            
            adfs = adfs if adfs else None,
            outputs = 
            
            parameters = 
        )        
    except KeyError as ke:
        raise ValueError(f"Key {ke} not found in unit list")

def build_io_config(unit):
    return io_config

@task(name="Generate payload file")
async def generate_payload(
    env: FlowEnvArgs,    
    unit_list: list[dict],
    auxip_items: list[(bool, ItemCollection)],
    
) -> dict:
    """
    Write the final payload file from its template version and staged items.

    Args:
        env: Prefect flow environment                
    """

    # TODO: should be moved to dpr_client.py and it should call dpr_client.py::update_configuration

    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "write-payload"):

        workflow_steps = []
        io_units = []
        for unit in unit_list:
            try:
                workflow_steps.append(build_workflow_step(unit))
                io_units.append(build_io_config(unit))
                    
            except KeyError as ke:
                raise ValueError(f"Key {ke} not found in unit list")
        # Build the full payload using the schema

        payload = PayloadSchema(
            general_configuration = GeneralConfiguration(
                                logging={"level": "DEBUG"},
            ),
            workflow = workflow_steps,
            io = 
        )

        return payload
    
    return None