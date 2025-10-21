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


from typing import Dict

import yaml
from prefect import get_run_logger, task
from pydantic import BaseModel
from pystac import ItemCollection

from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.payload_template import (
    AdfConfig,
    Breakpoints,
    DaskContext,
    EOQCConfig,
    ExternalModule,
    GeneralConfiguration,
    InputProduct,
    IOConfig,
    OutputProduct,
    PayloadSchema,
    StorageOptions,
    StoreOptionsWrapper,
    StoreParams,
    WorkflowStep,
)
from rs_workflows.record_performance import record_performance_indicators


def build_workflow_step(unit):
    # get inputs
    input_products = [Dict[str, str]]
    if "input_products" in unit:
        for input_product in unit["input_products"]:
            if isinstance(input_product, dict) and \
                "origin" in input_product and \
                "name" in input_product:
                if "pipeline_input" in input_product["origin"]:
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
    output_products = [Dict[str, str]]
    if "output_products" in unit:
        for output_product in unit["output_products"]:
            if isinstance(output_product, dict) and "name" in output_product:                
                left_part = output_product["regex"] if "regex" in output_product else output_product["name"]
                right_part = output_product["name"]
                if "origin" in output_product and "pipeline_output" not in output_product["origin"]:
                    right_part = output_product["origin"]
                output_products.append({left_part : right_part})
    
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
            outputs = output_products,            
            parameters = None,
        )        
    except KeyError as ke:
        raise ValueError(f"Key {ke} not found in unit list")

def build_io_config(unit, flow_input_product):
    """
    Build an IOConfig object containing input and output product definitions
    for a processing unit.

    Args:
        unit (dict): A dictionary describing the unit configuration.
            Must include 'input_products' and 'output_products' lists,
            each containing product metadata such as 'name', 'store_type', and optional 'type' or 'opening_mode'.
        flow_input_product (dict): A mapping of input product names to file paths.
            If an input name is not found, a default path ('/some/path') is used.

    Returns:
        IOConfig: An instance containing populated input and output products with
        default paths, types, and store parameters.
    """
    inputs = [
        InputProduct(
            id=inp['name'],
            # path is selected from flow_input_product with same name, otherwise, default path
            path=flow_input_product.get(inp['name'], '/some/path'),
            type=inp.get('type', 'filename'),
            store_type=inp['store_type']
        )
        for inp in unit['input_products']
    ]
    # To be updated, read from block or from env?
    store_params = StoreParams(
        options=[
            StoreOptionsWrapper(
                storage_options=[
                    StorageOptions(
                        key="${S3_ACCESSKEY_CLUSTER}",
                        secret="${S3_SECRETKEY_CLUSTER}",
                        client_kwargs={
                            "endpoint_url": "${S3_ENDPOINT_CLUSTER}",
                            "region_name": "${S3_REGION_CLUSTER}"
                        }
                    )
                ]
            )
        ]
    )
    outputs = [
        OutputProduct(
            id=outp['name'],
            path='/tmp/output',
            store_type=outp['store_type'],
            store_params = store_params,
            type=outp.get('type', 'filename'),
            opening_mode=outp.get('opening_mode', 'CREATE')
        )
        for outp in unit['output_products']
    ]

    io_config = IOConfig()
    io_config.input_products = inputs
    io_config.output_products = outputs

    return io_config

@task(name="Generate payload file")
async def generate_payload(
    env: FlowEnvArgs,
    unit_list: list[dict],
    auxip_items: list[(bool, ItemCollection)],
    dpr_input
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
        io_config = []
        logger.info("Geting workflow and I/O sections")
        for unit in unit_list:
            try:
                workflow_steps.append(build_workflow_step(unit))
                io_config.append(build_io_config(unit, dpr_input))

            except KeyError as ke:
                raise ValueError(f"Key {ke} not found in unit list") from ke
        # Build the full payload using the schema
        logger.info("Building the payload")
        payload = PayloadSchema(
            general_configuration = GeneralConfiguration(
                                logging={"level": "DEBUG"},
            ),
            workflow = workflow_steps,
            io = io_config,
        )

        return payload

    return None
