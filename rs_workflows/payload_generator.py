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
import json
import os

from prefect import get_run_logger, task

from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnv,
)
from rs_workflows.payload_template import (  # Breakpoints,; DaskContext,; EOQCConfig,; ExternalModule,
    AdfConfig,
    GeneralConfiguration,
    InputProduct,
    IOConfig,
    LoggingConfig,
    OutputProduct,
    PayloadSchema,
    StorageOptions,
    StoreOptionsWrapper,
    StoreParams,
    WorkflowStep,
)


def build_workflow_step(unit):
    """
    Constructs a `WorkflowStep` instance from a unit configuration dictionary.

    This function parses the given processing unit definition, extracting input products,
    auxiliary data files (ADFs), and output products. It then returns a `WorkflowStep`
    object ready to be integrated into a full processing payload schema.

    Args:
        unit (dict): A dictionary defining a single workflow unit.
            Expected keys include:
                - "name" (str): The unit name.
                - "module" (str): The module path or identifier.
                - "input_products" (list[dict], optional): List of input product mappings.
                - "input_adfs" (list[dict], optional): List of auxiliary data files.
                - "output_products" (list[dict], optional): List of output product mappings.

    Returns:
        WorkflowStep: A fully initialized workflow step object.

    Raises:
        ValueError: If a required key is missing from the provided unit dictionary.
    """
    # get inputs
    input_products = []
    if "input_products" in unit:
        for input_product in unit["input_products"]:
            if isinstance(input_product, dict) and "origin" in input_product and "name" in input_product:
                if "pipeline_input" in input_product["origin"]:
                    input_products.append({input_product["name"]: input_product["name"]})
                else:
                    input_products.append({input_product["name"]: input_product["origin"]})
    # get adfs
    adfs = []
    if "input_adfs" in unit:
        for input_adf in unit["input_adfs"]:
            if isinstance(input_adf, dict) and "name" in input_adf:
                adfs.append({"dem": input_adf["name"]})
    # get outputs
    output_products = []
    if "output_products" in unit:
        for output_product in unit["output_products"]:
            if isinstance(output_product, dict) and "name" in output_product:
                left_part = output_product["regex"] if "regex" in output_product else output_product["name"]
                right_part = output_product["name"]  # ==> "*pdf" : "name"
                # if "origin" in output_product and "pipeline_output" not in output_product["origin"]:
                #     right_part = output_product["origin"]
                output_products.append({left_part: right_part})
    try:
        return WorkflowStep(
            name=unit["name"],
            active=True,
            validate=False,
            module=unit["module"],
            processing_unit=unit["name"],
            inputs=input_products if input_products else None,
            adfs=adfs if adfs else None,
            outputs=output_products,
            parameters=None,
        )
    except KeyError as ke:
        raise ValueError(f"Key {ke} not found in unit list") from ke


def get_io(unit, dpr_process_in: DprProcessIn, store_params: StoreParams):
    """
    Builds input and output product configurations for a workflow step.

    This function constructs lists of `InputProduct` and `OutputProduct` objects
    based on the current processing unit definition and the dynamic process input data.

    Args:
        unit (dict): The processing unit dictionary containing I/O definitions.
        dpr_process_in (DprProcessIn): Input configuration object containing paths
            and input product mappings for the workflow.
        store_params (StoreParams): Storage configuration parameters, including
            credentials and endpoint information for S3 or equivalent storage.

    Returns:
        tuple[list[InputProduct], list[OutputProduct]]: A tuple containing
        input and output product lists ready for inclusion in an `IOConfig`.

    Raises:
        KeyError: If a required field (e.g., product name or store_type) is missing
        in the input or output product definitions.
    """

    inputs = [
        InputProduct(
            id=inp["name"],
            # path is selected from flow_input_product with same name, otherwise, default path
            path=dpr_process_in.input_products[inp["name"]],
            type=inp.get("type", "filename"),
            store_type=inp["store_type"],
            store_params=store_params,
        )
        for inp in unit["input_products"]
    ]

    outputs = [
        OutputProduct(
            id=outp["name"],
            path=dpr_process_in.s3_output_data,
            store_type=outp["store_type"],
            store_params=store_params,
            type=outp.get("type", "filename"),
            opening_mode=outp.get("opening_mode", "CREATE"),
        )
        for outp in unit["output_products"]
    ]
    return inputs, outputs


def load_store_params_from_config(config_path: str = "/etc/storage_configuration.json") -> StoreParams:
    """
    Loads storage configuration from a JSON file and constructs a StoreParams object.

    Args:
        config_path (str): Path to the storage configuration JSON file.
            Defaults to '/etc/storage_configuration.json'.

    Returns:
        StoreParams: The StoreParams object built from the configuration file.

    Raises:
        FileNotFoundError: If the JSON file does not exist.
        ValueError: If the JSON structure is invalid or missing required fields.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Storage configuration file not found: {config_path}")

    with open(config_path, encoding="utf-8") as f:
        storage_config = json.load(f)

    store_options_wrappers = []

    for storage_entry in storage_config.get("storage", []):
        name = storage_entry.get("name")
        if not name:
            continue

        # S3 configuration
        if name == "s3":
            opts = StorageOptions(
                key=f"${{{storage_entry['storage_options']['key']}}}",
                secret=f"${{{storage_entry['storage_options']['secret']}}}",
                client_kwargs={
                    "endpoint_url": storage_entry["storage_options"]["endpoint_url"],
                    "region_name": storage_entry["storage_options"]["region_name"],
                },
            )
            store_options_wrappers.append(StoreOptionsWrapper(storage_options=[opts]))

        # Non-S3 storage: shared_disk or local_disk
        else:
            opts = StorageOptions(
                key=None,
                secret=None,
                client_kwargs=None,
                relative_path=storage_entry.get("relative_path"),
                opening_mode=storage_entry.get("opening_mode", "CREATE_OVERWRITE"),
            )
            store_options_wrappers.append(StoreOptionsWrapper(storage_options=[opts]))

    return StoreParams(options=store_options_wrappers)


@task(name="Generate payload file")
def generate_payload(  # pylint: disable=unused-argument
    env: FlowEnv,
    unit_list: list[dict],
    adfs: list[tuple[str, str]],
    dpr_process_in: DprProcessIn,
) -> PayloadSchema:
    """
    Assembles and generates a payload schema for a DPR (Data Processing Request) job.

    This Prefect task builds the payload definition dynamically based on the provided
    workflow units, auxiliary data files, and input configuration. It produces a
    `PayloadSchema` object compatible with RS-Server DPR jobs.

    Args:
        env (FlowEnv): Environment configuration for the Prefect flow, including
            credentials, tracing, and runtime context.
        unit_list (list[dict]): List of workflow unit definitions containing I/O
            specifications and processing parameters.
        adfs (list[tuple[str, str]]): List of auxiliary item
            tuples, where each tuple includes the eopf type and the s3 storage path.
        dpr_process_in (DprProcessIn): DPR input process definition containing
            product paths and parameters.

    Returns:
        dict: A dictionary representation of the generated `PayloadSchema`.

    Raises:
        ValueError: If a required key is missing in one of the unit definitions.
        Exception: For any unexpected error during payload assembly.
    """

    # TODO: should be moved to dpr_client.py and it should call dpr_client.py::update_configuration

    logger = get_run_logger()
    # the values should be name of the secrets, and not the values of these secrets.
    # it's up to the processor to retrieve the values at the running time
    # The storage_configuration.json file should be mounted in /etc/storage_configuration.json
    # in cluster mode, it should be mounted as volume from a predefined (?) configmap

    logger.info("Loading StoreParams configuration")
    store_params = load_store_params_from_config()

    workflow_steps = []
    io_config = IOConfig()
    logger.info("Geting workflow and I/O sections")
    for unit in unit_list:
        try:
            workflow_steps.append(build_workflow_step(unit))
            input_products, output_products = get_io(unit, dpr_process_in, store_params)
            io_config.input_products += input_products
            io_config.output_products += output_products
        except KeyError as ke:
            raise ValueError(f"Key {ke} not found in unit list") from ke

    io_config.adfs = [AdfConfig(id=adf[0], path=adf[1], store_params=store_params) for adf in adfs]
    # Build the full payload using the schema
    logger.info("Building the payload")
    payload = PayloadSchema(
        # add some default params, as stated in a comment from jira (story 800)
        general_configuration=GeneralConfiguration(
            logging=LoggingConfig(level="DEBUG"),
            triggering__use_basic_logging=True,
            triggering__wait_before_exit=10,
        ),
        workflow=workflow_steps,
        io=io_config,  # type: ignore
    )
    logger.debug(f"Generated payload file: \n {payload}")
    return payload
