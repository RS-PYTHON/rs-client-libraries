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

"""DPR flow implementation"""

import copy
import tempfile
from os import path as osp

import yaml
from prefect import get_run_logger, task
from pydantic import BaseModel
from pystac import ItemCollection

from rs_client.ogcapi.dpr_client import DprClient
from rs_client.stac.catalog_client import CatalogClient
from rs_common import prefect_utils
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, ProcessorEnum


class PayloadValues(BaseModel):
    """Values read from the payload file."""

    module: str
    processing_unit: str


@task(name="Read payload values")
async def read_payload_values(s3_payload: str) -> PayloadValues:
    """
    Read values from the payload file.

    Args:
        s3_payload: S3 bucket location of the DPR payload file.
    """
    logger = get_run_logger()

    # Download the payload file into a temp file
    with tempfile.NamedTemporaryFile() as temp:
        await prefect_utils.s3_download_file(s3_payload, temp.name)

        # Read it as a yaml file
        with open(temp.name, encoding="utf-8") as opened:
            payload = yaml.safe_load(opened)

    workflow = payload.get("workflow", [])
    for step in workflow:
        if "name" not in step:
            continue
        module = step.get("module")
        processing_unit = step.get("processing_unit")
        if not module:
            raise ValueError(f"Missing 'module' in processor payload configuration: {step['name']}")
        if not processing_unit:
            raise ValueError(f"Missing 'processing_unit' in processor payload configuration: {step['name']}")
        logger.info(f"For {step['name']} found module: {module} and processing_unit: {processing_unit}")
        return PayloadValues(module=module, processing_unit=processing_unit)

    raise ValueError(f"No processor defined in the workflow of payload file {s3_payload}")


@task(name="Read TaskTable")
async def read_tasktable(
    env: FlowEnvArgs,
    processor: ProcessorEnum,  # pylint: disable=unused-argument
    payload_values: PayloadValues,  # pylint: disable=unused-argument
    cadip_items: ItemCollection,  # pylint: disable=unused-argument
) -> dict:
    """
    Read Auxip CQL2 filter from the processor tasktable. See:
    https://gitlab.eopf.copernicus.eu/cpm/eopf-cpm/-/blob/main/docs/source/processor-orchestration-guide/tasktables.rst

    Args:
        env: Prefect flow environment
        processor: DPR processor name
        payload_values: Values read from the payload file
        cadip_items: Results of the Cadip search
    """
    # logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "read-tasktable"):

        # TODO 2: for now the DPR endpoint returns an empty dict which raises a validation error.
        # So just return an empty dict for now.
        # auxip_cql2 = flow_env.rs_client.get_dpr_client().get_process(processor.value)
        return {}


@task(name="Write payload file")
async def write_payload(
    env: FlowEnvArgs,
    s3_payload_template: str,
    item_ids: list[str],
    catalog_collection_identifier: str,
    s3_output_data: str,
    s3_payload_run: str,
):
    """
    Write the final payload file from its template version and staged items.

    Args:
        env: Prefect flow environment
        s3_payload_template: S3 bucket location of the input template DPR payload file.
        item_ids: Item IDs to be processed by the processor.
        catalog_collection_identifier: Catalog collection identifier where the items are staged
        s3_output_data: S3 bucket location of the output processed products.
        s3_payload_run: S3 bucket location of the output final DPR payload file.
    """

    # TODO: should be moved to dpr_client.py and it should call dpr_client.py::update_configuration

    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "write-payload"):

        # Download the payload file into a temp file
        with tempfile.NamedTemporaryFile() as temp:
            await prefect_utils.s3_download_file(s3_payload_template, temp.name)

            # Read it as a yaml file
            with open(temp.name, encoding="utf-8") as opened:
                payload = yaml.safe_load(opened)

        # Create mandatory keys if missing
        payload.setdefault("I/O", {})

        # Retrieve a default template from the existing input_products if available;
        # otherwise, use a minimal default.
        if payload["I/O"].get("input_products"):
            default_template = copy.deepcopy(payload["I/O"]["input_products"][0])
        else:
            default_template = {"id": None, "path": None}

        # Retrieve a default template from the existing output_products if available;
        # otherwise, use a minimal default.
        if payload["I/O"].get("output_products"):
            default_template_out = copy.deepcopy(payload["I/O"]["output_products"][0])
        else:
            default_template_out = {"id": None, "path": None}
        output_ids = {}

        for wf in payload.get("workflow", []):
            outputs = wf.get("outputs", {})
            if outputs:
                # Merge outputs from all workflows; keys are not used here, only the values (output IDs)
                output_ids.update(outputs)
            else:
                raise ValueError("At least on output should be in outputs workflow")

        new_input_products = []
        workflow_inputs = {}
        cadip_index = 1
        auxip_index = 1

        # Get the staged items from the catalog
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        catalog_items = catalog_client.get_items(catalog_collection_identifier, item_ids)

        # For each staged item
        for item in catalog_items:
            if len(item.assets) == 0:
                raise ValueError(f"No assets in item {item.id}")

            first_asset = list(item.assets.items())[0][1]
            logger.info(f"first_asset = {first_asset}")
            full_s3_href = first_asset.extra_fields.get("alternate", {}).get("s3", {}).get("href")
            if not full_s3_href:
                raise ValueError(f"S3 HREF not found in extra fields for item {item.id}")

            session_s3_href = "/".join(full_s3_href.split("/")[:-1])
            logger.info(f"Session {item.id} has S3_HREF: {session_s3_href}")

            # Create a new input product using the default template.
            new_product = copy.deepcopy(default_template)
            new_product["id"] = item.id
            new_product["path"] = session_s3_href

            # these are not accepted by the real processor
            # if "cadip:id" in item.properties:
            #     new_product["store_type"] = "cadu"
            # if "auxip:id" in item.properties:
            #     new_product["store_type"] = "aux"

            new_product["store_type"] = "safe"

            new_input_products.append(new_product)

            if "cadip:id" in item.properties:
                workflow_inputs[f"CADU{cadip_index}"] = item.id
                cadip_index += 1
            if "auxip:id" in item.properties:
                workflow_inputs[f"AUX{auxip_index}"] = item.id
                auxip_index += 1

        # Build a new output_products list using the default template for each output
        new_output_products = []
        for _, output_id in output_ids.items():
            # Create a new entry from the default template
            product_entry = copy.deepcopy(default_template_out)
            # Update the id and path accordingly
            product_entry["id"] = output_id
            product_entry["path"] = f"{s3_output_data}"  # with /{output_id} ?
            new_output_products.append(product_entry)

        # Update payload with new input products and workflow inputs
        payload["I/O"]["input_products"] = new_input_products
        for wf in payload.get("workflow", []):
            wf["inputs"] = workflow_inputs
        payload["I/O"]["output_products"] = new_output_products

        # Write back the payload contents to a temp file
        with tempfile.NamedTemporaryFile() as temp:
            with open(temp.name, "w", encoding="utf-8") as opened:
                yaml.dump(payload, opened, default_flow_style=False, sort_keys=False)

            with open(temp.name, encoding="utf-8") as opened:
                payload_after = opened.read()
            logger.info(f"Payload file after 'write_payload':\n {payload_after}")

            # Upload the new config payload file back to S3
            await prefect_utils.s3_upload_file(temp.name, s3_payload_run)


@task(name="Run DPR processor")
async def run_processor(
    env: FlowEnvArgs,
    processor: ProcessorEnum,
    s3_payload_run: str,
    use_dpr_mockup: bool = False,
) -> list[dict]:
    """
    Run the DPR processor.

    Args:
        env: Prefect flow environment
        processor: DPR processor name
        s3_payload_run: S3 bucket location of the output final DPR payload file.
        use_dpr_mockup: Use the real or the mockup DPR processor ?
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "run-processor"):

        # Trigger the processor run from the dpr service
        dpr_client: DprClient = flow_env.rs_client.get_dpr_client()
        job_status = dpr_client.run_process(
            process=processor.value,
            s3_config_dir=osp.dirname(s3_payload_run),
            payload_subpath=osp.basename(s3_payload_run),
            s3_report_dir=None,
            use_dpr_mockup=use_dpr_mockup,
        )

        # Wait for the job to finish
        return dpr_client.wait_for_job(job_status, logger, f"{processor.value!r} processor")
