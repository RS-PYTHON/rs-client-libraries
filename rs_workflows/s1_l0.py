# Copyright 2024 CS Group
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

"""Prefect flow for processing a S1 L0 product"""
import json
import os.path as osp
from datetime import datetime
from pathlib import Path

import requests
import yaml
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from pystac import Collection, Extent, SpatialExtent, TemporalExtent

from rs_client.stac_client import StacClient
from rs_common.config import AUXIP_STATION, ECadipStation
from rs_common.logging import Logging
from rs_common.serialization import RsClientSerialization
from rs_workflows.staging import (
    CATALOG_REQUEST_TIMEOUT,
    create_collection_name,
    get_prefect_logger,
    update_stac_catalog,
)

CONFIG_DIR = Path(osp.realpath(osp.dirname(__file__))) / "config"
YAML_TEMPLATE_FILE = "dpr_config_template.yaml"
LOGGER_NAME = "s1_l0_wf"
DPR_PROCESSING_TIMEOUT = 14400  # 4 hours


@task
def start_dpr(dpr_endpoint: str, yaml_dpr_input: dict):
    """Starts the DPR processing with the given YAML input.

    Args:
        dpr_endpoint (str): The endpoint of the DPR processor
        yaml_dpr_input (dict): The YAML input for DPR processing.

    Returns:
        response (dict): The response JSON from the DPR simulator if successful, else None.
    """
    logger = Logging.default(LOGGER_NAME)
    logger.debug("Task start_dpr STARTED")
    logger.info("Faking dpr processing with the following input file:")
    logger.debug(yaml.dump(yaml_dpr_input))
    try:
        response = requests.post(
            dpr_endpoint.rstrip("/") + "/run",
            json=yaml.safe_load(yaml.dump(yaml_dpr_input)),
            timeout=DPR_PROCESSING_TIMEOUT,
        )
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Calling the dpr simulater resulted in exception: %s", e)
        return None

    if int(response.status_code) != 200:
        logger.error(f"The dpr simulator endpoint failed with status code: {response.status_code}")
        return None

    logger.debug("DPR processor results: \n\n")
    for attr in response.json():
        logger.debug(json.dumps(attr, indent=2))
    logger.debug("Task start_dpr FINISHED")
    return response.json()


@task
def build_eopf_triggering_yaml(cadip_files: dict, adgs_files: dict, product_types: list, temp_s3_path: str):
    """Builds the EOPF triggering YAML file using CADIP and ADGS file paths.

    Args:
        cadip_files (dict): CADIP files metadata.
        adgs_files (dict): ADGS files metadata.
        product_types (list): The types for the output products
        temp_s3_path (str): Temporary S3 path.

    Returns:
        response (dict): The generated YAML template with updated inputs and I/O products, or None if unsuccessful.
    Raises:
        FileNotFoundError: If the YAML template file is not found.
        yaml.YAMLError: If there is an error loading the YAML template file.
        IOError: If there is an input/output error while accessing the YAML template file.
    """
    logger = Logging.default(LOGGER_NAME)
    logger.debug("Task build_eopf_triggering_yaml STARTED")
    # Load YAML template
    try:
        with open(CONFIG_DIR / YAML_TEMPLATE_FILE, encoding="utf-8") as yaml_file:
            yaml_template = yaml.safe_load(yaml_file)
    except FileNotFoundError as e:
        logger.exception(f"Could not find the YAML template file: {e}")
        return None
    except yaml.YAMLError as e:
        logger.exception(f"Could not load the YAML template file: {e}")
        return None
    except IOError as e:
        logger.exception(f"Could not find the YAML template file: {e}")
        return None

    # Extract paths for CADIP and ADGS files
    cadip_paths = [file_prop["assets"]["file"]["alternate"]["s3"]["href"] for file_prop in cadip_files]
    adgs_paths = [file_prop["assets"]["file"]["alternate"]["s3"]["href"] for file_prop in adgs_files]
    # create the dictionaries to insert within the yaml template

    # Update the YAML template with inputs and I/O products
    yaml_template["workflow"][0]["inputs"], yaml_template["I/O"]["inputs_products"] = gen_payload_inputs(
        cadip_paths,
        adgs_paths,
    )

    # Update the YAML  with the outpurs
    yaml_template["workflow"][0]["parameters"]["product_types"] = product_types
    yaml_template["workflow"][0]["outputs"], yaml_template["I/O"]["output_products"] = gen_payload_outputs(
        product_types,
        temp_s3_path,
    )

    logger.debug("Task build_eopf_triggering_yaml FINISHED")
    return yaml_template


def gen_payload_inputs(cadu_list: list, adgs_list: list):
    """Generate payload inputs for the EOPF triggering YAML file.

    Args:
        cadu_list (list): List of CADU file paths.
        adgs_list (list): List of ADGS file paths.

    Returns:
        tuple ([list, list]): A tuple containing the composer dictionary and the YAML content list.
    """
    input_body = []
    composer = []

    def add_input(file_list, prefix, start_cnt):
        nonlocal composer
        nonlocal input_body

        for cnt, file in enumerate(file_list, start=start_cnt):
            file_id = f"in{cnt}"
            input_id = f"{prefix}{cnt}"
            composer.append({file_id: input_id})
            yaml_template = {"id": input_id, "path": file, "store_type": file.split(".")[-1], "store_params": {}}
            input_body.append(yaml_template)

    add_input(cadu_list, "CADU", 1)
    add_input(adgs_list, "ADGS", len(cadu_list) + 1)

    return composer, input_body


def gen_payload_outputs(product_types, temp_s3_path: str):
    """Generate payload outputs for product types.

    This function generates composer and output_body payloads for a list of product types,
    each associated with a temporary S3 path.

    Args:
        product_types (list): A list of product types.
        temp_s3_path (str): The temporary S3 path where the products will be stored.

    Returns:
        Tuple ([list, list]): A tuple containing composer and output_body payloads.

    """
    composer = []
    output_body = []

    temp_s3_path = temp_s3_path.rstrip("/")
    for typecnt, ptype in enumerate(product_types):
        composer.append({f"out{typecnt}": ptype})
        output_body.append(
            {
                "id": ptype,
                "path": f"{temp_s3_path}/{ptype}/",
                "type": "folder|zip",
                "store_type": "zarr",
                "store_params": {},
            },
        )

    return composer, output_body


def get_yaml_outputs(template: dict):
    """Extract the paths of YAML outputs from the template.

    Args:
        template (dict): The YAML template.

    Returns:
        list (list): A list of paths for YAML outputs.
    """
    return [out["path"] for out in template["I/O"]["output_products"]]


def create_cql2_filter(properties: dict, op: str = "and"):
    """
    Create a CQL2 filter based on provided properties,
    see: https://pystac-client.readthedocs.io/en/stable/tutorials/cql2-filter.html

    Args:
        properties (dict): Dictionary containing field-value pairs for filtering.
        op (str, optional): Logical operator to combine filter conditions. Defaults to "and".

    Returns:
        ret (dict): CQL2 filter.
    """
    args = [{"op": "=", "args": [{"property": field}, value]} for field, value in properties.items()]
    # args.append("collecttion=test_user_s1_chunk")
    # this filter is used for getting the files for one CADIP seesion id
    # There can't be more than 60 files for a cadip session, so a hardcoded limit of 1000 seems
    # to be ok for the time being
    return {"filter-lang": "cql2-json", "limit": "1000", "filter": {"op": op, "args": args}}


@task
def get_cadip_catalog_data(
    stac_client: StacClient,  # NOTE: maybe use RsClientSerialization instead
    collection: str,
    session_id: str,
):
    """Prefect task to retrieve CADIP catalog data for a specific collection and session ID.

    This task retrieves CADIP catalog data by sending a request to the CADIP
    catalog search endpoint with a filter based on the collection and session ID.

    Args:
        stac_client (StacClient): The StacClient instance for accessing the CADIP catalog.
        collection (str): The name of the collection.
        session_id (str): The session ID associated with the collection.

    Returns:
        response (dict): The CADIP catalog data if retrieval is successful, otherwise None.

    """

    logger = stac_client.logger
    logger.debug("Task get_cadip_catalog_data STARTED")

    _filter = create_cql2_filter({"collection": f"{stac_client.owner_id}_{collection}", "cadip:session_id": session_id})

    try:
        search = stac_client.search(filter=_filter)
        data = list(search.items_as_dicts())
        logger.debug("Task get_cadip_catalog_data FINISHED")
        return data
    except NotImplementedError as e:
        logger.exception("Search exception caught: %s", e)
        return None


@task
def get_adgs_catalog_data(
    stac_client: StacClient,  # NOTE: maybe use RsClientSerialization instead
    collection: str,
    files: list,
):
    """Prefect task to retrieve ADGS catalog data for specific files in a collection.

    This task retrieves ADGS catalog data by sending a request to the ADGS
    catalog search endpoint with filters based on the collection and file IDs.

    Args:
        stac_client (StacClient): The StacClient instance for accessing the ADGS catalog.
        collection (str): The name of the collection.
        files (list): A list of file IDs to retrieve from the catalog.

    Returns:
        response (dict): The ADGS catalog data if retrieval is successful, otherwise None.

    """

    logger = stac_client.logger
    logger.debug("Task get_adgs_catalog_data STARTED")

    _filter = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "collection"}, f"{stac_client.owner_id}_{collection}"]},
            {"op": "=", "args": [{"property": "owner"}, stac_client.owner_id]},
            {"op": "in", "args": [{"property": "id"}, files]},
        ],
    }
    try:
        search = stac_client.search(filter=_filter)
        data = list(search.items_as_dicts())
        logger.debug("Task get_adgs_catalog_data FINISHED")
        return data
    except NotImplementedError as e:
        logger.exception("Search exception caught: %s", e)
        return None


class PrefectS1L0FlowConfig:  # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """Configuration for Prefect flow related to S1 Level 0 data."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        stac_client: StacClient,
        url_dpr: str,
        mission: str,
        cadip_session_id: str,
        product_types: list,
        adgs_files: list,
        s3_path: str,
        temp_s3_path: str,
    ):
        """
        Initialize the PrefectS1L0FlowConfig object with provided parameters.

        Args:
            stac_client (StacClient): StacClient instance
            url_dpr (str): The URL of the dpr endpoint
            mission (str): The mission name.
            cadip_session_id (str): The CADIP session ID.
            product_types (list): The list with the products types to be processed by the DPR
            adgs_files (list): ADGS files in the catalog.
            s3_path (str): The S3 path.
            temp_s3_path (str): The temporary S3 path.
        """
        self.stac_client: StacClient | None = None  # don't save this instance
        self.rs_client_serialization = RsClientSerialization(stac_client)  # save the serialization parameters instead
        self.url_dpr = url_dpr
        self.mission = mission
        self.cadip_session_id = cadip_session_id
        self.product_types = product_types
        self.adgs_files = adgs_files
        self.s3_path = s3_path
        self.temp_s3_path = temp_s3_path


# At the time being, no more than 2 workers are required, because this flow runs at most 2 tasks in in parallel
# @flow # TO DEBUG THE CODE, JUST USE @flow
@flow(
    flow_run_name="{config.rs_client_serialization.get_flow_name}",
    task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 2, "threads_per_worker": 1}),
)
def s1_l0_flow(config: PrefectS1L0FlowConfig):  # pylint: disable=too-many-locals
    """Constructs a Prefect Flow for Sentinel-1 Level 0 processing.

    This flow oversees the execution of tasks involved in processing Sentinel-1 Level 0 data. It coordinates sequential
    and parallel tasks, including retrieving catalog data concurrently from CADIP and ADGS stations, generating YAML
    configuration based on the retrieved data, initiating the processing based on the YAML configuration, updating the
    STAC catalog according to processing results, and publishing the processed files contingent upon catalog updates.

    Args:
        config (PrefectS1L0FlowConfig): Configuration for the flow.

    """
    logger = get_prefect_logger(LOGGER_NAME)

    # Deserialize the RsClient instance
    config.stac_client = config.rs_client_serialization.deserialize(logger)  # type: ignore

    # Check the product types
    ok_types = ["S1SEWRAW", "S1SIWRAW", "S1SSMRAW", "S1SWVRAW"]
    for product_type in config.product_types:
        if product_type not in ok_types:
            raise RuntimeError(
                f"Unrecognized product type: {product_type}\n" "It should be one of: \n" + "\n".join(ok_types),
            )

    # TODO: the station for CADIP should come as an input
    cadip_collection = create_collection_name(config.mission, ECadipStation["CADIP"])
    adgs_collection = create_collection_name(config.mission, AUXIP_STATION)
    logger.debug(f"Collections: {cadip_collection} | {adgs_collection}")

    # gather the data for cadip session id
    logger.debug("Starting task get_cadip_catalog_data")
    cadip_catalog_data = get_cadip_catalog_data.submit(
        config.stac_client,  # type: ignore # NOTE: maybe use RsClientSerialization instead
        cadip_collection,
        config.cadip_session_id,
    )
    # logger.debug(f"cadip_catalog_data = {cadip_catalog_data} | {cadip_catalog_data.result()}")
    logger.debug("Starting task get_adgs_catalog_data")
    adgs_catalog_data = get_adgs_catalog_data.submit(
        config.stac_client,  # type: ignore # NOTE: maybe use RsClientSerialization instead
        adgs_collection,
        config.adgs_files,
    )

    # the previous tasks may be launched in parallel. The next task depends on the results from these previous tasks
    if not cadip_catalog_data.result():
        logger.error(f"No Cadip files were found in the catalog for Cadip session ID: {config.cadip_session_id!r}")
        return

    if not adgs_catalog_data.result():
        logger.error(  # pylint: disable=logging-not-lazy
            "None of these Auxip files were found in the catalog:\n" + "\n".join(config.adgs_files),
        )
        return

    logger.debug("Starting task build_eopf_triggering_yaml ")
    yaml_dpr_input = build_eopf_triggering_yaml.submit(
        cadip_catalog_data.result(),
        adgs_catalog_data.result(),
        config.product_types,
        config.temp_s3_path,
        wait_for=[cadip_catalog_data, adgs_catalog_data],
    )
    # the output product list should be :S1SEWRAW S1SIWRAW S1SSMRAW S1SWVRAW
    # according to the jira story

    # this task depends on the result from the previous task
    logger.debug("Starting task start_dpr")
    files_stac = start_dpr.submit(config.url_dpr, yaml_dpr_input.result(), wait_for=[yaml_dpr_input])

    if not files_stac.result():
        logger.error("DPR did not processed anything")
        return

    # The final collection where the processed products will be stored is assumed to be named "mission_dpr".
    # However, it might be necessary to allow the users to input a specific collection name
    # if they wish to do so. There is no established guide in the US for this matter.
    collection_name = f"{config.mission}_dpr"
    now = datetime.now()
    config.stac_client.add_collection(
        Collection(
            id=collection_name,
            description=None,  # rs-client will provide a default description for us
            extent=Extent(
                spatial=SpatialExtent(bboxes=[-180.0, -90.0, 180.0, 90.0]),
                temporal=TemporalExtent([now, now]),
            ),
        ),
        timeout=CATALOG_REQUEST_TIMEOUT,
    )

    fin_res = []
    for output_product in get_yaml_outputs(yaml_dpr_input.result()):
        matching_stac = next(
            (d for d in files_stac.result() if d["stac_discovery"]["properties"]["eopf:type"] in output_product),
            None,
        )

        matching_stac["stac_discovery"]["assets"] = {"file": {"href": ""}}

        # Update catalog (it moves the products from temporary bucket to the final one)
        logger.info("Starting task update_stac_catalog")
        fin_res.append(
            (
                update_stac_catalog.submit(
                    config.stac_client,  # NOTE: maybe use RsClientSerialization instead
                    collection_name,
                    matching_stac["stac_discovery"],
                    output_product,
                    wait_for=[files_stac],
                ),
                output_product,
            ),
        )
    logger.debug("All tasks submitted")
    for tsk in fin_res:
        if tsk[0].result():
            logger.info(f"File well published: {tsk[1]}")
        else:
            logger.error(f"File could not be published: {tsk[1]}")

    logger.info("S1 L0 prefect flow finished")
