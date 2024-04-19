"""Docstring will be here."""
import logging
import os
import os.path as osp
import pprint

# import pprint
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, List

import numpy as np
import requests
import yaml
from prefect import exceptions, flow, get_run_logger, task
from prefect_dask.task_runners import DaskTaskRunner

from rs_workflows.common import (
    ADGS,
    CADIP,
    CATALOG_REQUEST_TIMEOUT,
    SET_PREFECT_LOGGING_LEVEL,
    create_apikey_headers,
    create_collection_name,
    get_general_logger,
    get_prefect_logger,
    update_stac_catalog,
)

CONFIG_DIR = Path(osp.realpath(osp.dirname(__file__))) / "config"
YAML_TEMPLATE_FILE = "dpr_config_template.yaml"
LOGGER_NAME = "s1_l0_wf"


@task
def start_dpr(yaml_dpr_input):
    logger = get_general_logger(LOGGER_NAME)
    logger.debug("Task start_dpr STARTED")
    logger.info("Faking dpr processing with the following input file:")
    logger.info(yaml.dump(yaml_dpr_input))
    dpr_simulator_endpoint = "http://127.0.0.1:6002/run"  # rs-server host = the container name
    try:
        # import pdb
        # pdb.set_trace()
        response = requests.post(dpr_simulator_endpoint, json=yaml.safe_load(yaml.dump(yaml_dpr_input)))
        logger.debug(f"stat = {response.status_code}")
        logger.debug(f"json = {response.json()}")
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Calling the dpr simulater resulted in exception: %s", e)
        return None

    if response.status_code != 200:
        logger.error(f"The request response failed: {response}")
        return None

    pp = pprint.PrettyPrinter(indent=4)
    for attr in response.json():
        pp.pprint(attr)
    logger.debug("Task start_dpr ENDED")
    return response.json()


@task
def build_eopf_triggering_yaml(cadip_files, adgs_files):
    logger = get_general_logger(LOGGER_NAME)
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

    # Extract paths for CADIP and ADGS files
    cadip_paths = [file_prop["assets"]["file"]["alternate"]["s3"]["href"] for file_prop in cadip_files["features"]]
    adgs_paths = [file_prop["assets"]["file"]["alternate"]["s3"]["href"] for file_prop in adgs_files["features"]]
    # create the dictionaries to insert within the yaml template
    yaml_inputs, yaml_io_products = gen_payload_inputs(cadip_paths, adgs_paths)

    # Update YAML template with inputs and I/O products
    yaml_template["workflow"][0]["inputs"] = yaml_inputs
    yaml_template["I/O"]["inputs_products"] = yaml_io_products
    logger.debug("Task build_eopf_triggering_yaml ENDED")
    yaml_template = gen_payload_outputs(yaml_template)
    return yaml_template


def gen_payload_inputs(cadu_list, adgs_list):
    yaml_content = []
    composer = []

    def add_input(file_list, prefix, start_cnt):
        nonlocal composer
        nonlocal yaml_content

        for cnt, file in enumerate(file_list, start=start_cnt):
            file_id = f"in{cnt}"
            input_id = f"{prefix}{cnt}"
            composer.append({file_id: input_id})
            yaml_template = {"id": input_id, "path": file, "store_type": file.split(".")[-1], "store_params": {}}
            yaml_content.append(yaml_template)

    add_input(cadu_list, "CADU", 1)
    add_input(adgs_list, "ADGS", len(cadu_list) + 1)

    return composer, yaml_content


def gen_payload_outputs(template):
    composer = []
    output_body = []
    for typecnt, ptype in enumerate(template['workflow'][0]['parameters']['product_types']):
        composer.append({f"out{typecnt}": ptype})
        output_body.append({"id": ptype,
                            "path": f"s3://rs-cluster-temp/zarr/dpr_processor_output/{ptype}/",
                            "type": "folder|zip",
                            "store_type": "zarr",
                            "store_params": {}})
    template['workflow'][0]['outputs'] = composer
    template['I/O']['output_products'] = output_body
    return template


def get_yaml_outputs(template):
    return [out['path'] for out in template['I/O']['output_products']]

def create_cql2_filter(properties: dict, op="and"):
    args = [{"op": "=", "args": [{"property": field}, value]} for field, value in properties.items()]
    # args.append("collecttion=test_user_s1_chunk")
    return {"filter-lang": "cql2-json", "filter": {"op": op, "args": args}}


@task
def get_cadip_catalog_data(url_catalog, username, collection, session_id, apikey):
    logger = get_general_logger(LOGGER_NAME)
    logger.debug("Task get_cadip_catalog_data STARTED")
    catalog_endpoint = url_catalog.rstrip("/") + "/catalog/search"

    query = create_cql2_filter({"collection": f"{username}_{collection}", "cadip:session_id": session_id})
    logger.debug(f"{url_catalog} | {username} | {collection} | {session_id} | {apikey}")
    try:
        response = requests.post(
            catalog_endpoint,
            json=query,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **create_apikey_headers(apikey),
        )
        logger.debug(f"stat = {response.status_code}")
        logger.debug(f"json = {response.json()}")
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Request exception caught: %s", e)
        return None

    # response = requests.get(catalog_endpoint, \
    # params=payload, timeout=ENDPOINT_TIMEOUT, **create_apikey_headers(apikey))

    if response.status_code != 200:
        logger.error(f"The request response failed: {response}")
        return None

    try:
        logger.debug("Task get_cadip_catalog_data ENDED")
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None


@task
def get_adgs_catalog_data(url_catalog, username, collection, files, apikey):
    logger = get_prefect_logger(LOGGER_NAME)
    logger.debug("Task get_adgs_catalog_data STARTED")
    catalog_endpoint = url_catalog.rstrip("/") + "/catalog/search"

    payload = {"collection": f"{username}_{collection}", "ids": ",".join(files), "filter": f"owner_id='{username}'"}

    #     payload = {
    #         "collections": f"{username}_{collection}",
    #         "ids": "S1A_OPER_AUX_OBMEMC_PDMC_20140201T000000.xml,\
    # S1A_OPER_MPL_ORBPRE_20240113T021411_20240120T021411_0001.EOF,\
    # S1A_OPER_MPL_ORBSCT_20140507T150704_99999999T999999_0025.EOF"
    #     }
    logger.debug(f"payload = {payload}")
    try:
        response = requests.get(
            catalog_endpoint,
            params=payload,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **create_apikey_headers(apikey),
        )
        # response = requests.post(catalog_endpoint,
        # json=query, timeout=ENDPOINT_TIMEOUT, **create_apikey_headers(apikey))
        logger.debug(f"json = {response.json()}")
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Request exception caught: %s", e)
        return None

    if response.status_code != 200:
        logger.error(f"The request response failed: {response}")
        return None
    try:
        logger.debug("Task get_adgs_catalog_data ENDED")
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None


class PrefectS1L0FlowConfig:  # pylint: disable=too-few-public-methods
    """Configuration class for Prefect flow.

    This class inherits the PrefectCommonCongig and represents the configuration for a
    Prefect flow

    Attributes:
        max_workers (int): The maximum number of workers for the Prefect flow.
        start_datetime (datetime): The start datetime of the files that the station should return
        stop_datetime (datetime): The stop datetime of the files that the station should return
    """

    def __init__(  # pylint: disable=too-many-arguments
            self,
            user,
            url_catalog,
            mission,
            cadip_session_id,
            s3_path,
            apikey,
            max_workers,
    ):
        """
        Initialize the PrefectFlowConfig object with provided parameters.
        """
        self.user = user
        self.url_catalog = url_catalog
        self.mission = mission
        self.s3_path = s3_path
        self.apikey = apikey
        self.cadip_session_id = cadip_session_id
        self.max_workers: int = max_workers


@flow(task_runner=DaskTaskRunner())
def s1_l0_flow(config: PrefectS1L0FlowConfig):
    logger = get_prefect_logger(LOGGER_NAME)

    cadip_collection = create_collection_name(config.mission, CADIP[0])
    adgs_collection = create_collection_name(config.mission, ADGS)
    logger.debug(f"Collections: {cadip_collection} | {adgs_collection}")
    # S1A_20200105072204051312
    # gather the data for cadip session id
    logger.debug("Starting task get_cadip_catalog_data")
    cadip_catalog_data = get_cadip_catalog_data(
        config.url_catalog,
        config.user,
        cadip_collection,
        config.cadip_session_id,
        config.apikey,
    )
    logger.debug("Starting task get_adgs_catalog_data")
    # gather the data from ADGS. Excerpt from the RSPY-120 user story:
    # "3. search in the STAC collection rs-ops/s1_aux for the three required AUX
    # items (given that RSPY-115 has filled the collection). As we don't know yet the
    # rules that will be specified by DPR to retrieve the correct AUX data, we will for
    # now hardcode the AUX file names as below:
    adgs_files = [
        "S1A_AUX_PP2_V20200106T080000_G20200106T080000.SAFE",
        "S1A_OPER_MPL_ORBPRE_20200409T021411_20200416T021411_0001.EOF",
        "S1A_OPER_AUX_RESORB_OPOD_20210716T110702_V20210716T071044_20210716T102814.EOF",
    ]
    adgs_catalog_data = get_adgs_catalog_data(
        config.url_catalog, config.user, adgs_collection, adgs_files, config.apikey,
    )
    # the previous tasks may be launched in parallel. The next task depends on the results from the previous tasks
    logger.debug("Starting build_eopf_triggering_yaml get_adgs_catalog_data")
    yaml_dpr_input = build_eopf_triggering_yaml(
        cadip_catalog_data, adgs_catalog_data, wait_for=[cadip_catalog_data, adgs_catalog_data],
    )
    # this task depends on the result from the previous task
    logger.debug("Starting start_dpr get_adgs_catalog_data")
    files_stac = start_dpr(yaml_dpr_input, wait_for=[yaml_dpr_input])

    if not files_stac:
        logger.error("DPR did not processed anything")
        sys.exit(-1)

    # Temp, to be fixed
    collection_name = f"{config.user}_dpr"
    minimal_collection = {
        "id": collection_name,
        "type": "Collection",
        "description": "test_description",
        "stac_version": "1.0.0",
        "owner": config.user,
    }
    requests.post(f"{config.url_catalog}/catalog/collections", json=minimal_collection)

    for output_product in get_yaml_outputs(yaml_dpr_input):
        matching_stac = next((d for d in files_stac if d['stac_discovery']['properties']['eopf:type'] in output_product), None)
        # To be removed, temp fix
        matching_stac['stac_discovery']['assets'] = {"file": {"href": ""}}
        config.apikey = {}
        obs = f"s3://rs-cluster-temp/zarr/dpr_processor_output/S1SEWRAW"
        #
        update_stac_catalog(config.apikey, config.url_catalog, config.user, collection_name, matching_stac['stac_discovery'], obs,
                            logger)

    # collection_name = f"{config.user}_dpr_{datetime.now().strftime('%Y%m%dH%M%S')}"
    # for file_stac_info in files_stac:
    #     obs = f"{config.s3_path.rstrip('/')}/{file_stac_info['id']}"
    #     update_stac_catalog(config.apikey, config.url_catalog, config.user, collection_name, file_stac_info, obs, logger)
    logger.info("Finished !")
