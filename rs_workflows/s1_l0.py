"""Docstring will be here."""
import enum
import logging
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
    create_apikey_headers,
    get_general_logger,
)

CONFIG_DIR = Path(osp.realpath(osp.dirname(__file__))) / "config"
YAML_TEMPLATE_FILE = "dpr_config_template.yaml"
LOGGER_NAME = "s1_l0_wf"


def start_dpr(yaml_dpr_input):
    logger = get_general_logger(LOGGER_NAME)
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

    return response.json()


def build_eopf_triggering_yaml(cadip_files, adgs_files):
    logger = get_general_logger(LOGGER_NAME)
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


# def gen_payload_inputs(cadu_list, adgs_list):
#     composer = []
#     yaml_content = []

#     def generate_inputs(file_list, prefix, start_id):
#         nonlocal composer
#         nonlocal yaml_content

#         for input_cnt, file in enumerate(file_list):
#             file_id = f'in{input_cnt}'
#             input_id = f"{prefix}{start_id + input_cnt}"
#             composer.append({file_id: input_id})
#             yaml_template = {"id": input_id, "path": file, "store_type": file.split(".")[-1], "store_params": {}}
#             yaml_content.append(yaml_template)

#     generate_inputs(cadu_list, "CADU", 0)
#     generate_inputs(adgs_list, "ADGS", len(cadu_list))

#     return composer, yaml_content


def create_cql2_filter(properties: dict, op="and"):
    args = [{"op": "=", "args": [{"property": field}, value]} for field, value in properties.items()]
    # args.append("collecttion=test_user_s1_chunk")
    return {"filter-lang": "cql2-json", "filter": {"op": op, "args": args}}


def get_cadip_catalog_data(url_catalog, username, collection, session_id, apikey):
    logger = get_general_logger(LOGGER_NAME)

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
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None


def get_adgs_catalog_data(url_catalog, username, collection, files, apikey):
    logger = get_general_logger(LOGGER_NAME)
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
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None
