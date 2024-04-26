"""Prefect flow for processing a S1 L0 product"""
import os.path as osp
import pprint
from pathlib import Path

import requests
import yaml
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from starlette.status import HTTP_200_OK

from rs_workflows.common import (
    ADGS,
    CADIP,
    CATALOG_REQUEST_TIMEOUT,
    create_apikey_headers,
    create_collection_name,
    get_general_logger,
    get_prefect_logger,
    update_stac_catalog,
)

CONFIG_DIR = Path(osp.realpath(osp.dirname(__file__))) / "config"
YAML_TEMPLATE_FILE = "dpr_config_template.yaml"
LOGGER_NAME = "s1_l0_wf"
DPR_PROCESSING_TIMEOUT = 14400  # 4 hours


@task
def start_dpr(dpr_endpoint, yaml_dpr_input: dict):
    """Starts the DPR processing with the given YAML input.

    Args:
        yaml_dpr_input (dict): The YAML input for DPR processing.

    Returns:
        dict: The response JSON from the DPR simulator if successful, else None.

    Raises:
        None
    """
    logger = get_general_logger(LOGGER_NAME)
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
    pp = pprint.PrettyPrinter(indent=4)
    for attr in response.json():
        pp.pprint(attr)
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
        dict: The generated YAML template with updated inputs and I/O products, or None if unsuccessful.

    Raises:
        None
    """
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
    except IOError as e:
        logger.exception(f"Could not find the YAML template file: {e}")
        return None

    # Extract paths for CADIP and ADGS files
    cadip_paths = [file_prop["assets"]["file"]["alternate"]["s3"]["href"] for file_prop in cadip_files["features"]]
    adgs_paths = [file_prop["assets"]["file"]["alternate"]["s3"]["href"] for file_prop in adgs_files["features"]]
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
    """Generates payload inputs for the EOPF triggering YAML file.

    Args:
        cadu_list (list): List of CADU file paths.
        adgs_list (list): List of ADGS file paths.

    Returns:
        tuple: A tuple containing the composer dictionary and the YAML content list.

    Raises:
        None
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
    """Generates payload outputs for the EOPF triggering YAML file.

    Args:
        temp_s3_path (str): The temporary S3 path.

    Returns:
        dict: The updated YAML template.

    Raises:
        None
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
    """Extracts the paths of YAML outputs from the template.

    Args:
        template (dict): The YAML template.

    Returns:
        list: A list of paths for YAML outputs.

    Raises:
        None
    """
    return [out["path"] for out in template["I/O"]["output_products"]]


def create_cql2_filter(properties: dict, op: str = "and"):
    """
    Create a CQL2 filter based on provided properties.

    Args:
        properties (dict): Dictionary containing field-value pairs for filtering.
        op (str, optional): Logical operator to combine filter conditions. Defaults to "and".

    Returns:
        dict: CQL2 filter.
    """
    args = [{"op": "=", "args": [{"property": field}, value]} for field, value in properties.items()]
    # args.append("collecttion=test_user_s1_chunk")
    return {"filter-lang": "cql2-json", "limit": "1000", "filter": {"op": op, "args": args}}


@task
def get_cadip_catalog_data(url_catalog: str, username: str, collection: str, session_id: str, apikey: str):
    """Task to retrieve catalog data from CADIP.

    Args:
        url_catalog (str): URL of the catalog.
        username (str): User name.
        collection (str): Collection name.
        session_id (str): Session ID.
        apikey (str): API key.

    Returns:
        dict: Catalog data from CADIP.
    """
    logger = get_general_logger(LOGGER_NAME)
    logger.debug("Task get_cadip_catalog_data STARTED")
    catalog_endpoint = url_catalog.rstrip("/") + "/catalog/search"

    query = create_cql2_filter({"collection": f"{username}_{collection}", "cadip:session_id": session_id})
    # logger.debug(f"{url_catalog} | {username} | {collection} | {session_id} | {apikey}")
    try:
        response = requests.post(
            catalog_endpoint,
            json=query,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **create_apikey_headers(apikey),
        )
        # logger.debug(f"stat = {response.status_code}")
        # logger.debug(f"json = {response.json()}")
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Request exception caught: %s", e)
        return None

    if int(response.status_code) != HTTP_200_OK:
        logger.error(f"The request response failed: {response.status_code}")
        return None

    try:
        logger.debug("Task get_cadip_catalog_data FINISHED")
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None


@task
def get_adgs_catalog_data(url_catalog: str, username: str, collection: str, files: list, apikey: str):
    """Task to retrieve catalog data from ADGS

    Args:
        url_catalog (str): The URL of the catalog.
        username (str): The username.
        collection (str): The collection name.
        files (list): A list of file IDs.
        apikey (str): The API key.

    Returns:
        dict or None: The catalog data in JSON format if successful, None otherwise.

    Raises:
        None
    """
    logger = get_prefect_logger(LOGGER_NAME)
    logger.debug("Task get_adgs_catalog_data STARTED")
    catalog_endpoint = url_catalog.rstrip("/") + "/catalog/search"

    payload = {"collection": f"{username}_{collection}", "ids": ",".join(files), "filter": f"owner_id='{username}'"}

    logger.debug(f"payload = {payload}")
    try:
        response = requests.get(
            catalog_endpoint,
            params=payload,
            timeout=CATALOG_REQUEST_TIMEOUT,
            **create_apikey_headers(apikey),
        )

    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        logger.exception("Request exception caught: %s", e)
        return None

    if int(response.status_code) != HTTP_200_OK:
        logger.error(f"The request response failed: {response}")
        return None
    try:
        logger.debug("Task get_adgs_catalog_data FINISHED")
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None


class PrefectS1L0FlowConfig:  # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """Configuration for Prefect flow related to S1 Level 0 data."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user: str,
        url_catalog: str,
        url_dpr: str,
        mission: str,
        cadip_session_id: str,
        product_types: list,
        s3_path: str,
        temp_s3_path: str,
        apikey: str,
    ):
        """
        Initialize the PrefectS1L0FlowConfig object with provided parameters.

        Args:
            user (str): The username.
            url_catalog (str): The URL of the catalog.
            url_dpr (str): The URL of the dpr endpoint
            mission (str): The mission name.
            cadip_session_id (str): The CADIP session ID.
            product_types (list): The list with the products types to be processed by the DPR
            s3_path (str): The S3 path.
            temp_s3_path (str): The temporary S3 path.
            apikey (str): The API key.
        """
        self.user = user
        self.url_catalog = url_catalog
        self.url_dpr = url_dpr
        self.mission = mission
        self.cadip_session_id = cadip_session_id
        self.product_types = product_types
        self.s3_path = s3_path
        self.temp_s3_path = temp_s3_path
        self.apikey = apikey


@flow(task_runner=DaskTaskRunner())
def s1_l0_flow(config: PrefectS1L0FlowConfig):
    """
    Prefect flow for S1 Level 0 data processing.

    Args:
        config (PrefectS1L0FlowConfig): Configuration for the flow.

    Returns:
        None: If no data is found in the catalog.
    """
    logger = get_prefect_logger(LOGGER_NAME)

    cadip_collection = create_collection_name(config.mission, CADIP[0])
    adgs_collection = create_collection_name(config.mission, ADGS)
    logger.debug(f"Collections: {cadip_collection} | {adgs_collection}")
    # S1A_20200105072204051312
    # gather the data for cadip session id
    logger.debug("Starting task get_cadip_catalog_data")
    cadip_catalog_data = get_cadip_catalog_data.submit(
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
    adgs_catalog_data = get_adgs_catalog_data.submit(
        config.url_catalog,
        config.user,
        adgs_collection,
        adgs_files,
        config.apikey,
    )
    # the previous tasks may be launched in parallel. The next task depends on the results from these previous tasks
    if (
        not cadip_catalog_data.result()
        or not adgs_catalog_data.result()
        or int(cadip_catalog_data.result()["context"]["returned"]) == 0
        or int(adgs_catalog_data.result()["context"]["returned"]) == 0
    ):
        logger.error("No data found in catalog")
        return

    logger.debug("Starting task build_eopf_triggering_yaml ")
    yaml_dpr_input = build_eopf_triggering_yaml(
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
    files_stac = start_dpr.submit(config.url_dpr, yaml_dpr_input, wait_for=[yaml_dpr_input])

    if not files_stac.result():
        logger.error("DPR did not processed anything")
        return

    # Temp, to be fixed
    collection_name = f"{config.mission}_dpr"
    minimal_collection = {
        "id": collection_name,
        "type": "Collection",
        "description": "test_description",
        "stac_version": "1.0.0",
        "owner": config.user,
    }
    logger.debug(f"Creating collection for the DPR products: {collection_name}")
    requests.post(
        f"{config.url_catalog}/catalog/collections",
        json=minimal_collection,
        timeout=CATALOG_REQUEST_TIMEOUT,
        **create_apikey_headers(config.apikey),
    )
    fin_res = []
    for output_product in get_yaml_outputs(yaml_dpr_input):
        matching_stac = next(
            (d for d in files_stac.result() if d["stac_discovery"]["properties"]["eopf:type"] in output_product),
            None,
        )
        # To be removed, temp fix
        matching_stac["stac_discovery"]["assets"] = {"file": {"href": ""}}

        # Update catalog (it moves the products from temporary bucket to the final one)
        logger.info("Starting task update_stac_catalog")
        fin_res.append(
            (
                update_stac_catalog.submit(
                    create_apikey_headers(config.apikey),
                    config.url_catalog,
                    config.user,
                    collection_name,
                    matching_stac["stac_discovery"],
                    output_product,
                    logger,
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
