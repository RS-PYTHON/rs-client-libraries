"""Demo file to show how the prefect flow s1_l0_flow may be used"""
import argparse
import json
import os
import os.path as osp
from pathlib import Path

import pytest
import responses
import yaml

from rs_common.logging import Logging
from rs_workflows.s1_l0 import (  # CONFIG_DIR,; YAML_TEMPLATE_FILE,
    LOGGER_NAME,
    PrefectS1L0FlowConfig,
    build_eopf_triggering_yaml,
    create_cql2_filter,
    gen_payload_inputs,
    gen_payload_outputs,
    get_adgs_catalog_data,
    get_cadip_catalog_data,
    get_yaml_outputs,
    s1_l0_flow,
    start_dpr,
)

# from prefect.testing.utilities import prefect_test_harness


RESOURCES = Path(osp.realpath(osp.dirname(__file__))) / "resources"


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "endpoint, status",
    [
        ("http://127.0.0.1:5000", "200"),
        ("http://127.0.0.1:5000", "404"),
        ("http://bad_endpoint", "None"),
    ],
)
def test_start_dpr(endpoint, status):
    """Test the start_dpr function.

    This test function checks the behavior of the start_dpr function under various scenarios,
    including successful and failed requests

    Args:
        endpoint (str): The endpoint URL to test.
        status (str): The expected HTTP status code or "None" if an endpoint is expected to fail.

    """
    dpr_answer_path = RESOURCES / "dpr_answer.json"
    with open(dpr_answer_path, encoding="utf-8") as dpr_answer_f:
        dpr_answer = json.loads(dpr_answer_f.read())
    if "bad_endpoint" not in endpoint:
        responses.add(
            responses.POST,
            endpoint + "/run",
            json=dpr_answer,
            status=status,
        )
    yaml_input_path = RESOURCES / "dpr_config_test.yaml"
    # build the yaml config file
    with open(yaml_input_path, encoding="utf-8") as yaml_file:
        yaml_input = yaml.safe_load(yaml_file)

    if "bad_endpoint" not in endpoint:
        dpr_resp = start_dpr.fn(endpoint, yaml_input)
        if int(status) == 200:
            assert (
                dpr_resp[0]["stac_discovery"]["id"]
                == "S01SIWSLC_20231201T170634_0067_A117_S000_5464A_VH_IW1_7F0.zarr.zip"
            )
        else:
            assert dpr_resp is None
    else:
        assert start_dpr.fn(endpoint, yaml_input) is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "cadip_files, , adgs_files, product_types, temp_s3_path",
    [
        (
            {
                "type": "FeatureCollection",
                "context": {"limit": 1000, "returned": 60},
                "features": [
                    {
                        "id": "CADU.raw",
                        "assets": {
                            "file": {
                                "alternate": {"s3": {"href": "s3://test-bucket/CADU.raw"}},
                            },
                        },
                    },
                ],
            },
            {
                "type": "FeatureCollection",
                "context": {"limit": 1000, "returned": 60},
                "features": [
                    {
                        "id": "AUX.EOF",
                        "assets": {
                            "file": {
                                "alternate": {"s3": {"href": "s3://test-bucket/AUX.EOF"}},
                            },
                        },
                    },
                ],
            },
            ["S1SEWRAW"],
            "s3://test-bucket/PRODUCTS/",
        ),
    ],
)
def test_build_eopf_triggering_yaml(cadip_files, adgs_files, product_types, temp_s3_path, mocker):
    """Test the build_eopf_triggering_yaml function.

    This test function checks the behavior of the build_eopf_triggering_yaml function
    under different scenarios, including successful YAML generation and error handling.

    Args:
        cadip_files (dict): CADIP files information.
        adgs_files (dict): ADGS files information.
        product_types (list): List of product types.
        temp_s3_path (str): Temporary S3 path.

    """
    generated_yaml = build_eopf_triggering_yaml.fn(cadip_files, adgs_files, product_types, temp_s3_path)

    yaml_input_path = RESOURCES / "dpr_config_test.yaml"
    with open(yaml_input_path, encoding="utf-8") as yaml_file:
        assert generated_yaml == yaml.safe_load(yaml_file)

    mocker.patch("builtins.open", side_effect=FileNotFoundError)
    assert build_eopf_triggering_yaml.fn(cadip_files, adgs_files, product_types, temp_s3_path) is None

    mocker.patch("builtins.open", side_effect=IOError)
    assert build_eopf_triggering_yaml.fn(cadip_files, adgs_files, product_types, temp_s3_path) is None

    mocker.patch("builtins.open", side_effect=yaml.YAMLError)
    assert build_eopf_triggering_yaml.fn(cadip_files, adgs_files, product_types, temp_s3_path) is None


@pytest.mark.unit
def test_gen_payload_inputs():
    """Test the gen_payload_inputs function.

    This test function checks the behavior of the gen_payload_inputs function
    by comparing the generated composer and input_body lists with the expected values.

    The function is tested with two CADU files and two ADGS files in the input lists.
    It verifies that the composer list contains the correct inputs for the files
    and that the input_body list is correctly constructed with the file details.

    """
    cadu_list = ["s3://test-bucket/CADU.raw", "s3://test-bucket/CADU2.raw"]
    adgs_list = ["s3://test-bucket/ADGS.eof", "s3://test-bucket/ADGS2.eof"]

    composer = [{"in1": "CADU1"}, {"in2": "CADU2"}, {"in3": "ADGS3"}, {"in4": "ADGS4"}]
    input_body = [
        {"id": "CADU1", "path": "s3://test-bucket/CADU.raw", "store_type": "raw", "store_params": {}},
        {"id": "CADU2", "path": "s3://test-bucket/CADU2.raw", "store_type": "raw", "store_params": {}},
        {"id": "ADGS3", "path": "s3://test-bucket/ADGS.eof", "store_type": "eof", "store_params": {}},
        {"id": "ADGS4", "path": "s3://test-bucket/ADGS2.eof", "store_type": "eof", "store_params": {}},
    ]
    ret_composer, ret_input_body = gen_payload_inputs(cadu_list, adgs_list)

    assert composer == ret_composer
    assert input_body == ret_input_body


@pytest.mark.unit
def test_gen_payload_outputs():
    """Test the gen_payload_outputs function.

    This test function checks the behavior of the gen_payload_outputs function
    by comparing the generated composer and output_body lists with the expected values.

    The function is tested with a list of product types and a temporary S3 path.
    It verifies that the composer list contains the correct outputs for the product types
    and that the output_body list is correctly constructed with the file details.

    """
    product_types = ["S1SEWRAW", "S1SIWRAW", "S1SSMRAW", "S1SWVRAW"]
    composer = [{"out0": "S1SEWRAW"}, {"out1": "S1SIWRAW"}, {"out2": "S1SSMRAW"}, {"out3": "S1SWVRAW"}]
    output_body = [
        {
            "id": "S1SEWRAW",
            "path": "s3://rs-cluster-temp/PRODUCTS/S1SEWRAW/",
            "type": "folder|zip",
            "store_type": "zarr",
            "store_params": {},
        },
        {
            "id": "S1SIWRAW",
            "path": "s3://rs-cluster-temp/PRODUCTS/S1SIWRAW/",
            "type": "folder|zip",
            "store_type": "zarr",
            "store_params": {},
        },
        {
            "id": "S1SSMRAW",
            "path": "s3://rs-cluster-temp/PRODUCTS/S1SSMRAW/",
            "type": "folder|zip",
            "store_type": "zarr",
            "store_params": {},
        },
        {
            "id": "S1SWVRAW",
            "path": "s3://rs-cluster-temp/PRODUCTS/S1SWVRAW/",
            "type": "folder|zip",
            "store_type": "zarr",
            "store_params": {},
        },
    ]

    ret_composer, ret_output_body = gen_payload_outputs(product_types, "s3://rs-cluster-temp/PRODUCTS")
    print(ret_composer)
    print(ret_output_body)
    assert composer == ret_composer
    assert output_body == ret_output_body


@pytest.mark.unit
def test_get_yaml_outputs():
    """Test the get_yaml_outputs function.

    This test function checks the behavior of the get_yaml_outputs function
    by verifying that it correctly extracts output paths from the YAML configuration.

    It loads a YAML configuration from a file and asserts that the output path list
    extracted by the function matches the expected value.

    """
    yaml_input_path = RESOURCES / "dpr_config_test.yaml"
    # build the yaml config file
    with open(yaml_input_path, encoding="utf-8") as yaml_file:
        yaml_input = yaml.safe_load(yaml_file)
    assert ["s3://test-bucket/PRODUCTS/S1SEWRAW/"] == get_yaml_outputs(yaml_input)


@pytest.mark.unit
def test_create_cql2_filter():
    """Test the create_cql2_filter function.

    This test function checks the behavior of the create_cql2_filter function
    by verifying that it correctly generates a CQL2 filter dictionary based on the
    provided username, collection, and CADIP session ID.

    It constructs an expected filter dictionary and compares it with the filter
    dictionary returned by the function to ensure correctness.

    """
    username = "TestUser"
    collection = "s1_test"
    cadip_session_id = "S1A_20200105072204051312"
    expected_filter = {
        "filter-lang": "cql2-json",
        "limit": "1000",
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "collection"}, "TestUser_s1_test"]},
                {"op": "=", "args": [{"property": "cadip:session_id"}, "S1A_20200105072204051312"]},
            ],
        },
    }

    assert expected_filter == create_cql2_filter(
        {"collection": f"{username}_{collection}", "cadip:session_id": cadip_session_id},
    )


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "endpoint, status",
    [
        ("http://127.0.0.1:5000", "200"),
        ("http://127.0.0.1:5000", "404"),
        ("http://bad_endpoint", "None"),
    ],
)
def test_get_cadip_catalog_data(endpoint, status):
    """Test for the get_cadip_catalog_data function.

    This test function mocks API responses and verifies the behavior of the
    get_cadip_catalog_data function under different scenarios.

    Args:
        endpoint (str): The URL of the endpoint to mock API requests.
        status (str): The HTTP status code to mock API responses.

    The function loads an expected CADIP catalog data from a file and mocks
    the API response based on the provided endpoint and status. It then calls
    the get_cadip_catalog_data function with the specified parameters and
    asserts that it returns the expected catalog data when the endpoint responds
    with a 200 status code. Otherwise, it asserts that the function returns None.

    """
    username = "TestUser"
    collection = "s1_test"
    cadip_session_id = "S1A_20200105072204051312"
    apikey = ""
    cadip_catalog = RESOURCES / "cadip_catalog.json"
    with open(cadip_catalog, encoding="utf-8") as cadip_catalog_f:
        cadip_catalog = json.loads(cadip_catalog_f.read())
    if "bad_endpoint" not in endpoint:
        responses.add(
            responses.POST,
            endpoint + "/catalog/search",
            json=cadip_catalog,
            status=status,
        )

    if "bad_endpoint" not in endpoint:
        cadip_res = get_cadip_catalog_data.fn(endpoint, username, collection, cadip_session_id, apikey)
        print(cadip_res)
        if int(status) == 200:
            assert cadip_res == cadip_catalog
        else:
            assert cadip_res is None
    else:
        assert get_cadip_catalog_data.fn(endpoint, username, collection, cadip_session_id, apikey) is None


@pytest.mark.unit
@responses.activate
@pytest.mark.parametrize(
    "endpoint, status",
    [
        ("http://127.0.0.1:5000", "200"),
        ("http://127.0.0.1:5000", "404"),
        ("http://bad_endpoint", "None"),
    ],
)
def test_get_adgs_catalog_data(endpoint, status):
    """Test for the get_adgs_catalog_data function.

    This test function mocks API responses and verifies the behavior of the
    get_adgs_catalog_data function under different scenarios.

    Args:
        endpoint (str): The URL of the endpoint to mock API requests.
        status (str): The HTTP status code to mock API responses.

    The function loads an expected ADGS catalog data from a file and mocks
    the API response based on the provided endpoint and status. It then calls
    the get_adgs_catalog_data function with the specified parameters and
    asserts that it returns the expected catalog data when the endpoint responds
    with a 200 status code. Otherwise, it asserts that the function returns None.

    """
    username = "TestUser"
    collection = "s1_test"
    files_list = ["ADGS1.EOF", "ADGS2.EOF"]
    apikey = ""
    adgs_catalog = RESOURCES / "adgs_catalog.json"
    with open(adgs_catalog, encoding="utf-8") as adgs_catalog_f:
        adgs_catalog = json.loads(adgs_catalog_f.read())
    if "bad_endpoint" not in endpoint:
        responses.add(
            responses.GET,
            endpoint + "/catalog/search",
            json=adgs_catalog,
            status=status,
        )

    if "bad_endpoint" not in endpoint:
        adgs_res = get_adgs_catalog_data.fn(endpoint, username, collection, files_list, apikey)
        print(adgs_res)
        if int(status) == 200:
            assert adgs_res == adgs_catalog
        else:
            assert adgs_res is None
    else:
        assert get_adgs_catalog_data.fn(endpoint, username, collection, files_list, apikey) is None


# TODO: The unit testing for this prefect flow does not work
# We can consider that all the other files / task have been tested (see up)
# @pytest.mark.unit
# @responses.activate
# def test_s1_l0_flow(mocker):
#     username = "TestUser"
#     mission = "s1"
#     cadip_session_id = "S1A_20200105072204051312"
#     product_types = ["S1SEWRAW", "S1SIWRAW"]
#     s3_storage = "s3://test_final"
#     temp_s3_storage = "s3://test_temp"
#     apikey = ""
#     url_gen = "http://127.0.0.1:5000"
#     url_dpr = "http://127.0.0.1:5010"

#     # mock all the prefect tasks
#     cadip_catalog = RESOURCES / "cadip_catalog.json"
#     adgs_catalog = RESOURCES / "adgs_catalog.json"
#     with open(cadip_catalog, encoding="utf-8") as cadip_catalog_f:
#         file_loaded = json.loads(cadip_catalog_f.read())
#     mocker.patch(
#         "rs_workflows.s1_l0.get_cadip_catalog_data",
#         return_value=file_loaded,
#     )
#     with open(adgs_catalog, encoding="utf-8") as adgs_catalog_f:
#         file_loaded = json.loads(adgs_catalog_f.read())
#     mocker.patch(
#         "rs_workflows.s1_l0.get_adgs_catalog_data",
#         return_value=file_loaded,
#     )
#     yaml_input_path = RESOURCES / "dpr_config_test.yaml"
#     with open(yaml_input_path, encoding="utf-8") as yaml_file:
#         file_loaded = yaml.safe_load(yaml_file)
#     mocker.patch(
#         "rs_workflows.s1_l0.get_adgs_catalog_data",
#         return_value=file_loaded,
#     )
#     # TODO: the following mock did not work. I also tried to mock the endpoint,
#     # but inside the prefect task is not seen
#     dpr_answer_path = RESOURCES / "dpr_answer.json"
#     with open(dpr_answer_path, encoding="utf-8") as dpr_answer_f:
#         file_loaded = json.loads(dpr_answer_f.read())
#     mocker.patch(
#         "rs_workflows.s1_l0.start_dpr",
#         return_value=file_loaded,
#     )
#     # responses.add(
#     #         responses.GET,
#     #         url_dpr + "/run",
#     #         json=file_loaded,
#     #         status=200,
#     #     )

#     # mock the endpoint for catalog creation
#     responses.add(
#             responses.POST,
#             url_gen + "/catalog/collections",
#             status=200,
#         )
#     mocker.patch(
#         "rs_workflows.staging.update_stac_catalog",
#         return_value=True,
#     )
#     with prefect_test_harness():
#         s1_l0_flow(
#             PrefectS1L0FlowConfig(
#                 username,
#                 url_gen,
#                 url_dpr,
#                 mission,
#                 cadip_session_id,
#                 product_types,
#                 s3_storage,
#                 temp_s3_storage,
#                 apikey,
#             ),
#         )


if __name__ == "__main__":
    # This script initiates the processing of Sentinel-1 Level 0 products using the Prefect flow."""

    # It requires the CADIP session ID, RS-Server catalog URL, user name, mission name, S3 storage paths,
    # and optionally an API key (when this is run on the cluster).

    logger = Logging.default(LOGGER_NAME)

    parser = argparse.ArgumentParser(
        description="Starts the demo for sprint 1 phase",
    )
    parser.add_argument(
        "-s",
        "--session-id",
        type=str,
        required=True,
        help="The CADIP session id for which the processing is wanted",
    )
    parser.add_argument("-c", "--url-catalog", type=str, required=True, help="Url of the RS-Server catalog")

    parser.add_argument("-d", "--url-dpr", type=str, required=True, help="Url of the DPR endpoint")

    parser.add_argument("-u", "--user", type=str, required=True, help="User name")

    parser.add_argument("-m", "--mission", type=str, required=True, help="Mission name")

    parser.add_argument("-p", "--product-types", nargs="+", help="Set flag", required=True)

    parser.add_argument(
        "-o",
        "--s3-storage",
        type=str,
        required=True,
        help="S3 path on the bucket where the products will be copied",
        default="",
    )

    parser.add_argument(
        "-t",
        "--temp-s3-storage",
        type=str,
        required=True,
        help="S3 path on the bucket where the products will be copied",
        default="",
    )

    parser.add_argument(
        "-k",
        "--apikey",
        type=str,
        required=False,
        help="The apikey to be used in endpoints calling",
        default=None,
    )

    args = parser.parse_args()

    if not args.apikey:
        args.apikey = os.environ.get("RSPY_APIKEY", None)

    s1_l0_flow(
        PrefectS1L0FlowConfig(
            args.user,
            args.url_catalog,
            args.url_dpr,
            args.mission,
            args.session_id,
            args.product_types,
            args.s3_storage,
            args.temp_s3_storage,
            args.apikey,
        ),
    )
