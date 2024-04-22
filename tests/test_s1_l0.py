import argparse
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime

import requests

from rs_workflows.common import (
    ADGS,
    CADIP,
    create_collection_name,
    get_general_logger,
    update_stac_catalog,
)
from rs_workflows.s1_l0 import (
    LOGGER_NAME,
    PrefectS1L0FlowConfig,
    build_eopf_triggering_yaml,
    get_adgs_catalog_data,
    get_cadip_catalog_data,
    s1_l0_flow,
    start_dpr,
)

# RSPY_APIKEY="RSPY_APIKEY"
# import boto3
# import os
# s3_session = boto3.session.Session()
# s3_client = s3_session.client(
#     service_name="s3",
#     aws_access_key_id=os.environ["S3_ACCESSKEY"],
#     aws_secret_access_key=os.environ["S3_SECRETKEY"],
#     endpoint_url=os.environ["S3_ENDPOINT"],
#     region_name=os.environ["S3_REGION"],
# )

# buckets = ["rs-cluster-temp", "rs-cluster-catalog"] # bucket names under S3_ENDPOINT
# bucket_dir = "stations"
# bucket_url = f"s3://{buckets[0]}/{bucket_dir}"


# # If the bucket is already created, clear all files to start fresh for each demo.
# for b in buckets:
#     if b in [bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]]:
#         if 'Contents' in s3_client.list_objects(Bucket=b):
#             objects = s3_client.list_objects(Bucket=b)['Contents']
#             for obj in objects:
#                 # clear up the bucket
#                 s3_client.delete_object(Bucket=b, Key=obj['Key'])
#     else:
#         s3_client.create_bucket(Bucket=b)
# import pdb
# pdb.set_trace()
# for b in buckets:
#     print(f"Is {b} empty ?: ", 'Contents' not in s3_client.list_objects(Bucket=b))

"# Clean previous executions\n",
# requests.delete(f"{url_catalog}/catalog/collections/DemoUser:s1_aux", **HEADERS)


@dataclass
class Collection:
    """A collection for test purpose."""

    user: str
    name: str

    @property
    def id_(self) -> str:
        """Returns the id."""
        return f"{self.user}_{self.name}"

    @property
    def properties(self):
        """Returns the properties."""
        return {
            "id": self.name,
            "type": "Collection",
            "links": [
                {
                    "rel": "items",
                    "type": "application/geo+json",
                    "href": f"http://localhost:8082/collections/{self.name}/items",
                },
                {"rel": "parent", "type": "application/json", "href": "http://localhost:8082/"},
                {"rel": "root", "type": "application/json", "href": "http://localhost:8082/"},
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"""http://localhost:8082/collections/{self.name}""",
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/publicdomain/",
                    "title": "public domain",
                },
            ],
            "extent": {
                "spatial": {"bbox": [[-94.6911621, 37.0332547, -94.402771, 37.1077651]]},
                "temporal": {"interval": [["2000-02-01T00:00:00Z", "2000-02-12T00:00:00Z"]]},
            },
            "license": "public-domain",
            "description": "Some description",
            "stac_version": "1.0.0",
            "owner": self.user,
        }


def create_collection(url_catalog, user, collection_name, apikey_headers):
    catalog_endpoint = url_catalog.rstrip("/") + "/catalog/collections"
    collection_type = Collection(user, collection_name)
    logger.info(f"Endpoint used to insert the item info  within the catalog: {catalog_endpoint}")
    response = requests.post(catalog_endpoint, data=None, json=collection_type.properties, **apikey_headers)
    logger.info("response = {} ".format(response))


if __name__ == "__main__":
    """This is a demo which integrates the search and
    download from a CADIP server. It also checks the download status
    """

    logger = get_general_logger(LOGGER_NAME)

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

    parser.add_argument("-u", "--user", type=str, required=True, help="User name")

    parser.add_argument("-m", "--mission", type=str, required=True, help="Mission name")

    # parser.add_argument(
    #     "-t",
    #     "--max-tasks",
    #     type=int,
    #     required=False,
    #     help="Maximum number of prefect tasks. Default 1 (the prefect flow will not be started)",
    #     default=1,
    # )

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
            args.mission,
            args.session_id,
            args.s3_storage,
            args.temp_s3_storage,
            args.apikey,
        ),
    )

    # apikey_headers = {}
    # if not args.apikey:
    #     args.apikey = os.environ.get("RSPY_APIKEY", None)
    # if args.apikey:
    #     apikey_headers = {"headers": {"x-api-key": args.apikey}}

    # cadip_collection = create_collection_name(args.mission, CADIP[0])
    # adgs_collection = create_collection_name(args.mission, ADGS)

    # # S1A_20200105072204051312
    # # gather the data for cadip session id
    # cadip_catalog_data = get_cadip_catalog_data(
    #     args.url_catalog,
    #     args.user,
    #     cadip_collection,
    #     args.session_id,
    #     args.apikey,
    # )

    # adgs_files = [
    #     "S1A_AUX_PP2_V20200106T080000_G20200106T080000.SAFE",
    #     "S1A_OPER_MPL_ORBPRE_20200409T021411_20200416T021411_0001.EOF",
    #     "S1A_OPER_AUX_RESORB_OPOD_20210716T110702_V20210716T071044_20210716T102814.EOF",
    # ]
    # adgs_catalog_data = get_adgs_catalog_data(args.url_catalog, args.user, adgs_collection, adgs_files, args.apikey)
    # yaml_dpr_input = build_eopf_triggering_yaml(cadip_catalog_data, adgs_catalog_data)
    # files_stac = start_dpr(yaml_dpr_input)

    # if not files_stac:
    #     logger.error("DPR did not processed anything")
    #     sys.exit(-1)

    # collection_name = f"{args.user}_dpr_{datetime.now().strftime('%Y%m%dH%M%S')}"
    # # for file_stac_info in files_stac:
    # #     obs = f"{args.s3_storage.rstrip('/')}/{file_stac_info['id']}"
    # #     update_stac_catalog(args.apikey, args.url_catalog, args.user, collection_name, file_stac_info, obs, logger)
