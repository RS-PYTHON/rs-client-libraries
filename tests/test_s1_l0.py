"""Demo file to show how the prefect flow s1_l0_flow may be used"""
import argparse
import os

from rs_workflows.common import get_general_logger
from rs_workflows.s1_l0 import LOGGER_NAME, PrefectS1L0FlowConfig, s1_l0_flow

if __name__ == "__main__":
    # This script initiates the processing of Sentinel-1 Level 0 products using the Prefect flow."""

    # It requires the CADIP session ID, RS-Server catalog URL, user name, mission name, S3 storage paths,
    # and optionally an API key (when this is run on the cluster).

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

    parser.add_argument("-d", "--url-dpr", type=str, required=True, help="Url of the DPR endpoint")

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
            args.url_dpr,
            args.mission,
            args.session_id,
            args.s3_storage,
            args.temp_s3_storage,
            args.apikey,
        ),
    )
