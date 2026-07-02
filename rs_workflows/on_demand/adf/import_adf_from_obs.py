# Copyright 2023-2026 Airbus, CS Group
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

"""Import ADF from an S3 bucket and create a STAC item stored on the rs-catalog"""

import fnmatch
import json
import logging
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
from botocore.client import Config
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from pystac import Item

from rs_client.stac.catalog_client import CatalogClient
from rs_common.utils import extract_tar, strftime_millis
from rs_workflows.catalog_flow import check_and_create_collection
from rs_workflows.dpr_flow import create_stac_item
from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
)
from rs_workflows.payload_generator import (
    fetch_csv_from_endpoint,
    find_s3_output_bucket,
)
from rs_workflows.utils.catalog import get_single_catalog_item, published_stac_item


@task(cache_policy=NO_CACHE)
async def download_adf_files(
    s3_client: boto3.client,
    bucket: str,
    path: str,
    files: list[str],
    input_dir: str,
) -> list[str]:
    """
    Download ADF files from S3 to a temporary directory.
    Returns the list of local paths to the downloaded files.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)
    local_paths = []

    for file in files:
        s3_key = f"{path}/{file}"
        local_path = os.path.join(input_dir, file)
        logger.info(f"📥 Downloading '{file}' from '{bucket}' with path '{path}' to '{input_dir}'")
        s3_client.download_file(bucket, s3_key, local_path)
        local_paths.append(local_path)

    return local_paths


@task
async def extract_files(files: list[str], extract_dir: str) -> list[str]:
    """
    Extract all .tar.gz files to the specified directory.
    Returns the list of extracted file paths.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)
    extracted_files = []

    for file in files:
        logger.info(f"🧵 Extracting {file} to {extract_dir}")
        count, extracted_files = extract_tar(Path(file), Path(extract_dir))
        logger.debug(f"{count} files have been extracted. list : {extract_files}.")

    return [os.path.join(extract_dir, f) for f in extracted_files]


@task(cache_policy=NO_CACHE)
async def import_items(
    flow_env: FlowEnv,
    s3_client: boto3.client,
    extracted_files: list[str],
    output_path: str,
    override_collection: str,
    override: bool,
    extract_pattern: str,
    rehearsal_mode: bool,
) -> None:
    """
    Copy files matching the extract_pattern to the target OBS bucket.
    If rehearsal_mode is True, only describe the action.
    Returns the list of target S3 keys.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    logger.debug(f"Files will be filtered with pattern '{extract_pattern}'.")
    patterns = extract_pattern.split("|")

    for file in extracted_files:
        filename = Path(file).name
        logger.debug(f"Check for filename '{filename}'.")

        additional_path = output_path.strip("/")
        if additional_path != "":
            additional_path += "/"

        if any(fnmatch.fnmatch(filename, p) for p in patterns):
            # The file should be treated.
            logger.info("✅ This file match the pattern. It will be treated.")

            # Target collection computation
            item_product_type: str = filename[4:15].lower()
            target_collection: str = override_collection if override_collection else item_product_type
            logger.debug(f"The target collection is '{target_collection}'.")

            # Target bucket name computation
            bucket_configuration = fetch_csv_from_endpoint(os.environ["RSPY_HOST_OSAM"] + "/internal/configuration")
            output_bucket: str = find_s3_output_bucket(
                bucket_configuration,
                flow_env.owner_id,
                target_collection,
                item_product_type,
            )
            logger.info(f"🪣 The target bucket will be {output_bucket}.")

            # compute the path on the bucket
            # parent_dir = os.path.dirname(file)
            # relative_path = os.path.relpath(file, parent_dir)
            target_s3_key = f"{flow_env.owner_id}/{target_collection}/{additional_path}"

            # Item creation
            item: Item = await create_new_stac_item(filename, f"s3://{output_bucket}/{target_s3_key}")

            if rehearsal_mode:
                logger.info(
                    f"[REHEARSAL] Would create STAC item into collection {target_collection}:",
                    f" {json.dumps(item.to_dict(), indent=2)}",
                )

            else:
                # Assert that the collection is created, before searching item inside.
                await check_and_create_collection(flow_env, target_collection)

                # Check if the item is already inserted
                logger.debug(f"Check if '{item.id}' is already published on the collection '{target_collection}'.")
                result: Item | None = await get_single_catalog_item(flow_env, item.id, [target_collection])

                if result and override:
                    # We should delete the item from the catalog
                    logger.info(f"Remove the STAC item 🧊'{item.id}' from collection '{target_collection}'.")
                    catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
                    catalog_client.remove_item(target_collection, item.id, raise_for_status=False)
                    result = None

                if not result:
                    # Import the data into the destination bucket
                    logger.info(
                        f"📥 Copy filename '{filename}' into the bucket"
                        " 🪣'{output_bucket}' on the path '{target_s3_key}'.",
                    )
                    s3_client.upload_file(file, output_bucket, f"{target_s3_key}{filename}")

                    # Push the item into the collection
                    logger.debug(
                        f"Push STAC item into rs-catalog collection {target_collection}:"
                        f" {json.dumps(item.to_dict(), indent=2)}",
                    )
                    await published_stac_item(flow_env, item, target_collection)


def convert_date(input_date: str) -> str:
    """
    Convert input format YYYYmmddTHHMMSS to ("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    """
    return strftime_millis(datetime.strptime(input_date, "%Y%m%dT%H%M%S"))


@task
async def create_new_stac_item(item_name: str, href: str) -> Item:
    """
    For each output, create a STAC item with a single asset referencing the output location.
    Returns the STAC item and the asset path.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    # This work for ADF provided by the MPC
    # TODO: Adapt the code for ADF coming from other source ( if needed )
    # Extract the dates, product_type, platform
    cleaned_filename = re.sub(r"\.(zarr|ZARR|tgz|zip|ZIP|TGZ)$", "", item_name)
    dates = re.findall(r"\d{8}T\d{6}", item_name)
    start_datetime = convert_date(dates[0])
    end_datetime = convert_date(dates[1])
    eopf_origin_datetime = convert_date(dates[2])
    product_type: str = item_name[4:15].upper()
    platform: str = item_name[2:3].lower()
    constellation: str = item_name[1:2].lower()
    if constellation == "_":
        constellation = ""
        platform = ""
    else:
        if platform == "_":
            platform = ""
        else:
            platform = f"sentinel-{constellation}{platform}"
        constellation = f"sentinel-{constellation}"

    eopf_feature = {
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {
            "datetime": eopf_origin_datetime,
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "product:type": product_type,
            "platform": platform,
            "constellation": constellation,
        },
    }

    item: Item = create_stac_item(
        eopf_origin_datetime=eopf_origin_datetime,
        eopf_feature=eopf_feature,
        s3_data_location=href + item_name,
        product_name=cleaned_filename,
        dpr_processor="obs_import",
    )

    return item


@flow(name="import-adf-from-obs")
async def import_adf_from_obs(
    configuration: dict,
    owner: str = "copernicus",
    obs_id: str = "PUBLICATION",
    rehearsal_mode: bool = False,
):
    """
    # Import ADF from Object Storage

    Imports a set of *ADF files* into the *rs-catalog* from an object storage bucket.

    ---

    ## Workflow Steps

    1. *Download* — Retrieves the compressed ADF files from the object storage
    2. *Decompress* — Extracts the archive contents
    3. *Filter* — Selects only the relevant ADF files to import
    4. *Publish* — Pushes the selected files to the rs-catalog as STAC items

    ---

    ## Parameters

    | Parameter | Type | Default | Description |
    |---|---|---|---|
    | `configuration` | `dict` | *required* | JSON configuration (see format below) |
    | `owner` | `str` | `copernicus` | Name of the user triggering the flow |
    | `obs_id` | `str` | `PUBLICATION` | Object storage identifier for credentials |
    | `rehearsal_mode` | `bool` | `True` | If `True`, STAC items are *not* published |

    > *Note:* The collection where ADF files are published is derived from `product:type` by default,
    > but can be *overridden* via the configuration.

    ---

    ## Configuration Format example

    ```json
    {
        "input": {
            "bucket": "rs-f1-archive",
            "path": "S3_OL1/3.23/S3_OL1_3.23_2023-06-20/Ancillary_Data",
            "files": ["S3_OL1_3.23_2023-06-20_ADF.tar.gz"],
            "extract_pattern": "S3__*.tgz|S3A_*.tgz"
        },
        "output": {
            "additional_path": "",
            "collection": "adf-olci-baseline-3-23",
            "override": false
        }
    }
    ```
    ---
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    logger.info(f"Starting import-adf-from-obs flow for {owner}")
    env: FlowEnvArgs = FlowEnvArgs(owner_id=owner)

    # Load configuration from Prefect variable
    input_config = configuration["input"]
    output_config = configuration["output"]

    with tempfile.TemporaryDirectory(dir=".", prefix="tmp", delete=True) as temp_dir:
        temp_path: Path = Path(temp_dir)
        input_dir: Path = temp_path / "input"
        output_dir: Path = temp_path / "output"
        logger.info(f"Create input directory '{str(input_dir)}' and output directory '{str(output_dir)}'")
        input_dir.mkdir()
        output_dir.mkdir()

        # Init flow environment and opentelemetry span
        flow_env = FlowEnv(env)
        with flow_env.start_span(__name__, "import-adfs-obs"):
            logger.info(f"🪣 Retrieve credentials to access bucket linked to '{obs_id}'.")

            # Step 1: Connect to S3 with 2 differents settings
            s3_client_input = boto3.client(
                "s3",
                endpoint_url=os.environ[f"S3_{obs_id}_ENDPOINT"],
                aws_access_key_id=os.environ[f"S3_{obs_id}_ACCESSKEY"],
                aws_secret_access_key=os.environ[f"S3_{obs_id}_SECRETKEY"],
                config=Config(signature_version="s3v4"),
                region_name=os.environ[f"S3_{obs_id}_REGION"],
            )
            logger.debug("Get s3 client to retrieve input has been created.")

            s3_client_output = boto3.client(
                "s3",
                endpoint_url=os.environ["S3_ENDPOINT"],
                aws_access_key_id=os.environ["S3_ACCESSKEY"],
                aws_secret_access_key=os.environ["S3_SECRETKEY"],
                config=Config(signature_version="s3v4"),
                region_name=os.environ["S3_REGION"],
            )
            logger.debug("Get s3 client to copy output has been created.")

            # Step 2: Download ADF files
            downloaded_files = await download_adf_files(
                s3_client_input,
                input_config["bucket"],
                input_config["path"],
                input_config["files"],
                str(input_dir),
            )

            # Step 3: Extract files
            extracted_files = await extract_files(downloaded_files, str(output_dir))

            # Step 4: Import data and create STAC item
            await import_items(
                flow_env,
                s3_client_output,
                extracted_files,
                output_config["additional_path"],
                output_config["collection"],
                output_config["override"],
                input_config["extract_pattern"],
                rehearsal_mode,
            )
