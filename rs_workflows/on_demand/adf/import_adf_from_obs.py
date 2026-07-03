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
    extracted_files: list[str] = []

    for file in files:
        logger.info(f"🧵 Extracting {file} to {extract_dir}")
        count, extracted_files = extract_tar(Path(file), Path(extract_dir))
        logger.debug("%s files have been extracted:\n%s", count, "\n".join(f"- {f}" for f in extracted_files))

    return [os.path.join(extract_dir, f) for f in extracted_files]


def _filter_files_by_pattern(files: list[str], extract_pattern: str) -> list[str]:
    """Filter files matching at least one pattern in `extract_pattern` (split by '|')."""
    patterns = extract_pattern.split("|")
    return [file for file in files if any(fnmatch.fnmatch(Path(file).name, p) for p in patterns)]


def _compute_target_collection(filename: str, override_collection: str | None) -> str:
    """Compute the target collection from the filename or override."""
    if override_collection:
        return override_collection
    # Extract product type
    return filename[4:15].lower()


def _get_output_bucket(
    bucket_configuration: list[list[str]],
    owner_id: str,
    target_collection: str,
    product_type: str,
) -> str:
    """
    Find the S3 output bucket from the configuration.
    """
    output_bucket = find_s3_output_bucket(bucket_configuration, owner_id, target_collection, product_type)
    if not output_bucket:
        raise ValueError(
            f"❌ No S3 bucket found for owner={owner_id}, "
            f"collection={target_collection}, product:type={product_type}",
        )
    return output_bucket


def _build_s3_key(owner_id: str, target_collection: str, additional_path: str, filename: str) -> str:
    """
    Build the target S3 key path.
    """
    additional_path = additional_path.strip("/")
    if additional_path:
        additional_path += "/"
    return f"{owner_id}/{target_collection}/{additional_path}{filename}"


def _handle_rehearsal_mode(item: Item, target_collection: str) -> None:
    """
    Log the STAC item that would be created in rehearsal mode.
    """
    logger = get_run_logger()
    logger.info(
        f"[REHEARSAL] Would create STAC item in collection '{target_collection}':\n"
        f"{json.dumps(item.to_dict(), indent=2)}",
    )


async def _handle_production_mode(
    flow_env: FlowEnv,
    s3_client: boto3.client,
    file: str,
    output_bucket: str,
    target_s3_key: str,
    item: Item,
    target_collection: str,
    override: bool,
) -> None:
    """Handle file upload and STAC item publication in production mode."""
    logger = get_run_logger()

    # Ensure the collection exists
    await check_and_create_collection(flow_env, target_collection)

    # Check if the item already exists
    logger.debug(f"Checking if item '{item.id}' already exists in collection '{target_collection}'.")
    existing_item = await get_single_catalog_item(flow_env, item.id, [target_collection])

    if existing_item and override:
        logger.info(f"Removing existing STAC item 🧊 '{item.id}' from collection '{target_collection}'.")
        catalog_client = flow_env.rs_client.get_catalog_client()
        catalog_client.remove_item(target_collection, item.id, raise_for_status=False)
        existing_item = None

    if not existing_item:
        # Upload the file to S3
        logger.info(f"📥 Uploading file '{file}' to bucket 🪣 '{output_bucket}' at path '{target_s3_key}'.")
        s3_client.upload_file(file, output_bucket, target_s3_key)

        # Publish the STAC item
        logger.info(
            f"Publishing STAC item 🧊 '{item.id}' to collection '{target_collection}':\n"
            f"{json.dumps(item.to_dict(), indent=2)}",
        )
        await published_stac_item(flow_env, item, target_collection)


@task(cache_policy=NO_CACHE)
async def import_items(
    flow_env: FlowEnv,
    s3_client: boto3.client,
    extracted_files: list[str],
    output_path: str,
    override_collection: str | None = None,
    override: bool = False,
    extract_pattern: str = "*",
    rehearsal_mode: bool = False,
) -> None:
    """
    Copy files matching `extract_pattern` to the target S3 bucket and STAC catalog.
    If `rehearsal_mode` is True, only log the actions without executing them.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    # Step 1: Filter files by pattern
    logger.debug(f"Filtering files with pattern: '{extract_pattern}'")
    matching_files = _filter_files_by_pattern(extracted_files, extract_pattern)
    if not matching_files:
        logger.warning("⚠️ No files matched the extraction pattern. Exiting.")
        return

    # Step 2: Process each matching file
    for file in matching_files:
        filename = Path(file).name
        logger.debug(f"✅ The file '{filename}' matchs the pattern. Processing...")

        # Compute target collection
        target_collection = _compute_target_collection(filename, override_collection)
        logger.info(f"Target collection: '{target_collection}'")

        # Compute target S3 bucket
        product_type = filename[4:15].lower()
        bucket_configuration: list[list[str]] = fetch_csv_from_endpoint(
            os.environ["RSPY_HOST_OSAM"] + "/internal/configuration",
        )
        output_bucket = _get_output_bucket(bucket_configuration, flow_env.owner_id, target_collection, product_type)
        logger.info(f"🪣 Target S3 bucket: '{output_bucket}'")

        # Compute S3 key
        target_s3_key = _build_s3_key(flow_env.owner_id, target_collection, output_path, filename)
        logger.debug(f"Target S3 key: '{target_s3_key}'")

        # Create STAC item
        item = await create_new_stac_item(filename, f"s3://{output_bucket}/{target_s3_key}")

        # Handle rehearsal or production mode
        if rehearsal_mode:
            _handle_rehearsal_mode(item, target_collection)
        else:
            await _handle_production_mode(
                flow_env,
                s3_client,
                file,
                output_bucket,
                target_s3_key,
                item,
                target_collection,
                override,
            )


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
        s3_data_location=href,
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
            "override": False
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
            logger.debug("Get s3 client to retrieve input.")

            s3_client_output = boto3.client(
                "s3",
                endpoint_url=os.environ["S3_ENDPOINT"],
                aws_access_key_id=os.environ["S3_ACCESSKEY"],
                aws_secret_access_key=os.environ["S3_SECRETKEY"],
                config=Config(signature_version="s3v4"),
                region_name=os.environ["S3_REGION"],
            )
            logger.debug("Get s3 client to copy output.")

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
