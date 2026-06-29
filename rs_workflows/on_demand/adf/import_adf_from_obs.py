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

"""Convert a set of ADF data."""

import fnmatch
import glob
import json
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, List

import boto3
from botocore.client import Config
from prefect import flow, get_run_logger, task

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@task
async def connect_to_s3(env: FlowEnvArgs) -> boto3.client:
    """
    Connect to S3 (OBS) using the environment's credentials.
    Returns a boto3 S3 client.
    """
    logger = get_run_logger()
    logger.info(f"Connecting to S3 as {env.owner_id}")

    flow_env = FlowEnv(env)
    with flow_env:
        logger.info("Retrieve credentials to access Postgres quota database")

        logger.info(f"Retrieve credentials to access input bucket.")
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ADF_INPUT_ENDPOINT"],
            aws_access_key_id=os.environ["S3_ADF_INPUT_ACCESSKEY"],
            aws_secret_access_key=os.environ["S3_ADF_INPUT_SECRETKEY"],
            config=Config(signature_version="s3v4"),
            region_name=os.environ["S3_ADF_INPUT_REGION"],
        )

        return s3_client


@task
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
    local_paths = []

    for file in files:
        s3_key = f"{path}/{file}"
        local_path = os.path.join(input_dir, file)
        logger.info(f"Downloading {s3_key} from {bucket} to {local_path}")
        s3_client.download_file(bucket, s3_key, local_path)
        local_paths.append(local_path)

    return local_paths


@task
async def extract_files(local_paths: list[str], extract_dir: str) -> list[str]:
    """
    Extract all .tar.gz files to the specified directory.
    Returns the list of extracted file paths.
    """
    logger = get_run_logger()
    extracted_files = []

    for local_path in local_paths:
        logger.info(f"Extracting {local_path} to {extract_dir}")
        with tarfile.open(local_path, "r:gz") as tar:
            tar.extractall(path=extract_dir)
            extracted_files.extend(tar.getnames())

    return [os.path.join(extract_dir, f) for f in extracted_files]


@task
async def copy_to_target_obs(
    env: FlowEnvArgs,
    s3_client: boto3.client,
    extracted_files: list[str],
    output_bucket: str,
    output_path: str,
    extract_pattern: str,
    rehearsal_mode: bool,
) -> list[str]:
    """
    Copy files matching the extract_pattern to the target OBS bucket.
    If rehearsal_mode is True, only describe the action.
    Returns the list of target S3 keys.
    """
    logger = get_run_logger()
    target_keys = []

    for file in extracted_files:
        if fnmatch.fnmatch(file, extract_pattern):
            relative_path = os.path.relpath(file, extracted_files)
            target_key = f"{output_path}/{env.owner_id}/{relative_path}"
            target_s3_key = f"{env.environment}/{target_key}"  # Adjust as needed for your OBS structure

            if rehearsal_mode:
                logger.info(f"[REHEARSAL] Would copy {file} to s3://{output_bucket}/{target_s3_key}")
            else:
                logger.info(f"Copying {file} to s3://{output_bucket}/{target_s3_key}")
                s3_client.upload_file(file, output_bucket, target_s3_key)

        target_keys.append(f"s3://{output_bucket}/{target_s3_key}")

    return target_keys


@task
async def create_stac_item(target_keys: list[str], rehearsal_mode: bool) -> list[dict]:
    """
    For each output, create a STAC item with a single asset referencing the output location.
    If rehearsal_mode is True, only describe the action.
    Returns the list of STAC items.
    """
    logger = get_run_logger()
    stac_items = []

    for key in target_keys:
        item = {"type": "Feature", "properties": {}, "assets": {"data": {"href": key}}}
        if rehearsal_mode:
            logger.info(f"[REHEARSAL] Would create STAC item for {key}")
        else:
            logger.info(f"Created STAC item for {key}")
        stac_items.append(item)

    return stac_items


@flow(name="import-adf-from-obs")
async def import_adf_from_obs(env: FlowEnvArgs, configuration: dict, rehearsal_mode: bool = True):
    """
    Main flow: import ADF files from OBS, extract, copy to target, and create STAC items.
    """
    logger = get_run_logger()
    logger.info(f"Starting import-adf-from-obs flow for {env.owner_id}")

    # Load configuration from Prefect variable
    config = json.loads(configuration)
    input_config = config["input"]
    output_config = config["output"]

    input_dir: str
    output_dir: str
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        input_dir = temp_path / "input"
        output_dir = temp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

    # Step 1: Connect to S3
    s3_client = await connect_to_s3(env)

    # Step 2: Download ADF files
    downloaded_files = await download_adf_files(
        s3_client,
        input_config["bucket"],
        input_config["path"],
        input_config["files"],
        input_dir,
    )

    # Step 3: Extract files
    extracted_files = await extract_files(downloaded_files, output_dir)

    # Step 4: Copy to target OBS
    target_keys = await copy_to_target_obs(
        env,
        s3_client,
        extracted_files,
        output_config["bucket"],
        output_config["path"],
        input_config["extract_pattern"],
        rehearsal_mode,
    )

    # Step 5: Create STAC Item
    stac_items = await create_stac_item(target_keys, rehearsal_mode)

    logger.info(f"Flow completed. Target keys: {target_keys}")
    return {"target_keys": target_keys, "stac_items": stac_items}
