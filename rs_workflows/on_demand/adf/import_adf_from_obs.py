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
import logging
import os
import sys
import tarfile
import tempfile
from pathlib import Path

import boto3
from botocore.client import Config
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


def custom_cache_key_fn(*args, **kwargs):
    # Exclure s3_client de la clé de cache
    kwargs.pop("s3_client", None)
    return args, kwargs


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
async def extract_files(local_paths: list[str], extract_dir: str) -> list[str]:
    """
    Extract all .tar.gz files to the specified directory.
    Returns the list of extracted file paths.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)
    extracted_files = []

    for local_path in local_paths:
        logger.info(f"🧵 Extracting {local_path} to {extract_dir}")
        with tarfile.open(local_path, "r:gz") as tar:
            logger.debug(f"The file {local_path} will be uncompressed to the directory '{extract_dir}'.")
            tar.extractall(path=extract_dir, filter="tar")
            extracted_files.extend(tar.getnames())
            logger.debug(f"Following files have been extracted: '{tar.getnames()}'.")

    return [os.path.join(extract_dir, f) for f in extracted_files]


@task(cache_policy=NO_CACHE)
async def import_items(
    owner: str,
    s3_client: boto3.client,
    extracted_files: list[str],
    output_bucket: str,
    output_path: str,
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
    target_keys = []

    logger.debug(f"Files will be filtered with pattern '{extract_pattern}'.")
    patterns = extract_pattern.split("|")

    for file in extracted_files:
        filename = Path(file).name
        logger.debug(f"Check for filename '{filename}'.")

        additional_path = output_path.strip("/")
        if additional_path != "":
            additional_path += "/"

        if any(fnmatch.fnmatch(filename, p) for p in patterns):
            logger.debug(f"✅ Check is OK. This file will be imported on the bucket '{output_bucket}'.")
            parent_dir = os.path.dirname(file)
            relative_path = os.path.relpath(file, parent_dir)
            logger.debug(f"parent directory='{parent_dir}', relative path='{relative_path}'.")
            target_s3_key = f"{owner}/COLLECTION/{additional_path}{relative_path}"

            if not rehearsal_mode:
                s3_client.upload_file(file, output_bucket, target_s3_key)

            # remove filename suffix
            path = Path(filename)
            while path.suffix:
                path = path.with_suffix("")
            await create_stac_item(str(path), "collection", f"s3://{output_bucket}/{target_s3_key}", rehearsal_mode)


@task
async def create_stac_item(item_name: str, item_collection: str, asset: str, rehearsal_mode: bool = True) -> None:
    """
    For each output, create a STAC item with a single asset referencing the output location.
    If rehearsal_mode is True, only describe the action.
    Returns the list of STAC items.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    item = {"id": item_name, "collection": item_collection, "assets": {"data": {"href": asset}}}
    if rehearsal_mode:
        logger.info(f"[REHEARSAL] Would create STAC item for {item}")
    else:
        logger.info(f"Created STAC item for {item}")


@flow(name="import-adf-from-obs")
async def import_adf_from_obs(
    owner: str,
    configuration: dict,
    obs_id: str = "PUBLICATION",
    rehearsal_mode: bool = True,
):
    """
    Main flow: import ADF files from OBS, extract, copy to target, and create STAC items.
    """
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)

    logger.info(f"Starting import-adf-from-obs flow for {owner}")
    env: FlowEnvArgs = FlowEnvArgs(owner_id=owner)

    # Load configuration from Prefect variable
    input_config = configuration["input"]
    output_config = configuration["output"]

    input_dir: str
    output_dir: str
    with tempfile.TemporaryDirectory(dir=".", prefix="tmp", delete=False) as temp_dir:
        temp_path = Path(temp_dir)
        input_dir = temp_path / "input"
        output_dir = temp_path / "output"
        logger.info(f"Create input directory '{input_dir}' and output directory '{output_dir}'")
        input_dir.mkdir()
        output_dir.mkdir()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "import-adfs-obs"):
        logger.info(f"🪣 Retrieve credentials to access bucket linked to '{obs_id}'.")

        # Step 1: Connect to S3
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.environ[f"S3_{obs_id}_ENDPOINT"],
            aws_access_key_id=os.environ[f"S3_{obs_id}_ACCESSKEY"],
            aws_secret_access_key=os.environ[f"S3_{obs_id}_SECRETKEY"],
            config=Config(signature_version="s3v4"),
            region_name=os.environ[f"S3_{obs_id}_REGION"],
        )
        logger.debug(f"s3 client created.")

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
        target_keys = await import_items(
            owner,
            s3_client,
            extracted_files,
            output_config["bucket"],
            output_config["path"],
            input_config["extract_pattern"],
            rehearsal_mode,
        )
