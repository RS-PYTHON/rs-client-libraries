# Copyright 2024 CS Group
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

"""Utility Python module for the tutorials, to be shared with the prefect or dask workers.

WARNING: AFTER EACH MODIFICATION, RESTART THE JUPYTER NOTEBOOK KERNEL !
"""

import asyncio
import getpass
import os
import secrets
import socket
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Dict, Optional, Union

from botocore.utils import calculate_md5
from fastapi.concurrency import run_in_threadpool
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client
from prefect.exceptions import ObjectNotFound
from prefect.utilities.asyncutils import sync_compatible
from prefect_aws import AwsCredentials, S3Bucket

# In local mode, all your services are running locally.
# In cluster mode, we use the services deployed on the RS-Server website.
# This configuration is set in an environment variable.
LOCAL_MODE: bool
CLUSTER_MODE: bool

# Current user name
OWNER_ID: str

# Prefect block names
BLOCK_NAME_S3_SHARE = "s3-share"
BLOCK_NAME_ENV_VARS = "env-vars-{OWNER_ID}"

# S3 bucket object for each bucket name.
S3_BUCKETS: dict[str, S3Bucket] = {}

# S3 bucket for sharing data with prefect
PREFECT_SHARE_BUCKET: S3Bucket = None


def init_global_vars():
    """
    Init the global variables above.
    Needs to be called from the client, then called again from the prefect worker.
    """
    global LOCAL_MODE, CLUSTER_MODE, OWNER_ID, BLOCK_NAME_ENV_VARS

    LOCAL_MODE: bool = os.getenv("RSPY_LOCAL_MODE") == "1"
    CLUSTER_MODE: bool = not LOCAL_MODE

    OWNER_ID = os.environ["JUPYTERHUB_USER"] if CLUSTER_MODE else os.environ["RSPY_HOST_USER"]

    BLOCK_NAME_ENV_VARS = BLOCK_NAME_ENV_VARS.format(OWNER_ID=OWNER_ID)


init_global_vars()  # call it from the client


def get_ip_address() -> str:
    """Return IP address, see: https://stackoverflow.com/a/166520"""
    return socket.gethostbyname(socket.gethostname())


@sync_compatible
async def read_apikey(optional: bool = True, save_to_env: bool = True) -> None:
    """
    Read the API key, either from the environment variable or from an interactive input form.

    Args:
        optional (bool): If False and if the env var is missing, ask it from an interactive input form.
        save_to_env (bool): If True, saves the API key to the ~/.env file.
    """
    # No API key in local mode
    if LOCAL_MODE:
        return

    # If the API is saved as an env var in the ~/.env file, then it has already
    # been read automatically by rs-infra-core/.github/jupyter/resources/00-read-env.py
    apikey = os.getenv("RSPY_APIKEY")
    if (not apikey) and (not optional):

        # Else read it from user input
        apikey = getpass.getpass(f"Enter your API key:")

        # Save the env var
        os.environ["RSPY_APIKEY"] = apikey

        # Append it to the ~/.env file, if requested.
        # Don't overwrite the full ~/.env file because it can contain other user info.
        if save_to_env:
            with open(os.path.expanduser("~/.env"), "a") as env_file:
                env_file.write(f"\nRSPY_APIKEY={apikey}\n")
                print("API key saved to ~/.env.")


@sync_compatible
async def init_prefect_blocks():
    """Init the prefect blocks from the client environment (= from jupyter)"""

    # In cluster mode, read the API key
    if CLUSTER_MODE:
        await read_apikey()

    # Read environment variables that are available from the client env in both local and cluster mode.
    # They are optional.
    env_vars = {}
    for key in (
        "RSPY_LOCAL_MODE",
        "JUPYTERHUB_USER",
        "RSPY_HOST_USER",
        "RSPY_APIKEY",
        "RSPY_OAUTH2_COOKIE",
        "AWS_REQUEST_CHECKSUM_CALCULATION",
        "AWS_RESPONSE_CHECKSUM_VALIDATION",
        "RSPY_WEBSITE",
        "RSPY_UAC_CHECK_URL",
        "TEMPO_ENDPOINT",
        "OTEL_PYTHON_REQUESTS_TRACE_HEADERS",
        "OTEL_PYTHON_REQUESTS_TRACE_BODY",
    ):
        if value := os.environ.get(key):
            env_vars[key] = value

    # In local mode, some secret env vars are known from the client env or hard-coded
    if LOCAL_MODE:
        secret_env = os.environ | {"PREFECT_SHARE_FOLDER": "prefect-share"}  # merge dicts

    # In cluster mode, they should be defined in a block
    else:
        secret_env: dict = await Secret.load(BLOCK_NAME_S3_SHARE)

        # Also read the rspy api key and oauth2 cookie
        if value := await read_apikey():
            env_vars["RSPY_APIKEY"] = value

    # Read the secret env vars. They are mandatory.
    for key in (
        "PREFECT_SHARE_BUCKET",
        "PREFECT_SHARE_FOLDER",
        "S3_ACCESSKEY",
        "S3_SECRETKEY",
        "S3_REGION",
        "S3_ENDPOINT",
    ):
        env_vars[key] = secret_env[key]

    # Save env vars in a secret block for the current user
    Secret(value=env_vars).save(BLOCK_NAME_ENV_VARS)


@sync_compatible
async def blocks_to_env_vars():
    """
    Convert the prefect blocks into environment variables.
    """

    #
    # User authentication = api key and oauth2 cookie

    # Only for cluster mode. Don't overwrite existing environment, if any.
    if cluster_mode:
        auth_user = (await read_block(Secret, get_block_auth_user())).get()
        for key, value in auth_user.items():
            if key not in os.environ:
                os.environ[key] = value


def hack_for_jupyter(func: Callable, *args, **kwargs) -> asyncio.Task:
    """From Jupyter we need this hack to deploy prefect flows"""
    coroutine = run_in_threadpool(func, *args, **kwargs)
    return asyncio.create_task(coroutine)


async def wait_for_deployment(name: str, wait=1, max_retry=30):
    """Wait for prefect deployment to be finished."""
    # Taken from prefect/cli/deployment.py::inspect
    retry = 0
    async with get_client() as client:
        while True:
            try:
                await client.read_deployment_by_name(name)
                print(f"Finished deploying prefect flow: {name!r}")
                return
            except ObjectNotFound:
                retry += 1
                if retry >= max_retry:
                    raise
                print(f"Wait for deployment of prefect flow: {name!r} ...")
                await asyncio.sleep(wait)


#
# Utility functions for s3 bucket operations.


def get_s3_bucket(s3_path: str) -> tuple[S3Bucket, str]:
    """
    Return a prefect S3 bucket object and S3 "object name" (= S3 path without s3://bucket-name)
    from the given S3 path.
    We use the prefect higher-level functions instead of those from boto3.
    Maybe this is not optimized and we should use boto3 instead... but it's only for the demos.
    """

    # Remove the s3:// prefix and split by /
    split = s3_path.removeprefix("s3").removeprefix("S3").strip(":/").split("/")

    # Filter empty elements (if we had double //)
    split = list(filter(None, split))

    if not split:
        raise Exception(f"Invalid S3 path: {s3_path!r}")

    bucket_name = split[0]
    object_name = "/".join(split[1:])

    # Init a new prefect object for this bucket, or create a new one
    # with the same credentials as the configured one, with no prefixed folder.
    try:
        return S3_BUCKETS[bucket_name], object_name
    except KeyError:
        aws_credentials = AwsCredentials(
            aws_access_key_id=os.environ["S3_ACCESSKEY"],
            aws_secret_access_key=os.environ["S3_SECRETKEY"],
            region_name=os.environ["S3_REGION"],
            aws_client_parameters={"endpoint_url": os.environ["S3_ENDPOINT"]},
        )
        s3_bucket = S3Bucket(
            bucket_name=bucket_name,
            credentials=aws_credentials,
            bucket_folder="",
        )
        S3_BUCKETS[bucket_name] = s3_bucket
        return s3_bucket, object_name


@sync_compatible
async def s3_upload_file(
    from_path: str | Path,
    s3_path: str,
    **upload_kwargs: dict[str, Any],
) -> str:
    """See: S3Bucket.upload_from_path"""
    s3_bucket, to_path = get_s3_bucket(s3_path)
    return await s3_bucket.upload_from_path(from_path, to_path, **upload_kwargs)


@sync_compatible
async def s3_upload_empty_file(
    s3_path: str,
    **upload_kwargs: dict[str, Any],
) -> str:
    """Upload an empty temp file to the S3 bucket."""

    # Create a tmp file
    with tempfile.NamedTemporaryFile() as tmp:

        # Add contents to the file or boto3 has a strange behavior after uploading an empty file
        tmp.write(b"empty")
        tmp.flush()

        # Upload the file
        return await s3_upload_file(tmp.name, s3_path, **upload_kwargs)


@sync_compatible
async def s3_upload_dir(
    from_folder: str | Path,
    s3_path: str,
    **upload_kwargs: dict[str, Any],
) -> str | None:
    """
    See: S3Bucket.upload_from_folder

    Uploads files *within* a folder (excluding the folder itself) to the object storage service folder.
    """
    s3_bucket, to_path = get_s3_bucket(s3_path)
    return await s3_bucket.upload_from_folder(from_folder, to_path, **upload_kwargs)


@sync_compatible
async def s3_download_file(
    s3_path: str,
    to_path: str | Path | None,
    **download_kwargs: dict[str, Any],
) -> Path:
    """See: S3Bucket.download_object_to_path"""
    s3_bucket, from_path = get_s3_bucket(s3_path)
    await s3_bucket.download_object_to_path(from_path, to_path, **download_kwargs)


@sync_compatible
async def s3_download_dir(
    s3_path: str,
    local_path: str | None = None,
) -> None:
    """See: S3Bucket.get_directory"""
    s3_bucket, from_path = get_s3_bucket(s3_path)
    await s3_bucket.get_directory(from_path, local_path)


def s3_delete(s3_prefix: str):
    """Remove all files from S3 bucket with the given prefix, using low-level client and Content-MD5 header."""
    s3_bucket, prefix = get_s3_bucket(s3_prefix)
    if not prefix.endswith("/"):
        prefix += "/"
    objects_to_delete = [{"Key": obj.key} for obj in s3_bucket._get_bucket_resource().objects.filter(Prefix=prefix)]

    if not objects_to_delete:
        return

    # Hook to compute Content-MD5 from actual serialized body
    def inject_md5_on_real_payload(request, **kwargs):
        request.headers["Content-MD5"] = calculate_md5(request.body)

    s3_client = s3_bucket._get_s3_client()
    s3_client.meta.events.register(
        "before-sign.s3.DeleteObjects",
        inject_md5_on_real_payload,
    )

    return s3_client.delete_objects(
        Bucket=s3_bucket.bucket_name,
        Delete={"Objects": objects_to_delete, "Quiet": True},
    )
