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
import json
import os
import re
import socket
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from botocore.utils import calculate_md5
from fastapi.concurrency import run_in_threadpool
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client
from prefect.exceptions import ObjectNotFound
from prefect.utilities.asyncutils import sync_compatible
from prefect_aws import AwsCredentials, S3Bucket

from rs_common.utils import env_bool

# In local mode, all your services are running locally.
# In cluster mode, we use the services deployed on the RS-Server website.
# This configuration is set in an environment variable.
local_mode: bool = False
cluster_mode: bool = not local_mode

# Current user
owner_id: str | None = ""

# Prefect block names
BLOCK_NAME_ENV_GLOBAL: str = "env-vars"
BLOCK_NAME_ENV_USER: str = "env-vars-{0}"  # env variables for the user/owner_id
BLOCK_NAME_SHARE_BUCKET: str = "share-bucket"

# S3 bucket object for each bucket name.
S3_BUCKETS: dict[str, S3Bucket] = {}


def init_global_env(new_owner_id: str | None = None):
    """
    Init the global variables above from the env vars.
    Needs to be called from the client, then called again from the prefect workers.

    Args:
        new_owner_id: new ower/user id, if known.
    """
    global local_mode, cluster_mode, owner_id

    local_mode = env_bool("RSPY_LOCAL_MODE", default=False)
    cluster_mode = not local_mode

    # Override the owner id or read it from the env vars
    if new_owner_id:
        owner_id = new_owner_id
    else:
        owner_id = os.getenv("JUPYTERHUB_USER") if cluster_mode else os.getenv("RSPY_HOST_USER")


# Init the global variables
init_global_env()


def get_ip_address() -> str:
    """Return IP address, see: https://stackoverflow.com/a/166520"""
    return socket.gethostbyname(socket.gethostname())


def format_env_user(any_owner_id: str):
    """Format the Prefect secret block for the current user"""
    name = BLOCK_NAME_ENV_USER.format(any_owner_id).lower()
    return re.sub("[^a-zA-Z0-9]", "-", name)  # replace special characters by dash


@sync_compatible
async def read_apikey(optional: bool = True, save_to_env: bool = True) -> None:
    """
    Read the API key, either from the environment variable or from an interactive input form.

    Args:
        optional (bool): If False and if the env var is missing, ask it from an interactive input form.
        save_to_env (bool): If True, saves the API key to the ~/.env file.
    """
    # No API key in local mode
    if local_mode:
        return

    # If the API is saved as an env var in the ~/.env file, then it has already
    # been read automatically by rs-infra-core/.github/jupyter/resources/00-read-env.py
    apikey = os.getenv("RSPY_APIKEY")
    if (not apikey) and (not optional):

        # Else read it from user input
        apikey = getpass.getpass("Enter your API key:")

        # Save the env var
        os.environ["RSPY_APIKEY"] = apikey

        # Append it to the ~/.env file, if requested.
        # Don't overwrite the full ~/.env file because it can contain other user info.
        if save_to_env:
            with open(os.path.expanduser("~/.env"), "a", encoding="utf-8") as env_file:
                env_file.write(f"\nRSPY_APIKEY={apikey}\n")
                print("API key saved to ~/.env.")


@sync_compatible
async def init_prefect_blocks():
    """Init prefect blocks from the client environment (= from jupyter)"""

    #
    # Env vars for all users

    # In local mode, create the block that contains the environment variables for all users.
    # In cluster mode, this block has already been created by an admin.
    # All fields are mandatory.
    if local_mode:
        await Secret(
            value={  # type: ignore[arg-type]
                "RSPY_LOCAL_MODE": "1",
                "PREFECT_BUCKET_NAME": os.getenv("PREFECT_BUCKET_NAME") or os.environ["RSPY_TEMP_BUCKET"],
                "PREFECT_BUCKET_FOLDER": os.getenv("PREFECT_BUCKET_FOLDER", "prefect-share"),
                "S3_ACCESSKEY": os.environ["S3_ACCESSKEY"],
                "S3_SECRETKEY": os.environ["S3_SECRETKEY"],
                "S3_REGION": os.environ["S3_REGION"],
                "S3_ENDPOINT": os.environ["S3_ENDPOINT"],
                "LOCAL_DASK_USERNAME": os.environ["LOCAL_DASK_USERNAME"],
                "LOCAL_DASK_PASSWORD": os.environ["LOCAL_DASK_PASSWORD"],
            },
        ).save(BLOCK_NAME_ENV_GLOBAL, overwrite=True)

    #
    # Env vars for current user/owner_id

    # In cluster mode, read the API key
    if cluster_mode:
        await read_apikey()

    # Read environment variables that are available from the client env in both local and cluster mode.
    # They are optional.
    env_vars = {}
    for key in (
        "AWS_REQUEST_CHECKSUM_CALCULATION",
        "AWS_RESPONSE_CHECKSUM_VALIDATION",
        "OTEL_PYTHON_REQUESTS_TRACE_HEADERS",
        "OTEL_PYTHON_REQUESTS_TRACE_BODY",
        "RSPY_APIKEY",
        "RSPY_OAUTH2_COOKIE",
        "RSPY_UAC_CHECK_URL",
        "RSPY_WEBSITE",
        "TEMPO_ENDPOINT",
        # dask in cluster mode
        "DASK_GATEWAY_ADDRESS",
        # dask in local mode
        "DASK_GATEWAY_STAGING_ADDRESS",
        "DASK_GATEWAY_EOPF_ADDRESS",
        "DASK_GATEWAY_EOPF_MOCKUP_ADDRESS",
        "DASK_GATEWAY_STAGING_PUBLIC",
        "DASK_GATEWAY_EOPF_PUBLIC",
        "DASK_GATEWAY_EOPF_MOCKUP_PUBLIC",
    ):
        if value := os.getenv(key):
            env_vars[key] = value

    # Save env vars in a secret block for the current user
    await Secret(value=env_vars).save(format_env_user(owner_id), overwrite=True)  # type: ignore

    # Now read back the blocks so we are sure our env vars are up-to-date
    await read_prefect_blocks(owner_id)


@sync_compatible
async def read_prefect_blocks(any_owner_id: str | None = None):
    """
    Read prefect blocks from the prefect flow and tasks into env vars and global vars.

    Args:
        owner_id: Read prefect blocks for a specific user.
    """

    # Read the env vars for all users
    os.environ.update((await Secret.load(BLOCK_NAME_ENV_GLOBAL)).get())

    # Read the env vars for the given user
    if any_owner_id:
        os.environ.update((await Secret.load(format_env_user(any_owner_id))).get())

    # Init the env of the current module from the env vars we have just read
    init_global_env(any_owner_id)


def hack_for_jupyter(func: Callable, *args, **kwargs) -> asyncio.Task:
    """From Jupyter we need this hack to deploy prefect flows"""
    coroutine = run_in_threadpool(func, *args, **kwargs)
    return asyncio.create_task(coroutine)


async def wait_for_deployment(name: str, wait: int | float = 1, max_retry: int = 30):
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
    """

    # Remove the s3:// prefix and split by /
    split = s3_path.removeprefix("s3").removeprefix("S3").strip(":/").split("/")

    # Filter empty elements (if we had double //)
    split = list(filter(None, split))

    if not split:
        raise ValueError(f"Invalid S3 path: {s3_path!r}")

    bucket_name = split[0]
    object_name = "/".join(split[1:])

    # Try to return an existing bucket object
    try:
        return S3_BUCKETS[bucket_name], object_name

    # Else create a new one
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
            bucket_folder="",  # no prefixed folder
        )
        S3_BUCKETS[bucket_name] = s3_bucket
        return s3_bucket, object_name


@sync_compatible
async def get_share_bucket() -> S3Bucket:
    """Get the prefect share bucket folder"""

    # Try to read it from the prefect block
    try:
        return await S3Bucket.load(BLOCK_NAME_SHARE_BUCKET)

    # If it doesn't exist yet, create and return it
    except ValueError:
        pass

    # Read the prefect blocks that contain the S3 authentication
    bucket_name = os.environ["PREFECT_BUCKET_NAME"]
    bucket_folder = os.environ["PREFECT_BUCKET_FOLDER"]

    # Get a s3 bucket object from its name
    generic_bucket, _ = get_s3_bucket(bucket_name)

    # Create a new object with the same credentials and a prefixed folder
    share_bucket = S3Bucket(
        bucket_name=bucket_name,
        bucket_folder=bucket_folder,
        credentials=generic_bucket.credentials,
    )

    # Save it as a prefect block and return it
    await share_bucket.save(BLOCK_NAME_SHARE_BUCKET, overwrite=True)
    return share_bucket


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
    return await s3_bucket.download_object_to_path(from_path, to_path, **download_kwargs)


@sync_compatible
async def s3_download_dir(
    s3_path: str,
    local_path: str | None = None,
) -> None:
    """See: S3Bucket.get_directory"""
    s3_bucket, from_path = get_s3_bucket(s3_path)
    await s3_bucket.get_directory(from_path, local_path)


def s3_delete(s3_prefix: str, log: bool = False):
    """Remove all files from S3 bucket with the given prefix, using low-level client and Content-MD5 header."""
    s3_bucket, prefix = get_s3_bucket(s3_prefix)
    objects = s3_bucket._get_bucket_resource().objects  # pylint: disable=protected-access

    # objects.filter(Prefix=prefix) makes a recursive search and returns only files, not folders.
    # If we want to delete a file, call the filter and only keep the exact matches.
    try_file = [obj.key for obj in objects.filter(Prefix=prefix)]
    if prefix in try_file:
        objects_to_delete = [{"Key": prefix}]

    # If we want to delete a folder, make sure the prefix ends by /, call the filter and keep everything
    else:
        objects_to_delete = [{"Key": obj.key} for obj in objects.filter(Prefix=prefix.rstrip("/") + "/")]

    if not objects_to_delete:
        return

    s3_client = s3_bucket._get_s3_client()  # pylint: disable=protected-access

    # Not for the local minio bucket
    endpoint_url = s3_bucket.credentials.aws_client_parameters.endpoint_url
    if not any(provider in endpoint_url for provider in ["localhost", "minio"]):

        # Hook to compute Content-MD5 from actual serialized body
        def inject_md5_on_real_payload(request, **_):
            request.headers["Content-MD5"] = calculate_md5(request.body)

        s3_client.meta.events.register(
            "before-sign.s3.DeleteObjects",
            inject_md5_on_real_payload,
        )

    if log:
        keys = [f"s3://{s3_bucket.bucket_name}/{o.get('Key')}" for o in objects_to_delete]
        s3_bucket.logger.info(
            f"Delete from {endpoint_url!r}: {json.dumps(keys, indent=2)}",  # nosec hardcoded_sql_expressions
        )

    # Split the list of objects to delete
    chunk_size = 100
    for i in range(0, len(objects_to_delete), chunk_size):
        s3_client.delete_objects(
            Bucket=s3_bucket.bucket_name,
            Delete={"Objects": objects_to_delete[i : i + chunk_size], "Quiet": True},  # noqa: E203
        )
