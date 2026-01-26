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
from threading import Lock
from typing import Any

from fastapi.concurrency import run_in_threadpool
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client
from prefect.exceptions import ObjectNotFound
from prefect.utilities.asyncutils import sync_compatible
from prefect_aws import AwsCredentials, S3Bucket
from prefect_aws.client_parameters import AwsClientParameters
from pydantic import SecretStr

from rs_client.osam_client import BucketCredentials, OsamClient
from rs_common.logging import Logging
from rs_common.utils import env_bool

# In local mode, all your services are running locally.
# In cluster mode, we use the services deployed on the RS-Server website.
# This configuration is set in an environment variable.
local_mode: bool = False
cluster_mode: bool = not local_mode

# Current user
owner_id: str = ""

# Prefect block names
BLOCK_NAME_ENV_GLOBAL: str = "env-vars"
BLOCK_NAME_ENV_USER: str = "env-vars-{0}"  # env variables for the user/owner_id
BLOCK_NAME_SHARE_BUCKET_GLOBAL: str = "share-bucket"
BLOCK_NAME_SHARE_BUCKET_USER: str = "share-bucket-{0}"  # share bucket for the user/owner_id

# S3 bucket object for each bucket name.
S3_BUCKETS: dict[str, S3Bucket] = {}

lock = Lock()

logger = Logging.default(__name__)


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
        owner_id = os.getenv("JUPYTERHUB_USER", "") if cluster_mode else os.getenv("RSPY_HOST_USER", "")


# Init the global variables
init_global_env()


def get_ip_address() -> str:
    """Return IP address, see: https://stackoverflow.com/a/166520"""
    return socket.gethostbyname(socket.gethostname())


def format_env_user(block_name: str, any_owner_id: str):
    """Format the Prefect secret block for the current user"""
    name = block_name.format(any_owner_id).lower()
    return re.sub("[^a-zA-Z0-9]", "-", name)  # replace special characters by dash


@sync_compatible
async def read_apikey(optional: bool = False, save_to_env: bool = True) -> None:
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
    if apikey:
        logger.debug(f"Use API key (probably from '~/.env'): '{apikey[:4]}***'")
    elif optional:
        logger.debug("Don't use any API key")
    else:
        # Else read it from user input
        apikey = getpass.getpass("Enter your API key:")

        # Save the env var
        os.environ["RSPY_APIKEY"] = apikey

        # Append it to the ~/.env file, if requested.
        # Don't overwrite the full ~/.env file because it can contain other user info.
        if save_to_env:
            with open(os.path.expanduser("~/.env"), "a", encoding="utf-8") as env_file:
                env_file.write(f"\nRSPY_APIKEY={apikey}\n")
                logger.debug("API key saved to '~/.env'")


@sync_compatible
async def update_prefect_block(block_name: str, add_values: dict | None = None, remove_keys: list | None = None):
    """
    Write a prefect secret block with values, or update it if it already exists.

    Args:
        block_name: Prefect secret block name
        add_values: Key/values to add from the block
        remove_keys: Key/values to remove from the block
    """
    if add_values is None:
        add_values = {}
    if remove_keys is None:
        remove_keys = []

    with lock:
        # Try to read the block, if it exists
        try:
            old_values = (await Secret.load(block_name)).get()
        except ValueError:
            old_values = {}

        # Add new values
        updated_values = old_values | add_values

        # Remove values
        for key in remove_keys:
            updated_values.pop(key, None)

        # Write the block if it didn't exist or if values have changed
        if (not old_values) or (old_values != updated_values):
            await Secret(value=updated_values).save(block_name, overwrite=True)


@sync_compatible
async def init_prefect_blocks():
    """Init prefect blocks from the client environment (= from jupyter)"""
    #
    # Env vars for all users

    # In local mode, create the block that contains the environment variables for all users.
    # In cluster mode, this block has already been created by an admin.
    if local_mode:

        # Get all env var names that start with DASK_GATEWAY_
        regex = re.compile("^DASK_GATEWAY_.*")
        dask_gateway_vars = [v for v in os.environ if regex.match(v)]

        # Copy env vars. They are all mandatory.
        global_env_vars = {}
        for key in [
            "RSPY_LOCAL_MODE",
            "PREFECT_BUCKET_NAME",
            "PREFECT_BUCKET_FOLDER",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_PORT",
            "POSTGRES_PI_DB",
            "POSTGRES_HOST",
            *dask_gateway_vars,
            "LOCAL_DASK_USERNAME",
            "LOCAL_DASK_PASSWORD",
        ]:
            global_env_vars[key] = os.environ[key]

        # Save env vars in a secret block for all users
        await update_prefect_block(BLOCK_NAME_ENV_GLOBAL, global_env_vars)

    #
    # Env vars for current user/owner_id

    # In cluster mode, read the API key
    if cluster_mode:
        await read_apikey()

    # Read env vars that are available from the client env in both local and cluster mode.
    user_env_vars = {}

    # Read optional env vars
    for key in ["RSPY_APIKEY"]:
        if value := os.getenv(key):
            user_env_vars[key] = value

    # These are False by default. Save them only if they are set to True.
    for key in [
        "OTEL_PYTHON_REQUESTS_TRACE_HEADERS",
        "OTEL_PYTHON_REQUESTS_TRACE_BODY",
    ]:
        if env_bool(key, default=False):
            user_env_vars[key] = "1"

    # Save env vars in a secret block for the current user
    await update_prefect_block(format_env_user(BLOCK_NAME_ENV_USER, owner_id), user_env_vars)

    # Now read back the blocks so we are sure our env vars are up-to-date
    await read_prefect_blocks(owner_id)


@sync_compatible
async def save_bucket_credentials(
    osam_client: OsamClient | None = None,
    obs_credentials: dict[str, BucketCredentials] | None = None,
):
    """
    Save bucket credentials in the prefect secret block that contains
    the env variables for the current user/owner_id.

    Args:
        osam_client: Used to read the user's credentials from the OSAM service. They are saved in the env vars:
        osam_client, S3_SECRETKEY, S3_ENDPOINT, S3_REGION
        obs_credentials: Used to save custom bucket credentials. For each dict key "<OBS_ID>", the credentials are
        saved in the env vars: S3_<OBS_ID>_ACCESSKEY, S3_<OBS_ID>_SECRETKEY, S3_<OBS_ID>_ENDPOINT, S3_<OBS_ID>_REGION
    """
    env_vars = {}
    if obs_credentials is None:
        obs_credentials = {}

    # Read the user's credentials from osam
    if osam_client:
        credentials = await osam_client.get_credentials()
        env_vars.update(
            {
                "S3_ACCESSKEY": credentials.access_key,
                "S3_SECRETKEY": credentials.secret_key,
                "S3_ENDPOINT": credentials.endpoint,
                "S3_REGION": credentials.region,
            },
        )

    # Add custom credentials
    for key, credentials in obs_credentials.items():
        key = key.upper()
        env_vars.update(
            {
                f"S3_{key}_ACCESSKEY": credentials.access_key,
                f"S3_{key}_SECRETKEY": credentials.secret_key,
                f"S3_{key}_ENDPOINT": credentials.endpoint,
                f"S3_{key}_REGION": credentials.region,
            },
        )

    # Update the prefect block
    await update_prefect_block(format_env_user(BLOCK_NAME_ENV_USER, owner_id), env_vars)

    # Save the env vars
    os.environ.update(env_vars)


@sync_compatible
async def remove_bucket_credentials(obs_ids: list[str] | str):
    """
    Remove bucket credentials from the prefect secret block that contains
    the env variables for the current user/owner_id.

    Args:
        obs_ids: "<OBS_ID>" keys to remove from the env vars:
        S3_<OBS_ID>_ACCESSKEY, S3_<OBS_ID>_SECRETKEY, S3_<OBS_ID>_ENDPOINT, S3_<OBS_ID>_REGION
    """
    # Convert string to list
    if isinstance(obs_ids, str):
        obs_ids = [obs_ids]

    env_vars = []
    for key in obs_ids:
        key = key.upper()
        env_vars += [f"S3_{key}_ACCESSKEY", f"S3_{key}_SECRETKEY", f"S3_{key}_ENDPOINT", f"S3_{key}_REGION"]

    # Update the prefect block
    await update_prefect_block(format_env_user(BLOCK_NAME_ENV_USER, owner_id), remove_keys=env_vars)

    # NOTE: I'm not sure we should remove the env vars from os.environ, it's risky because if the env vars
    # are also defined in other blocks, then maybe we don't want to remove them from os.environ.


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
        os.environ.update((await Secret.load(format_env_user(BLOCK_NAME_ENV_USER, any_owner_id))).get())

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
                logger.info(f"Finished deploying prefect flow: {name!r}")
                return
            except ObjectNotFound:
                retry += 1
                if retry >= max_retry:
                    raise
                logger.info(f"Wait for deployment of prefect flow: {name!r} ...")
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
            aws_secret_access_key=SecretStr(os.environ["S3_SECRETKEY"]),
            region_name=os.environ["S3_REGION"],
            aws_client_parameters=AwsClientParameters(endpoint_url=os.environ["S3_ENDPOINT"]),
        )
        s3_bucket = S3Bucket(
            bucket_name=bucket_name,
            credentials=aws_credentials,
            bucket_folder="",  # no prefixed folder
        )
        S3_BUCKETS[bucket_name] = s3_bucket
        return s3_bucket, object_name


@sync_compatible
async def get_share_bucket(sub_folder: str = "") -> tuple[S3Bucket, str]:
    """
    Get the prefect share bucket folder.

    Args:
        sub_folder: subfolder to use in the bucket. If empty, use the default folder and secret block.
        Else use a specific secret block. WARNING: this only works in a single-threaded environment
        else these specific secret blocks may overwrite each other.

    Returns:
        The prefect block for the share bucket and the block name.
    """

    # Use the default secret block for the default folder
    if not sub_folder:
        block_name = BLOCK_NAME_SHARE_BUCKET_GLOBAL

        # Try to read and return the prefect block, if it already exists.
        try:
            return await S3Bucket.load(block_name), block_name

        # If it doesn't exist yet, create and return it
        except ValueError:
            pass

    # Use a specific block for the current user. Always overwrite it, because the subfolder may have changed.
    else:
        block_name = format_env_user(BLOCK_NAME_SHARE_BUCKET_USER, owner_id)  # type: ignore

    # Read the prefect blocks that contain the S3 authentication
    bucket_name = os.environ["PREFECT_BUCKET_NAME"]
    bucket_folder = os.environ["PREFECT_BUCKET_FOLDER"]

    if sub_folder:
        bucket_folder = os.path.join(bucket_folder, sub_folder)

    # Get a s3 bucket object from its name
    generic_bucket, _ = get_s3_bucket(bucket_name)

    # Create a new object with the same credentials and a prefixed folder
    share_bucket = S3Bucket(
        bucket_name=bucket_name,
        bucket_folder=bucket_folder,
        credentials=generic_bucket.credentials,
    )

    # Save it as a prefect block and return it
    await share_bucket.save(block_name, overwrite=True)
    return share_bucket, block_name


@sync_compatible
async def s3_upload_file(
    from_path: str | Path,
    s3_path: str,
    **upload_kwargs: dict[str, Any],
) -> str:
    """See: S3Bucket.upload_from_path"""
    s3_bucket, to_path = get_s3_bucket(s3_path)
    return await s3_bucket.aupload_from_path(from_path, to_path, **upload_kwargs)


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
    return await s3_bucket.aupload_from_folder(from_folder, to_path, **upload_kwargs)


@sync_compatible
async def s3_download_file(
    s3_path: str,
    to_path: str | Path | None,
    **download_kwargs: dict[str, Any],
) -> Path:
    """See: S3Bucket.download_object_to_path"""
    s3_bucket, from_path = get_s3_bucket(s3_path)
    return await s3_bucket.adownload_object_to_path(from_path, to_path, **download_kwargs)


@sync_compatible
async def s3_download_dir(
    s3_path: str,
    local_path: str | None = None,
) -> None:
    """See: S3Bucket.get_directory"""
    s3_bucket, from_path = get_s3_bucket(s3_path)
    await s3_bucket.aget_directory(from_path, local_path)


def s3_delete(s3_prefix: str, log: bool = False):
    """Remove all files from S3 bucket with the given prefix, using low-level client."""
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
    endpoint_url = s3_bucket.credentials.aws_client_parameters.endpoint_url

    if log:
        keys = [f"s3://{s3_bucket.bucket_name}/{o.get('Key')}" for o in objects_to_delete]
        s3_bucket.logger.info(
            f"Delete from {endpoint_url!r}: {json.dumps(keys, indent=2)}",  # nosec hardcoded_sql_expressions
        )

    # Split the list of objects to delete
    chunk_size = 1000
    for i in range(0, len(objects_to_delete), chunk_size):
        s3_client.delete_objects(
            Bucket=s3_bucket.bucket_name,
            Delete={"Objects": objects_to_delete[i : i + chunk_size], "Quiet": True},  # noqa: E203
        )
