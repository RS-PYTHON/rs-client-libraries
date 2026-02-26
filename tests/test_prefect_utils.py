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

"""Test the prefect_utils module"""

import getpass
import os
import socket
from contextlib import suppress
from importlib import reload
from unittest.mock import AsyncMock, Mock, mock_open, patch

import pytest
from prefect.blocks.system import Secret
from prefect.exceptions import ObjectNotFound
from prefect_aws import S3Bucket

from rs_client.osam_client import BucketCredentials
from rs_common import prefect_utils

OWNER_ID = "OWNER_ID"


def set_local_mode(value: bool, monkeypatch):
    """Configure local or cluster mode"""
    monkeypatch.setenv("RSPY_LOCAL_MODE", str(value))
    reload(prefect_utils)


@pytest.fixture(name="set_env", autouse=True)
def __set_env(monkeypatch):
    """Fixture to set environment"""
    monkeypatch.setenv("JUPYTERHUB_USER", OWNER_ID)
    monkeypatch.setenv("RSPY_HOST_USER", OWNER_ID)


async def test_get_ip_address():
    """Test get_ip_address function"""
    assert prefect_utils.get_ip_address() == socket.gethostbyname(socket.gethostname())


async def test_read_apikey(monkeypatch, mocker):
    """Test the read_apikey function"""

    # Only in cluster mode
    set_local_mode(False, monkeypatch)

    # Mock the getpass.getpass function
    apikey = "my_apikey"
    mocker.patch.object(getpass, "getpass", return_value=apikey)

    # Don't really save to .env file. See: https://docs.python.org/3.3/library/unittest.mock.html#mock-open
    opened = mock_open()
    with patch("builtins.open", opened, create=True):
        await prefect_utils.read_apikey(optional=False, save_to_env=True)
        assert os.environ["RSPY_APIKEY"] == apikey
    opened.assert_called_once_with(os.path.expanduser("~/.env"), "a", encoding="utf-8")
    handle = opened()
    handle.write.assert_called_once_with(f"\nRSPY_APIKEY={apikey}\n")


@pytest.mark.parametrize("local_mode", [True, False], ids=["local", "cluster"])
async def test_init_prefect_blocks(monkeypatch, mock_prefect, local_mode):  # pylint: disable=unused-argument
    """Test the init_prefect_blocks function"""

    # Set local or cluster mode
    set_local_mode(local_mode, monkeypatch)

    # Remove the existing blocks, if any
    user_block_name = prefect_utils.format_env_user(prefect_utils.BLOCK_NAME_ENV_USER, OWNER_ID)
    with suppress(ValueError):
        await Secret.delete(prefect_utils.BLOCK_NAME_ENV_GLOBAL)
    with suppress(ValueError):
        await Secret.delete(user_block_name)

    # Environment variables for all users
    env_global = {
        "RSPY_LOCAL_MODE": "1" if local_mode else "0",
        "PREFECT_BUCKET_NAME": "PREFECT_BUCKET_NAME",
        "PREFECT_BUCKET_FOLDER": "PREFECT_BUCKET_FOLDER",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_HOST": "test_host",
        "POSTGRES_PORT": "5432",
        "POSTGRES_PI_DB": "test_db",
        "DASK_GATEWAY_ADDRESS": "DASK_GATEWAY_ADDRESS",
        "LOCAL_DASK_USERNAME": "LOCAL_DASK_USERNAME",
        "LOCAL_DASK_PASSWORD": "LOCAL_DASK_PASSWORD",
    }

    # In local mode, they must be set in the env
    if local_mode:
        for key, value in env_global.items():
            monkeypatch.setenv(key, value)

    # In cluster mode, they must be set in a prefect block
    else:
        env_global["TEMPO_ENDPOINT"] = "TEMPO_ENDPOINT"
        await Secret(value=env_global).save(  # type: ignore[arg-type]
            prefect_utils.BLOCK_NAME_ENV_GLOBAL,
            overwrite=True,
        )

    # Any other env var for the current user
    env_user = {
        "RSPY_APIKEY": "RSPY_APIKEY",
    }
    for key, value in env_user.items():
        monkeypatch.setenv(key, value)

    env_user.update(
        {
            # Default bucket credentials
            "S3_ACCESSKEY": "ak0",
            "S3_SECRETKEY": "sk0",
            "S3_ENDPOINT": "endpoint0",
            "S3_REGION": "region0",
            # Add extra bucket credentials
            "S3_OBS1_ACCESSKEY": "ak1",
            "S3_OBS1_SECRETKEY": "sk1",
            "S3_OBS1_ENDPOINT": "endpoint1",
            "S3_OBS1_REGION": "region1",
            # Add extra bucket credentials
            "S3_OBS2_ACCESSKEY": "ak2",
            "S3_OBS2_SECRETKEY": "sk2",
            "S3_OBS2_ENDPOINT": "endpoint2",
            "S3_OBS2_REGION": "region2",
        },
    )

    # Call the function
    await prefect_utils.init_prefect_blocks()

    async def mock_get_credentials() -> BucketCredentials:
        """Mock the osam get_credenitals() method"""
        return BucketCredentials(access_key="ak0", secret_key="sk0", endpoint="endpoint0", region="region0")

    osam_client = Mock()
    osam_client.get_credentials = mock_get_credentials

    # Add user's credentials
    await prefect_utils.save_bucket_credentials(
        osam_client,
        {
            "obs1": BucketCredentials(access_key="ak1", secret_key="sk1", endpoint="endpoint1", region="region1"),
            "obs2": BucketCredentials(access_key="ak2", secret_key="sk2", endpoint="endpoint2", region="region2"),
        },
    )

    # Check that the blocks were written with the right values
    assert env_global == (await Secret.load(prefect_utils.BLOCK_NAME_ENV_GLOBAL)).get()
    assert env_user == (await Secret.load(user_block_name)).get()

    # Check that the values were save as env vars
    for key, value in {**env_global, **env_user}.items():
        assert os.environ[key] == value

    # If we call the block init a second time, nothing changes
    await prefect_utils.init_prefect_blocks()
    assert env_global == (await Secret.load(prefect_utils.BLOCK_NAME_ENV_GLOBAL)).get()
    assert env_user == (await Secret.load(user_block_name)).get()

    # Remove credentials and check that they are missing from the user block
    await prefect_utils.remove_bucket_credentials("obs2")
    for key in list(env_user.keys()):
        if "OBS2" in key:
            env_user.pop(key)
    assert env_user == (await Secret.load(user_block_name)).get()


@pytest.mark.asyncio
async def test_wait_for_deployment(mocker):
    """Test the wait_for_deployment function"""

    wait = 0.1

    # Mock read_deployment_by_name to fail twice, then succeed
    mock_read_deployment = mocker.patch(
        "prefect.client.orchestration._deployments.client.DeploymentAsyncClient.read_deployment_by_name",
        new_callable=AsyncMock,
        side_effect=[ObjectNotFound(RuntimeError("not found")), ObjectNotFound(RuntimeError("not found")), None],
    )

    # Test nominal case
    await prefect_utils.wait_for_deployment("name", wait)
    assert mock_read_deployment.call_count == 3

    # Test timeout
    mock_read_deployment.side_effect = [ObjectNotFound(RuntimeError("not found"))] * 3
    with pytest.raises(ObjectNotFound):
        await prefect_utils.wait_for_deployment("name", wait, max_retry=2)


async def test_bucket_functions(monkeypatch, mocker):
    """Test the bucket function"""

    # Set env vars for the bucket
    for key, value in {
        "PREFECT_BUCKET_NAME": "PREFECT_BUCKET_NAME",
        "PREFECT_BUCKET_FOLDER": "PREFECT_BUCKET_FOLDER",
        "S3_ACCESSKEY": "S3_ACCESSKEY",
        "S3_SECRETKEY": "S3_SECRETKEY",
        "S3_REGION": "region3",
        "S3_ENDPOINT": "https://endpoint3",
    }.items():
        monkeypatch.setenv(key, value)

    # Spy on functions
    spy_save = mocker.spy(S3Bucket, "save")
    spy_get_s3_bucket = mocker.spy(prefect_utils, "get_s3_bucket")

    # Call the function several times
    for _ in range(5):
        await prefect_utils.get_share_bucket()

    # Check that the spied functions were called only 0 or 1 time.
    # They are called 0 times if you reuse the same prefect env and the block was already created.
    assert spy_save.call_count <= 1
    assert spy_get_s3_bucket.call_count <= 1

    #
    # Test bucket operations, just call the functions, don't check the underlying s3 functions

    mocker.patch.object(S3Bucket, "aupload_from_path", my_spy := AsyncMock())
    await prefect_utils.s3_upload_file("from_path", "s3_path")
    my_spy.assert_called_once()

    my_spy.reset_mock()
    await prefect_utils.s3_upload_empty_file("s3_path")
    my_spy.assert_called_once()

    mocker.patch.object(S3Bucket, "aupload_from_folder", my_spy := AsyncMock())
    await prefect_utils.s3_upload_dir("from_folder", "s3_path")
    my_spy.assert_called_once()

    mocker.patch.object(S3Bucket, "adownload_object_to_path", my_spy := AsyncMock())
    await prefect_utils.s3_download_file("s3_path", "to_path")
    my_spy.assert_called_once()

    mocker.patch.object(S3Bucket, "aget_directory", my_spy := AsyncMock())
    await prefect_utils.s3_download_dir("s3_path", "local_path")
    my_spy.assert_called_once()

    # s3_bucket._get_bucket_resource().objects.filter(...) should return a list of mock objects
    mocker.patch.object(S3Bucket, "_get_bucket_resource", Mock())
    Mock.filter = Mock(return_value=[Mock()])
    # Spy on s3_bucket._get_s3_client().delete_objects(...)
    mocker.patch.object(S3Bucket, "_get_s3_client", Mock())
    Mock.delete_objects = (spy_delete_objects := Mock())
    # Call the function
    prefect_utils.s3_delete("s3_prefix")
    spy_delete_objects.assert_called_once()
