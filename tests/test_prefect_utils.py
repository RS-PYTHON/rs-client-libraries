# Copyright 2025 CS Group
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
from datetime import datetime
from importlib import reload
from unittest.mock import AsyncMock, Mock, PropertyMock, mock_open, patch

import pytest
from prefect.blocks.system import Secret
from prefect.exceptions import ObjectNotFound
from prefect.testing.utilities import prefect_test_harness
from prefect_aws import S3Bucket

from rs_common import prefect_utils

OWNER_ID = "OWNER_ID"


# Init a mockup prefect server, see: https://docs.prefect.io/v3/how-to-guides/workflows/test-workflows
# NOTE: this takes long, so for local testing you can comment it and replace with "docker compose up" from rs-demo
# @pytest.fixture(autouse=True, scope="session")
# def prefect_test_fixture():
#     with prefect_test_harness():
#         yield


def set_local_mode(value: bool, monkeypatch):
    """Configure local or cluster mode"""
    monkeypatch.setenv("RSPY_LOCAL_MODE", str(value))
    reload(prefect_utils)


@pytest.fixture(name="set_env", autouse=True)
def __set_env():
    """Fixture to set environment"""
    os.environ["JUPYTERHUB_USER"] = os.environ["RSPY_HOST_USER"] = OWNER_ID


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
    m = mock_open()
    with patch("builtins.open", m, create=True):
        await prefect_utils.read_apikey(optional=False, save_to_env=True)
    m.assert_called_once_with(os.path.expanduser("~/.env"), "a", encoding="utf-8")
    handle = m()
    handle.write.assert_called_once_with(f"\nRSPY_APIKEY={apikey}\n")


@pytest.mark.parametrize("local_mode", [True, False])
async def test_init_prefect_blocks(monkeypatch, local_mode):
    """Test the init_prefect_blocks function"""

    # Set local or cluster mode
    set_local_mode(local_mode, monkeypatch)

    # Environment variables for all users
    env_global = {
        "RSPY_LOCAL_MODE": "1" if local_mode else "0",
        "PREFECT_BUCKET_NAME": "PREFECT_BUCKET_NAME",
        "PREFECT_BUCKET_FOLDER": "PREFECT_BUCKET_FOLDER",
        "S3_ACCESSKEY": "S3_ACCESSKEY",
        "S3_SECRETKEY": "S3_SECRETKEY",
        "S3_REGION": "S3_REGION",
        "S3_ENDPOINT": "S3_ENDPOINT",
        "LOCAL_DASK_USERNAME": "LOCAL_DASK_USERNAME",
        "LOCAL_DASK_PASSWORD": "LOCAL_DASK_PASSWORD",
    }

    # In local mode, they must be set in the env
    if local_mode:
        for key, value in env_global.items():
            monkeypatch.setenv(key, value)

    # In global mode, they must be set in a prefect block
    else:
        await Secret(value=env_global).save(  # type: ignore[arg-type]
            prefect_utils.BLOCK_NAME_ENV_GLOBAL,
            overwrite=True,
        )

    # Any other env var for the current user
    env_user = {"TEMPO_ENDPOINT": "TEMPO_ENDPOINT"}
    for key, value in env_user.items():
        monkeypatch.setenv(key, value)

    # Call the function
    await prefect_utils.init_prefect_blocks()

    # Check that the blocks were written with the right values
    env_global2 = (await Secret.load(prefect_utils.BLOCK_NAME_ENV_GLOBAL)).get()
    env_user2 = (await Secret.load(prefect_utils.format_env_user(OWNER_ID))).get()
    assert env_global == env_global2
    assert env_user == env_user2

    # Check that the values were save as env vars
    for key, value in {**env_global, **env_user}.items():
        assert os.environ[key] == value


async def test_wait_for_deployment(mocker):
    """Test the wait_for_deployment function"""

    wait = 0.1
    mock_interval = 0.15

    time1 = datetime.now()

    def patch_read_deployment(*_):
        """Path the read_deployment_by_name function. Return success after n seconds."""
        diff = datetime.now() - time1
        if diff.total_seconds() >= mock_interval:
            return  # success
        raise ObjectNotFound(RuntimeError())

    mock_read_deployment = mocker.patch(
        "prefect.client.orchestration._deployments.client.DeploymentAsyncClient.read_deployment_by_name",
        side_effect=patch_read_deployment,
    )

    # Test nominal case
    time1 = datetime.now()
    await prefect_utils.wait_for_deployment("name", wait)
    assert mock_read_deployment.call_count == 3

    # Test timeout
    with pytest.raises(ObjectNotFound):
        time1 = datetime.now()
        await prefect_utils.wait_for_deployment("name", wait, max_retry=2)


async def test_bucket_functions(monkeypatch, mocker):
    """Test the bucket function"""

    # Set env vars for the bucket
    for key, value in {
        "PREFECT_BUCKET_NAME": "PREFECT_BUCKET_NAME",
        "PREFECT_BUCKET_FOLDER": "PREFECT_BUCKET_FOLDER",
        "S3_ACCESSKEY": "S3_ACCESSKEY",
        "S3_SECRETKEY": "S3_SECRETKEY",
        "S3_REGION": "S3_REGION",
        "S3_ENDPOINT": "S3_ENDPOINT",
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

    mocker.patch.object(S3Bucket, "upload_from_path", my_spy := AsyncMock())
    await prefect_utils.s3_upload_file("from_path", "s3_path")
    my_spy.assert_called_once()

    my_spy.reset_mock()
    await prefect_utils.s3_upload_empty_file("s3_path")
    my_spy.assert_called_once()

    mocker.patch.object(S3Bucket, "upload_from_folder", my_spy := AsyncMock())
    await prefect_utils.s3_upload_dir("from_folder", "s3_path")
    my_spy.assert_called_once()

    mocker.patch.object(S3Bucket, "download_object_to_path", my_spy := AsyncMock())
    await prefect_utils.s3_download_file("s3_path", "to_path")
    my_spy.assert_called_once()

    mocker.patch.object(S3Bucket, "get_directory", my_spy := AsyncMock())
    await prefect_utils.s3_download_dir("s3_path", "local_path")
    my_spy.assert_called_once()

    # s3_bucket._get_bucket_resource().objects.filter(...) should return a list of mock objects
    Mock.objects = PropertyMock()
    Mock.objects.filter = Mock(return_value=[Mock()])
    mocker.patch.object(S3Bucket, "_get_bucket_resource", Mock())
    mocker.patch.object(S3Bucket, "_get_s3_client", Mock())
    # Spy on s3_bucket._get_s3_client().delete_objects(...)
    Mock.delete_objects = (my_spy := Mock())
    # Call the function
    prefect_utils.s3_delete("s3_prefix")
    my_spy.assert_called_once()
