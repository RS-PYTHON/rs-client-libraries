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

"""Test rs-client-libraries dpr functions"""

import getpass
import tempfile
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest
import responses
from starlette import status

from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_client.rs_client import RsClient
from tests.conftest import MOCKED_RSPY_WEBSITE

RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = getpass.getuser()
CLUSTER_INFO = ClusterInfo("", "", "")


@pytest.fixture(name="dpr_client")
def get_dpr_client() -> DprClient:
    """Create a dpr client

    Args:
        href (str): rs_server href for local-mode

    Returns:
        DprClient: DprClient instance
    """
    client = RsClient(
        rs_server_href=MOCKED_RSPY_WEBSITE,
        rs_server_api_key=RS_SERVER_API_KEY,
        owner_id=OWNER_ID,
        logger=None,
    )
    return client.get_dpr_client()


@responses.activate
@pytest.mark.parametrize("process", ["mockup", DprProcessor.S1L0.value])
def test_dpr_client(mocker, dpr_client: DprClient, process: str, ogcapi_response_sample: dict):
    """Test nominal DPR service response"""

    # Mock response from DPR service
    responses.add(
        method=responses.POST,
        url=f"{MOCKED_RSPY_WEBSITE}/dpr/processes/{process}/execution",
        json=ogcapi_response_sample,
        status=status.HTTP_200_OK,
    )

    def mock_download(_, to_path, **__):
        """
        Mock downloading of the payload file (for the mockup process).
        Create an empty yaml file in the target path..
        """
        with open(to_path, "w", encoding="utf-8") as opened:
            opened.write("empty:")

    mocker.patch("rs_client.ogcapi.dpr_client.prefect_utils.s3_download_file", new=mock_download)

    # Run the DPR processing
    assert dpr_client.run_process(process, CLUSTER_INFO, "", "", "", {}) == ogcapi_response_sample


@pytest.mark.asyncio
@pytest.mark.parametrize("local_mode", [True, False])
async def test_update_configuration(mocker, dpr_client: DprClient, local_mode):
    """Test DprClient.update_configuration"""

    payload_contents = """
store_params:
    storage_options:
    key: ${S3_ACCESSKEY_CLUSTER}
    secret: ${S3_SECRETKEY_CLUSTER}
    client_kwargs:
        endpoint_url: ${S3_ENDPOINT_CLUSTER}
        region_name: ${S3_REGION_CLUSTER}

dask_context:
  cluster_type: gateway
  cluster_config:
    address: ${DASK_GATEWAY_ADDRESS}
    reuse_cluster: ${DASK_CLUSTER_INSTANCE}
    auth:
      type: jupyterhub
      api_token: ${JUPYTERHUB_API_TOKEN}
    auth_local_mode: # auth for local mode
      type: basic
      username: ${LOCAL_DASK_USERNAME}
      password: ${LOCAL_DASK_PASSWORD}

I/O:
  output_products:
  - path: s3://bucket/output
"""

    expected_results_local = """
store_params:
  storage_options: null
  key: ${access_key}
  secret: ${secret_key}
  client_kwargs:
    endpoint_url: ${host_bucket}
    region_name: ${bucket_location}
dask_context:
  cluster_type: gateway
  cluster_config:
    address: address-value
    reuse_cluster: instance-value
    auth:
      type: basic
      username: ${LOCAL_DASK_USERNAME}
      password: ${LOCAL_DASK_PASSWORD}
I/O:
  output_products:
  - path: s3://bucket/output
"""

    expected_results_cluster = """
store_params:
  storage_options: null
  key: ${S3_ACCESSKEY}
  secret: ${S3_SECRETKEY}
  client_kwargs:
    endpoint_url: ${S3_ENDPOINT}
    region_name: ${S3_REGION}
dask_context:
  cluster_type: gateway
  cluster_config:
    address: address-value
    reuse_cluster: instance-value
    auth:
      type: jupyterhub
      api_token: ${JUPYTERHUB_API_TOKEN}
I/O:
  output_products:
  - path: s3://bucket/output
"""

    dpr_client.local_mode = local_mode

    mock_s3_upload_empty = mocker.patch(
        "rs_client.ogcapi.dpr_client.prefect_utils.s3_upload_empty_file",
        new_callable=AsyncMock,
    )

    async def mock_upload(from_path, _, **__):
        """
        Mock uploading of the payload file.
        Return the uploaded file contents.
        """
        async with await anyio.open_file(from_path, encoding="utf-8") as opened:
            return await opened.read()

    mocker.patch("rs_client.ogcapi.dpr_client.prefect_utils.s3_upload_file", new=mock_upload)

    # Write dummy payload file
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(payload_contents.encode("utf-8"))
        tmp.flush()

        uploaded_contents = await dpr_client.update_configuration(
            local_path=tmp.name,
            s3_path="",
            is_payload=True,
            DASK_GATEWAY_ADDRESS="address-value",
            DASK_CLUSTER_INSTANCE="instance-value",
        )

    mock_s3_upload_empty.assert_awaited_with("s3://bucket/output/.empty")
    assert uploaded_contents.strip() == (expected_results_local if local_mode else expected_results_cluster).strip()


def test_logs_are_parsed_and_logged():
    """Test dpr_flow's read of processor log file."""

    entries = [
        {
            "level": "INFO",
            "message": ("Quality Control: Detected 0 duplicated packets in SAR instrument data"),
        },
        {
            "level": "DEBUG",
            "message": ">> EOTriggerWorkflowParser.parse",
        },
    ]

    mock_logger = MagicMock()

    for entry in entries:
        level = entry["level"].strip().lower()
        getattr(mock_logger, level, mock_logger.info)(entry["message"])

    mock_logger.info.assert_any_call("Quality Control: Detected 0 duplicated packets in SAR instrument data")

    mock_logger.debug.assert_any_call(">> EOTriggerWorkflowParser.parse")
