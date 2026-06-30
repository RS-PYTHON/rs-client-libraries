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
from unittest.mock import AsyncMock

import pytest
import responses
from starlette import status

from rs_client.ogcapi.dpr_client import ClusterInfo, DprClient, DprProcessor
from rs_client.rs_client import RsClient
from rs_workflows.dpr_flow import run_processor
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

    # Mock reading of the payload file (for the mockup process): return empty yaml bytes
    mocker.patch(
        "rs_client.ogcapi.dpr_client.prefect_utils.s3_read_bytes",
        return_value=b"empty:",
    )

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

    async def mock_upload(data, _s3_path, **__):
        """
        Mock uploading of the payload contents.
        Return the uploaded contents as text.
        """
        return data.decode("utf-8")

    mocker.patch("rs_client.ogcapi.dpr_client.prefect_utils.s3_upload_bytes", new=mock_upload)

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


# ---------------------------------------------------------------------------

# paths_to_delete logic tests (run_processor autoclean collection)


def _make_mock_product(mocker, *, prod_id, path, final_product, autoclean):
    """Create a mock output product with the given attributes."""
    prod = mocker.Mock()
    prod.id = prod_id
    prod.path = path
    prod.final_product = final_product
    prod.autoclean = autoclean
    return prod


def _setup_run_processor_mocks(mocker):
    """
    Patch all run_processor dependencies unrelated to paths_to_delete so that
    the function can complete without real S3/DPR infrastructure.
    """
    mocker.patch("rs_workflows.dpr_flow.get_run_logger", return_value=mocker.Mock())
    mocker.patch("rs_workflows.dpr_flow.record_performance_indicators")
    mocker.patch("rs_workflows.dpr_flow.update_eopf_assets", return_value=[])

    mock_dpr_client = mocker.Mock()
    mock_dpr_client.run_process.return_value = mocker.Mock()
    mock_dpr_client.wait_for_job.return_value = None

    mock_flow_env = mocker.MagicMock()
    mock_flow_env.rs_client.get_dpr_client.return_value = mock_dpr_client
    mocker.patch("rs_workflows.dpr_flow.FlowEnv", return_value=mock_flow_env)

    mocker.patch(
        "rs_workflows.dpr_flow.prefect_utils.s3_download_dir",
        new_callable=AsyncMock,
    )


@pytest.mark.asyncio
async def test_paths_to_delete_contains_autoclean_paths(mocker):
    """
    Verify that run_processor collects the paths of output products whose
    autoclean flag is True, and passes them to clean_paths.
    Products with autoclean=False must not appear in the list.
    """
    _setup_run_processor_mocks(mocker)
    mock_clean = mocker.patch("rs_workflows.dpr_flow.clean_paths")

    prod_autoclean = _make_mock_product(
        mocker,
        prod_id="p1",
        path="/shared/output/p1",
        final_product=True,
        autoclean=True,
    )
    prod_no_autoclean = _make_mock_product(
        mocker,
        prod_id="p2",
        path="/shared/output/p2",
        final_product=True,
        autoclean=False,
    )

    payload = mocker.Mock()
    payload.io = mocker.Mock()
    payload.io.output_products = [prod_autoclean, prod_no_autoclean]

    await run_processor.fn(
        env=mocker.Mock(),
        processor="mockup",
        payload=payload,
        cluster_info=CLUSTER_INFO,
        s3_payload_run="s3://bucket/payload.yaml",
        input_products=[],
    )

    mock_clean.assert_called_once()
    paths_called = mock_clean.call_args[0][0]
    assert "/shared/output/p1" in paths_called
    assert "/shared/output/p2" not in paths_called


@pytest.mark.asyncio
async def test_paths_to_delete_deduplicates_same_path(mocker):
    """
    Verify that run_processor does not add the same filesystem path twice to
    paths_to_delete even when multiple output products share the same path
    and all have autoclean=True.
    """
    _setup_run_processor_mocks(mocker)
    mock_clean = mocker.patch("rs_workflows.dpr_flow.clean_paths")

    shared_path = "/shared/output/common"
    prod_1 = _make_mock_product(mocker, prod_id="p1", path=shared_path, final_product=True, autoclean=True)
    prod_2 = _make_mock_product(mocker, prod_id="p2", path=shared_path, final_product=False, autoclean=True)

    payload = mocker.Mock()
    payload.io = mocker.Mock()
    payload.io.output_products = [prod_1, prod_2]

    await run_processor.fn(
        env=mocker.Mock(),
        processor="mockup",
        payload=payload,
        cluster_info=CLUSTER_INFO,
        s3_payload_run="s3://bucket/payload.yaml",
        input_products=[],
    )

    mock_clean.assert_called_once()
    paths_called = mock_clean.call_args[0][0]
    assert paths_called.count(shared_path) == 1


@pytest.mark.asyncio
async def test_paths_to_delete_empty_when_no_autoclean(mocker):
    """
    Verify that clean_paths is called with an empty list when no output product
    has autoclean=True, i.e. no cleanup is scheduled.
    """
    _setup_run_processor_mocks(mocker)
    mock_clean = mocker.patch("rs_workflows.dpr_flow.clean_paths")

    prod = _make_mock_product(mocker, prod_id="p1", path="/shared/output/p1", final_product=True, autoclean=False)

    payload = mocker.Mock()
    payload.io = mocker.Mock()
    payload.io.output_products = [prod]

    await run_processor.fn(
        env=mocker.Mock(),
        processor="mockup",
        payload=payload,
        cluster_info=CLUSTER_INFO,
        s3_payload_run="s3://bucket/payload.yaml",
        input_products=[],
    )

    mock_clean.assert_called_once()
    paths_called = mock_clean.call_args[0][0]
    assert paths_called == []


@pytest.mark.asyncio
async def test_paths_to_delete_independent_of_final_product_flag(mocker):
    """
    Verify that autoclean path collection is orthogonal to final_product:
    both final and non-final output products with autoclean=True must have
    their paths included in paths_to_delete.
    """
    _setup_run_processor_mocks(mocker)
    mock_clean = mocker.patch("rs_workflows.dpr_flow.clean_paths")

    prod_final_autoclean = _make_mock_product(
        mocker,
        prod_id="final",
        path="/shared/final",
        final_product=True,
        autoclean=True,
    )
    prod_intermediate_autoclean = _make_mock_product(
        mocker,
        prod_id="intermediate",
        path="/shared/intermediate",
        final_product=False,
        autoclean=True,
    )

    payload = mocker.Mock()
    payload.io = mocker.Mock()
    payload.io.output_products = [prod_final_autoclean, prod_intermediate_autoclean]

    await run_processor.fn(
        env=mocker.Mock(),
        processor="mockup",
        payload=payload,
        cluster_info=CLUSTER_INFO,
        s3_payload_run="s3://bucket/payload.yaml",
        input_products=[],
    )

    mock_clean.assert_called_once()
    paths_called = mock_clean.call_args[0][0]
    assert "/shared/final" in paths_called
    assert "/shared/intermediate" in paths_called
