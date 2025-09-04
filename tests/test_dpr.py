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

"""Test rs-client-libraries dpr functions"""


import getpass

import pytest
import responses
from starlette import status

from rs_client.ogcapi.dpr_client import DprClient, DprProcess
from rs_client.rs_client import RsClient

RS_SERVER_API_KEY = "RS_SERVER_API_KEY"
OWNER_ID = getpass.getuser()


@pytest.fixture(name="dummy_href")
def get_dummy_href():
    """
    Dummy href for local_mode
    """
    dummy_href = "https://DUMMY_HREF"
    return dummy_href


@pytest.fixture(name="dpr_client")
def get_dpr_client(dummy_href) -> DprClient:
    """Create a dpr client

    Args:
        href (str): rs_server href for local-mode

    Returns:
        DprClient: DprClient instance
    """
    client = RsClient(
        rs_server_href=dummy_href,
        rs_server_api_key=RS_SERVER_API_KEY,
        owner_id=OWNER_ID,
        logger=None,
    )
    return client.get_dpr_client()


@pytest.fixture(name="dpr_response_sample")
def get_dpr_response_sample() -> dict:
    """
    Return sample rs-dpr-service response.
    """
    return {
        "status": "running",
        "message": "Processor execution started",
        "processID": "dpr-service",
        "progress": 0,
        "type": "process",
        "created": "2025-09-04T09:41:26Z",
        "started": "2025-09-04T09:41:26Z",
        "updated": "2025-09-04T09:41:26Z",
        "jobID": "f4efad68-e198-4a08-b6ee-de67d497ca31",
    }


@responses.activate
@pytest.mark.parametrize("process", [DprProcess.MOCKUP, DprProcess.S1L0])
def test_dpr_client(mocker, dpr_client: DprClient, process: DprProcess, dummy_href: str, dpr_response_sample: dict):
    """Test nominal DPR service response"""

    # Mock response from DPR service
    responses.add(
        method=responses.POST,
        url=f"{dummy_href}/dpr/processes/{process.value}/execution",
        json=dpr_response_sample,
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
    assert dpr_client.run_process(process, "", "", "", {}) == dpr_response_sample
