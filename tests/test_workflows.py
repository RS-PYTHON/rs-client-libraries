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

"""Test the Prefect workflows"""

import os
from collections import defaultdict
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from prefect.blocks.system import Secret

from rs_client.rs_client import RsClient
from rs_common import prefect_utils
from rs_workflows import (
    auxip_flow,
    cadip_flow,
    catalog_flow,
    dpr_flow,
    on_demand_processing,
    staging_flow,
)
from rs_workflows.flow_utils import FlowEnvArgs, ProcessorEnum

OWNER_ID = "OWNER_ID"
S3_PAYLOAD = "S3_PAYLOAD"
RSPY_WEBSITE = "RSPY_WEBSITE"
RSPY_APIKEY = "RSPY_APIKEY"

# Recursive defaultdict, see: https://stackoverflow.com/a/8702435
MOCK_DICT = lambda: defaultdict(MOCK_DICT)  # type: ignore # pylint: disable=unnecessary-lambda-assignment # noqa: E731

#########
# Mocks #
#########


class MockStr(Mock):
    """Mock str"""

    def split(self, *_, **__):
        """Mock str split"""
        return ["split1", "split2"]


class MockRsClient(Mock):
    """Mock RsClient class"""

    # Mocked properties
    id = "id"
    assets = {"asset1": MockStr(), "asset2": MockStr()}
    properties = {
        "prop1": MockStr(),
        "prop2": MockStr(),
    }

    def search(self, *_, **__):
        """Mock stac search"""
        return [MockRsClient()] * 2

    def get_items(self, *_, **__):
        """Mock stac get_items"""
        return [MockRsClient()] * 2

    def get_collections(self, *_, **__):
        """Mock stac get_collections"""
        return [MockRsClient()] * 2

    def wait_for_job(self, *_, **__):
        """Mock DprClient wait_for_job"""
        return [MOCK_DICT()] * 2


############
# DPR FLOW #
############


async def mock_s3_download_file(
    s3_path: str,
    to_path: str | Path | None,
    **__: dict[str, Any],
) -> Path:
    """Mock the prefect_utils.s3_download_file function"""
    if not to_path:
        return Path()

    # Mock the downloading of S3_PAYLOAD
    if s3_path.startswith(S3_PAYLOAD):
        with open(to_path, "w", encoding="utf-8") as opened:
            opened.write(
                """
workflow:
- name: workflow_name
  module: workflow_module
  processing_unit: workflow_processing_unit
  outputs:
    out1: output1
    out2: output2
""",
            )

    return Path(to_path)


#############
# MAIN FLOW #
#############


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(RsClient, "get_auxip_client", MockRsClient)
@patch.object(RsClient, "get_cadip_client", MockRsClient)
@patch.object(RsClient, "get_catalog_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(RsClient, "get_dpr_client", MockRsClient)
@patch.object(catalog_flow, "datetime", Mock())
async def test_on_demand_processing(mocker, mock_prefect):  # pylint: disable=unused-argument
    """Test the on_demand_processing flow"""

    # Create prefect block
    await Secret(
        value={  # type: ignore[arg-type]
            "RSPY_WEBSITE": RSPY_WEBSITE,
            "RSPY_APIKEY": RSPY_APIKEY,
        },
    ).save(
        prefect_utils.format_env_user(OWNER_ID),
        overwrite=True,
    )

    # We'll just check that the prefect tasks and flows were called.
    # We don't check the underlying RsClient functions, this is already done in dedicated pytests.
    spied = {
        mocker.spy(prefect_function, "fn"): call_count  # spy on <flow>.fn or <task>.fn = the underlying python function
        for prefect_function, call_count in {
            auxip_flow.search: 1,
            auxip_flow.search_task: 1,
            cadip_flow.search: 1,
            cadip_flow.search_task: 1,
            dpr_flow.read_payload_values: 1,
            dpr_flow.read_tasktable: 1,
            dpr_flow.write_payload: 1,
            dpr_flow.run_processor: 1,
            staging_flow.staging_task_auxip: 1,
            staging_flow.staging_task_cadip: 1,
            staging_flow.staging: 2,
            catalog_flow.publish: 1,
        }.items()
    }

    # Run the prefect flow
    await on_demand_processing.on_demand_processing(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor=ProcessorEnum.S1L0,
        cadip_collection_identifier="cadip_collection_identifier",
        session_identifier="session_identifier",
        catalog_collection_identifier="catalog_collection_identifier",
        s3_payload_template=S3_PAYLOAD,
        s3_output_data="s3_output_data",
        use_dpr_mockup=False,
    )

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(RsClient, "get_cadip_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(catalog_flow, "datetime", Mock())
async def test_on_demand_cadip_staging(mocker, mock_prefect):  # pylint: disable=unused-argument
    """Test the on_demand_cadip_staging flow"""

    # Create prefect block
    await Secret(
        value={  # type: ignore[arg-type]
            "RSPY_WEBSITE": RSPY_WEBSITE,
            "RSPY_APIKEY": RSPY_APIKEY,
        },
    ).save(
        prefect_utils.format_env_user(OWNER_ID),
        overwrite=True,
    )

    # We'll just check that the prefect tasks and flows were called.
    # We don't check the underlying RsClient functions, this is already done in dedicated pytests.
    spied = {
        mocker.spy(prefect_function, "fn"): call_count  # spy on <flow>.fn or <task>.fn = the underlying python function
        for prefect_function, call_count in {
            cadip_flow.search: 1,
            cadip_flow.search_task: 1,
            staging_flow.staging_task_cadip: 1,
            staging_flow.staging: 1,
        }.items()
    }

    # Run the prefect flow
    await on_demand_processing.on_demand_cadip_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        cadip_collection_identifier="cadip_collection_identifier",
        session_identifier="session_identifier",
        catalog_collection_identifier="catalog_collection_identifier",
    )

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count


@patch.dict(os.environ, {}, clear=False)  # don't modify os.environ outside this test
@patch.object(prefect_utils, "s3_download_file", mock_s3_download_file)
@patch.object(prefect_utils, "s3_upload_file", AsyncMock())
@patch.object(RsClient, "get_auxip_client", MockRsClient)
@patch.object(RsClient, "get_staging_client", MockRsClient)
@patch.object(catalog_flow, "datetime", Mock())
async def test_on_demand_auxip_staging(mocker, mock_prefect):  # pylint: disable=unused-argument
    """Test the on_demand_auxip_staging flow"""

    # Create prefect block
    await Secret(
        value={  # type: ignore[arg-type]
            "RSPY_WEBSITE": RSPY_WEBSITE,
            "RSPY_APIKEY": RSPY_APIKEY,
        },
    ).save(
        prefect_utils.format_env_user(OWNER_ID),
        overwrite=True,
    )

    # We'll just check that the prefect tasks and flows were called.
    # We don't check the underlying RsClient functions, this is already done in dedicated pytests.
    spied = {
        mocker.spy(prefect_function, "fn"): call_count  # spy on <flow>.fn or <task>.fn = the underlying python function
        for prefect_function, call_count in {
            auxip_flow.search: 1,
            auxip_flow.search_task: 1,
            staging_flow.staging_task_auxip: 1,
            staging_flow.staging: 1,
            on_demand_processing.filter_product_type: 1,
        }.items()
    }

    # Run the prefect flow
    await on_demand_processing.on_demand_auxip_staging(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        start_datetime="2024-05-27T09:44:09.509000Z",
        end_datetime="2024-05-27T09:44:19.509000Z",
        product_type="AUX_PP2",
        catalog_collection_identifier="catalog_collection_identifier",
    )

    # Check calls
    for fn, call_count in spied.items():
        assert fn.await_count == call_count
