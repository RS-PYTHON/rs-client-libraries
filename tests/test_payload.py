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

"""Test whole payload generation using payload_builder.py, payload_generator.py, the storage configuration, ..."""

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
import pytest_responses  # pylint: disable=unused-import # noqa: F401 # used to avoid adding @responses.activate
import yaml
from prefect import flow

from rs_common.logging import Logging
from rs_workflows import (
    on_demand_processing,
)
from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnv,
    FlowEnvArgs,
    RetryConfig,
)
from rs_workflows.payload_generator import RSPY_CATALOG_BUCKET
from tests.conftest import (
    MOCKED_BUCKET,
    OWNER_ID,
)
from tests.test_utils import setup_worklow_test_env

CONFIG_DIR = Path(__file__).parent / "resources/test_payload"
DASK_CLUSTER_LABEL = "DASK_CLUSTER_LABEL"

logger = Logging.default(__name__)


#########
# Tests #
#########


@pytest.mark.parametrize(
    "mocked_stac_catalog_get_collection",
    [
        [
            # test case 1
            "ax___fro_ax",
            "ax___osf_ax",
            "s01-cadip-session",
            # test case 2 and 3
            "oper_mpl_orbsct",
            "S1A-aux-GIP_TILPAR",
            "S1A-aux-None_ETA__AX",
        ],
    ],
    indirect=True,
    ids=[""],
)
@pytest.mark.parametrize(
    "mocked_stac_catalog_search_inside_collection",
    [
        [
            "auxip",
            [
                "catalog",
                [
                    # test case 1
                    "ax___fro_ax",
                    "ax___osf_ax",
                    "s01-cadip-session",
                    # test case 2 and 3
                    "oper_mpl_orbsct",
                    "S1A-aux-GIP_TILPAR",
                    "S1A-aux-None_ETA__AX",
                ],
            ],
            "edh",
        ],
    ],
    indirect=True,
    ids=[""],
)
@pytest.mark.parametrize(
    "storage_configuration",
    [CONFIG_DIR / "storage-test.json"],
    indirect=True,
    ids=[""],
)
@pytest.mark.parametrize(
    "dpr_params_filename,tasktable_filename,payload_filename",
    [
        ["parameter-call-test1.json", "tasktable-test1.json", "case1"],
        ["parameter-call-test2.json", "tasktable-test2.json", "case2"],
    ],
    ids=["case1", "case2"],
)
async def test_whole_payload(
    request,
    mocker,
    dpr_params_filename: str,  # file that contains input parameters for executing the 'dpr-process' flow
    tasktable_filename: str,  # file that contains the tasktable
    payload_filename: str,  # file that contains the generated payload
    mocked_rspy_landing_pages,  # /auxip, /cadip, /catalog, /...
    mocked_stac_catalog_get_collection,  # /catalog/collections[/...]
    mocked_stac_catalog_search_inside_collection,  # /auxip/search[/...], /catalog/search[/...]
    mocked_staging_response,  # /processes/staging/execution, /jobs/{job_id}
    storage_configuration,
    _mock_os_env,
):  # pylint: disable=unused-argument
    """Test whole payload generation"""

    # Mocks
    mocker.patch(
        "rs_workflows.payload_generator.fetch_csv_from_endpoint",
        return_value=[["*", "*", "*", "90", RSPY_CATALOG_BUCKET]],
    )
    mocker.patch(
        "rs_workflows.payload_generator.resolve_stac_input_path",
        return_value=(None, f"s3://{MOCKED_BUCKET}/S1CADUS"),
    )
    await setup_worklow_test_env()

    # Read input parameters
    with open(CONFIG_DIR / dpr_params_filename, encoding="utf-8") as opened:
        params = json.load(opened)
    dpr_input = DprProcessIn(
        env=FlowEnvArgs(owner_id=OWNER_ID),
        processor_name="mockup",
        processor_version="1.0",
        dask_cluster_label=DASK_CLUSTER_LABEL,
        start_datetime=datetime(2023, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 10, 3, 11, 0, 0, tzinfo=timezone.utc),
        satellite="S1A",
        s3_payload_file=f"s3://{MOCKED_BUCKET}/payload.yaml",
        **params,
    )

    # Read tasktable
    with open(str(CONFIG_DIR / tasktable_filename), encoding="utf-8") as opened:
        task_table = json.load(opened)

    @flow(name="process-generic")
    async def from_a_flow():
        """Build and generate the payload file from a prefect flow"""
        payload_task, source_items = on_demand_processing.build_and_generate_payload(
            logger,
            flow_env=FlowEnv(dpr_input.env),
            task_table=task_table,
            dpr_input=dpr_input,
            retry_config=RetryConfig(staging_retries=0),
        )
        return payload_task.result(), source_items

    payload, _source_items = await from_a_flow()
    payload_dict = payload.dump(reveal_secrets=True)

    # Remove random uuids from output paths
    for product in payload_dict["io"]["output_products"]:
        path = product["path"]
        try:
            UUID(Path(path).stem)
            product["path"] = str(Path(path).parent / "00000000-0000-0000-0000-000000000001")
        except ValueError:  # not an uuid
            pass

    # Write result
    with open(CONFIG_DIR / f"generated-payload-{payload_filename}.yml", "w", encoding="utf-8") as opened:
        opened.write(yaml.dump(payload_dict, default_flow_style=False, sort_keys=False))

    # Compare with reference
    with open(CONFIG_DIR / f"reference-payload-{payload_filename}.yml", encoding="utf-8") as opened:
        reference = yaml.safe_load(opened)
    assert payload_dict == reference
