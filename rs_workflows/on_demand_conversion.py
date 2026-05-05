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

"""Prefect flows and tasks for on-demand SAFE -zarr conversion."""

from prefect import flow, get_run_logger, task
from rs_workflows.flow_utils import FlowEnv, ConversionIn, RetryConfig



@flow
async def on_demand_conversion(
    conversion_input: ConversionIn,
    retry_config: RetryConfig = RetryConfig(),
):
    logger = get_run_logger()
    logger.info(f"Starting on-demand conversion flow with input: {conversion_input}")
    # 1. stage
    # 2. unzip if needed
    # 3. compute ptype and other metadata
    # 4. convert to zarr
    # 5. upload to S3
    # 6. post/ put to catalog
    # 7. cleanup (legacy files, staging area)
    logger.info("On-demand conversion flow completed successfully.")