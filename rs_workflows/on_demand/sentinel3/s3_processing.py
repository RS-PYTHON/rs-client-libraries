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

"""Orchestrate Sentinel-3 staging, Level-0, and OLCI Level-1 deployments."""

from collections.abc import Awaitable
from typing import Any, cast

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from prefect.variables import Variable

CADIP_STAGING_DEPLOYMENT = "stage-cadip-with-options/On-demand Cadip staging"
S3_L0_DEPLOYMENT = "process-s3-l0/on_demand_S3L0"
S3_L1_OLCI_DEPLOYMENT = "process-s3-l1-olci/on_demand_S3L1OLCI"
CADIP_COLLECTION = "cadip"
STAGING_CATALOG_COLLECTION = "AUTOMATED_S3L0_INPUT"
S3_L0_SETTINGS_VARIABLE = "s3-l0-default-setting"
S3_L0_OUTPUT_COLLECTION = "AUTOMATED_S3L0_OUTPUT"


def _build_l1_input_products(finished_products: list[dict[str, str]]) -> list[dict[str, str]]:
    """Convert compact L0 product mappings into Level-1 flow input products."""
    return [
        {
            "name": product_type,
            "item_id": item_id,
            "collection_name": S3_L0_OUTPUT_COLLECTION,
        }
        for product in finished_products
        for product_type, item_id in product.items()
    ]


def _ensure_completed(flow_run: Any, step: str) -> None:
    """Raise when a deployment run did not finish successfully."""
    if flow_run.state is None:
        raise RuntimeError(f"{step} deployment completed without a state")
    if not flow_run.state.is_completed():
        raise RuntimeError(
            f"{step} deployment did not complete successfully: "
            f"state={flow_run.state.name!r}, message={flow_run.state.message!r}"
        )


@flow(name="full-s3-processing-chain")
async def process_s3(session_id: str, owner_identifier: str = "opadeanu") -> None:
    """Run CADIP staging, S3 L0, and S3 OLCI L1 sequentially for one session."""
    logger = get_run_logger()

    logger.info("Starting CADIP staging deployment for session %s", session_id)
    staging_run = await run_deployment(
        name=CADIP_STAGING_DEPLOYMENT,
        parameters={
            "env": {"owner_id": owner_identifier},
            "cadip_collection_identifier": CADIP_COLLECTION,
            "session_identifier": session_id,
            "catalog_collection_identifier": STAGING_CATALOG_COLLECTION,
        },
        flow_run_name=f"stage-{session_id}",
    )
    _ensure_completed(staging_run, "CADIP staging")

    logger.info("CADIP staging completed; starting S3 L0 deployment for session %s", session_id)
    l0_run = await run_deployment(
        name=S3_L0_DEPLOYMENT,
        parameters={"session": session_id},
        flow_run_name=f"s3-l0-{session_id}",
    )
    _ensure_completed(l0_run, "S3 L0")

    l0_settings = await cast(Awaitable[Any], Variable.get(S3_L0_SETTINGS_VARIABLE, default={}))
    finished_products = l0_settings.get("s3_l0_finished") if isinstance(l0_settings, dict) else None
    if not isinstance(finished_products, list):
        raise RuntimeError(
            f"Prefect variable {S3_L0_SETTINGS_VARIABLE!r} does not contain a valid 's3_l0_finished' list"
        )

    l1_input_products = _build_l1_input_products(finished_products)
    logger.info("S3 L0 completed with %d products; starting S3 OLCI L1 deployment", len(l1_input_products))
    l1_run = await run_deployment(
        name=S3_L1_OLCI_DEPLOYMENT,
        parameters={"flow_params": {"input_products": l1_input_products}},
        flow_run_name=f"s3-l1-olci-{session_id}",
    )
    _ensure_completed(l1_run, "S3 OLCI L1")
    logger.info("S3 processing completed successfully for session %s", session_id)
