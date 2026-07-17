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

from typing import Any

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

CADIP_STAGING_DEPLOYMENT = "stage-cadip-with-options/On-demand Cadip staging"
S3_L0_DEPLOYMENT = "process-s3-l0/on_demand_S3L0"
S3_L1_OLCI_DEPLOYMENT = "process-s3-l1-olci/on_demand_S3L1OLCI"
CADIP_COLLECTION = "cadip"
STAGING_CATALOG_COLLECTION = "AUTOMATED_S3L0_INPUT"


def _build_l1_input_products(published_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Convert published L0 STAC items into Level-1 flow input products."""
    return [
        {
            "name": item["properties"]["product:type"],
            "item_id": item["id"],
            "collection_name": item["collection"],
        }
        for item in published_items
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
    assert l0_run.state is not None
    l0_products = await l0_run.state.result()
    if not isinstance(l0_products, list):
        raise RuntimeError(f"S3 L0 deployment returned {type(l0_products).__name__}, expected a list")

    l1_input_products = _build_l1_input_products(l0_products)
    logger.info("S3 L0 completed with %d products; starting S3 OLCI L1 deployment", len(l1_input_products))
    l1_run = await run_deployment(
        name=S3_L1_OLCI_DEPLOYMENT,
        parameters={"flow_params": {"input_products": l1_input_products}},
        flow_run_name=f"s3-l1-olci-{session_id}",
    )
    _ensure_completed(l1_run, "S3 OLCI L1")
    logger.info("S3 processing completed successfully for session %s", session_id)
