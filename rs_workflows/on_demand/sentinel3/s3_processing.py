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

"""Orchestrate the complete Sentinel-3 staging and processing chain.

This module provides a parent Prefect flow which coordinates three independent
deployments:

1. stage one Sentinel-3 session from CADIP into the RSPY catalog;
2. process the staged session with the Sentinel-3 Level-0 processor;
3. pass the Level-0 products to the Sentinel-3 OLCI Level-1 processor.

The deployments remain independently observable in Prefect. ``run_deployment``
waits for each child run to reach a terminal state, while ``_ensure_completed``
prevents the next processing stage from starting after a failed, crashed, or
cancelled child run.

Level-0 products are currently exchanged through the unified Prefect variable
``s3-processing-default-setting``. The L0 flow writes a compact list under
``l0.s3_l0_finished`` and this orchestrator converts that list into the
``FlowInputProduct`` representation expected by L1. This bridge avoids reading
a child flow result from pod-local Prefect storage, which is not shared between
Kubernetes flow-run pods.
"""

from collections.abc import Awaitable
from typing import Any, cast

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from prefect.variables import Variable

from rs_workflows.on_demand.common.types import S3_PROCESSING_CONFIGURATION

# Full Prefect deployment names use the ``<flow-name>/<deployment-name>`` form.
# Keeping them as constants makes environment-specific deployment names easy to
# find and change without touching the orchestration logic.
CADIP_STAGING_DEPLOYMENT = "stage-cadip-with-options/On-demand Cadip staging"
S3_L0_DEPLOYMENT = "process-s3-l0/on_demand_S3L0"
S3_L1_OLCI_DEPLOYMENT = "process-s3-l1-olci/on_demand_S3L1OLCI"

# Staging searches the CADIP service through the logical ``cadip`` collection
# and publishes the staged session into the catalog collection consumed by L0.
CADIP_COLLECTION = "cadip"
STAGING_CATALOG_COLLECTION = "AUTOMATED_S3L0_INPUT"

# Every dynamically generated L1 input points to the collection where the L0
# deployment publishes its products.
S3_L0_OUTPUT_COLLECTION = "AUTOMATED_S3L0_OUTPUT"


def _build_l1_input_products(finished_products: list[dict[str, str]]) -> list[dict[str, str]]:
    """Convert compact L0 output mappings into L1 input parameters.

    L0 stores its successful outputs in a compact, JSON-serializable form::

        [{"S03OLCL0_": "S03OLCL0__20200121T045644_....zarr"}]

    L1 expects objects compatible with ``FlowInputProduct``::

        {
            "name": "S03OLCL0_",
            "item_id": "S03OLCL0__20200121T045644_....zarr",
            "collection_name": "AUTOMATED_S3L0_OUTPUT",
        }

    A compact dictionary normally contains one product type and item ID. The
    nested comprehension also behaves predictably if a dictionary contains
    multiple entries: each entry becomes a distinct L1 input product.

    Args:
        finished_products: Compact ``product_type -> catalog item ID`` mappings
            written by the L0 flow.

    Returns:
        Parameters accepted by the ``input_products`` field of
        ``Level1FlowParams``.
    """
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
    """Ensure a child deployment reached Prefect's ``Completed`` state.

    ``run_deployment`` waits for a terminal state by default, but a terminal
    state is not necessarily successful: it may also be Failed, Crashed, or
    Cancelled. Checking the state explicitly makes this parent flow fail at the
    correct boundary and prevents downstream deployments from being launched.

    Args:
        flow_run: Flow-run object returned by ``run_deployment``.
        step: Human-readable stage name included in error messages.

    Raises:
        RuntimeError: If Prefect returned no state or the state is not
            ``Completed``.
    """
    if flow_run.state is None:
        raise RuntimeError(f"{step} deployment completed without a state")
    if not flow_run.state.is_completed():
        raise RuntimeError(
            f"{step} deployment did not complete successfully: "
            f"state={flow_run.state.name!r}, message={flow_run.state.message!r}",
        )


@flow(name="full-s3-processing-chain")
async def process_s3(session_id: str, owner_identifier: str = "opadeanu") -> None:
    """Run CADIP staging, S3 L0, and S3 OLCI L1 for one session.

    The flow is deliberately sequential. Each deployment starts only after its
    predecessor has completed successfully:

    ``CADIP staging -> S3 L0 -> S3 OLCI L1``

    Parameters configured on the child deployments remain in effect. This
    orchestrator overrides only the run-specific values: the session and owner
    for staging, the session for L0, and the generated L0 inputs for L1.

    Args:
        session_id: Sentinel-3 CADIP session identifier, for example
            ``S3A_20200121061417020456``.
        owner_identifier: Owner used by ``FlowEnvArgs`` during staging. It
            selects the appropriate user credentials and catalog namespace.

    Raises:
        RuntimeError: If a child deployment does not complete successfully or
            L0 does not publish a valid ``l0.s3_l0_finished`` list.
    """
    logger = get_run_logger()

    # Stage the raw CADIP session first. Other required staging parameters are
    # stable for this environment, while ``session_identifier`` changes for
    # every parent-flow run.
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

    # The S3 L0 deployment reads its processor configuration from the ``l0``
    # section of the unified Prefect variable. It receives only the staged
    # session ID from this parent flow.
    logger.info("CADIP staging completed; starting S3 L0 deployment for session %s", session_id)
    l0_run = await run_deployment(
        name=S3_L0_DEPLOYMENT,
        parameters={"session": session_id},
        flow_run_name=f"s3-l0-{session_id}",
    )
    _ensure_completed(l0_run, "S3 L0")

    # L0 writes compact output references into the unified variable after DPR
    # and catalog publication succeed. We use this JSON bridge because child
    # deployment results otherwise live in pod-local ``~/.prefect/storage`` and
    # cannot be read from the separate orchestrator pod.
    s3_settings = await cast(Awaitable[Any], Variable.get(S3_PROCESSING_CONFIGURATION, default={}))
    l0_settings = s3_settings.get("l0") if isinstance(s3_settings, dict) else None
    finished_products = l0_settings.get("s3_l0_finished") if isinstance(l0_settings, dict) else None
    if not isinstance(finished_products, list):
        raise RuntimeError(
            f"Prefect variable {S3_PROCESSING_CONFIGURATION!r} does not contain a valid " "'l0.s3_l0_finished' list",
        )

    # Convert catalog product references into Level1FlowParams.input_products.
    # Passing ``flow_params`` here overrides only these dynamic inputs; all
    # remaining L1 defaults are resolved from ``common`` + ``l1`` by the child.
    l1_input_products = _build_l1_input_products(finished_products)
    logger.info("S3 L0 completed with %d products; starting S3 OLCI L1 deployment", len(l1_input_products))
    l1_run = await run_deployment(
        name=S3_L1_OLCI_DEPLOYMENT,
        parameters={"flow_params": {"input_products": l1_input_products}},
        flow_run_name=f"s3-l1-olci-{session_id}",
    )
    _ensure_completed(l1_run, "S3 OLCI L1")

    # Reaching this log means all three independently deployed flows completed
    # successfully. Any exception above marks this parent run as failed.
    logger.info("S3 processing completed successfully for session %s", session_id)
