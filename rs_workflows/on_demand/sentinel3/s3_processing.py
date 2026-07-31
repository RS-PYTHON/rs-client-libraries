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

Level-0 products are exchanged through Prefect's persisted flow result. The L0
deployment writes its return value to a shared-disk ``LocalFileSystem`` block,
and this orchestrator reads that result before converting it into the
``FlowInputProduct`` representation expected by L1. The shared storage makes
the result available across the separate Kubernetes flow-run pods.
"""

from typing import Any

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

from rs_workflows.on_demand.common.types import S3_PROCESSING_CONFIGURATION
from rs_workflows.on_demand.sentinel3.s3_processing_utils import (
    build_olci_l1_input_products,
    read_s3_orchestration_settings,
)


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


async def _run_child_deployment(
    step: str,
    *,
    name: str,
    parameters: dict[str, Any],
    flow_run_name: str,
) -> Any:
    """Run a child deployment and return only after successful completion."""
    flow_run = await run_deployment(
        name=name,
        parameters=parameters,
        flow_run_name=flow_run_name,
    )
    _ensure_completed(flow_run, step)
    return flow_run


@flow(name="full-s3-processing-chain")
async def process_s3(session_id: str) -> None:
    """Run CADIP staging, S3 L0, and S3 OLCI L1 for one session.

    The flow is deliberately sequential. Each deployment starts only after its
    predecessor has completed successfully:

    ``CADIP staging -> S3 L0 -> S3 OLCI L1``

    Parameters configured on the child deployments remain in effect. This
    orchestrator overrides only the run-specific values: the session for
    staging and L0, and the generated L0 inputs for L1.

    Args:
        session_id: Sentinel-3 CADIP session identifier, for example
            ``S3A_20200121061417020456``.

    Raises:
        RuntimeError: If a child deployment does not complete successfully or
            L0 does not return a valid product list.
    """
    logger = get_run_logger()
    settings = await read_s3_orchestration_settings()
    logger.info("Loaded S3 orchestration settings from Prefect variable %s", S3_PROCESSING_CONFIGURATION)

    # Stage the raw CADIP session first. Other required staging parameters are
    # stable for this environment, while ``session_identifier`` changes for
    # every parent-flow run.
    logger.info("Starting CADIP staging deployment for session %s", session_id)
    await _run_child_deployment(
        "CADIP staging",
        name=settings.cadip_staging_deployment,
        parameters={
            "cadip_collection_identifier": settings.cadip_collection,
            "session_identifier": session_id,
            "catalog_collection_identifier": settings.staging_catalog_collection,
        },
        flow_run_name=f"stage-{session_id}",
    )

    # The S3 L0 deployment reads its processor configuration from the ``l0``
    # section of the unified Prefect variable. It receives only the staged
    # session ID from this parent flow.
    logger.info("CADIP staging completed; starting S3 L0 deployment for session %s", session_id)
    l0_run = await _run_child_deployment(
        "S3 L0",
        name=settings.s3_l0_deployment,
        parameters={"session": session_id},
        flow_run_name=f"s3-l0-{session_id}",
    )

    # L0 persists its return value on the shared disk configured as Prefect
    # result storage, so this orchestrator can load it from its separate pod.
    logger.info("Loading persisted S3 L0 result for flow_run_id=%s", l0_run.id)
    s3_l0_result = await l0_run.state.result()
    if not isinstance(s3_l0_result, list):
        raise RuntimeError(f"S3 L0 returned {type(s3_l0_result).__name__}, expected a list of products")
    logger.info("Loaded %d products from the persisted S3 L0 result", len(s3_l0_result))

    # Convert catalog product references into Level1FlowParams.input_products.
    # Passing ``flow_params`` here overrides only these dynamic inputs; all
    # remaining L1 defaults are resolved from ``common`` + ``l1`` by the child.
    l1_input_products = build_olci_l1_input_products(s3_l0_result, settings.s3_l0_output_collection)
    source_l0_run_id = str(l0_run.id)[:8]
    logger.info("S3 L0 completed with %d products; starting S3 OLCI L1 deployment", len(l1_input_products))
    await _run_child_deployment(
        "S3 OLCI L1",
        name=settings.s3_l1_olci_deployment,
        parameters={
            "flow_params": {"input_products": l1_input_products},
            "source_l0_run_id": source_l0_run_id,
        },
        flow_run_name=f"s3-l1-olci-from-{source_l0_run_id}",
    )

    # Reaching this log means all three independently deployed flows completed
    # successfully. Any exception above marks this parent run as failed.
    logger.info("S3 processing completed successfully for session %s", session_id)
