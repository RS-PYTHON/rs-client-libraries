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

"""common Level-0 processing."""

from prefect import get_run_logger
from pystac import Item

from rs_workflows.flow_utils import (
    FlowEnv,
    FlowEnvArgs,
    FlowInputProduct,
)
from rs_workflows.on_demand.common.types import Level0FlowParams
from rs_workflows.utils.catalog import get_single_catalog_item
from rs_workflows.utils.dpr import call_dpr_flow


async def process_l0_last_steps(
    mission: str,
    session: str,
    flow_params: Level0FlowParams,
    input_products: list[FlowInputProduct],
    verbose: bool,
):
    """
    Final processing steps that are common to all missions.
    Raises:
        ValueError: _description_
    """
    logger = get_run_logger()
    logger.info(
        "Starting L0 last steps: mission=%r, session=%r, verbose=%r, input_products_count=%d",
        mission,
        session,
        verbose,
        len(input_products),
    )
    logger.info("Raw flow_params: %r (type=%s)", flow_params, type(flow_params).__name__)
    for index, input_product in enumerate(input_products):
        logger.info(
            "Input product [%d]: %r (type=%s)",
            index,
            input_product,
            type(input_product).__name__,
        )

    # Resolve parameters
    flow_params = flow_params or Level0FlowParams()
    try:
        p = await flow_params.resolve(mission)
    except Exception:
        logger.exception("Failed to resolve flow parameters for mission=%r", mission)
        raise

    logger.info("Resolved flow parameters: %r (type=%s)", p, type(p).__name__)

    flow_env = FlowEnv(FlowEnvArgs(owner_id=p.owner_identifier))
    logger.info("Created FlowEnv for owner_id=%r", p.owner_identifier)

    with flow_env.start_span(__name__, f"sentinel{mission}-level0-processing"):
        logger.info(
            "Looking up catalog session: session=%r, collection=%r",
            session,
            p.session_collection,
        )
        try:
            item_session: Item | None = await get_single_catalog_item(flow_env, session, [p.session_collection])
        except Exception:
            logger.exception(
                "Catalog lookup failed: session=%r, collection=%r",
                session,
                p.session_collection,
            )
            raise

        if not item_session:
            logger.error("❌ Session %r was not found; DPR processing cannot be launched.", session)
            return
        logger.info(f"✅ The session {session} has been found in the catalog.")
        logger.info("Catalog item id=%r, properties=%r", item_session.id, item_session.properties)

        # Satellite identifier
        satellite_value = f"sentinel-{mission}{session[2].lower()}"

        # Call DPR flow
        dpr_env = FlowEnvArgs(owner_id=p.owner_identifier)
        dpr_parameters = {
            "input_products": input_products,
            "external_variables": {
                "start_datetime": p.start_datetime,
                "end_datetime": p.end_datetime,
                "satellite": satellite_value,
            },
            "dask_cluster_label": p.dask_cluster_label,
            "processor_name": p.processor_name,
            "processor_version": p.processor_version,
            "pipeline": p.pipeline,
            "unit": p.unit,
            "priority": p.priority,
            "processing_mode": p.processing_mode,
            "workflow": p.workflow,
            "generated_product_to_collection_identifier": p.generated_product_to_collection_identifier or [],
            "auxiliary_product_to_collection_identifier": p.auxiliary_product_to_collection_identifier or [],
            "logging_level": p.logging_level,
        }
        logger.info("About to call call_dpr_flow with env=%r (type=%s)", dpr_env, type(dpr_env).__name__)

        for parameter_name, parameter_value in dpr_parameters.items():
            logger.info(
                "call_dpr_flow parameter %s=%r (type=%s)",
                parameter_name,
                parameter_value,
                type(parameter_value).__name__,
            )

        try:
            s3_l0_result = await call_dpr_flow(
                dpr_env,
                input_products=input_products,
                external_variables={
                    "start_datetime": p.start_datetime,
                    "end_datetime": p.end_datetime,
                    "satellite": satellite_value,
                },
                dask_cluster_label=p.dask_cluster_label,
                processor_name=p.processor_name,
                processor_version=p.processor_version,
                pipeline=p.pipeline,
                unit=p.unit,
                priority=p.priority,
                processing_mode=p.processing_mode,
                workflow=p.workflow,
                generated_product_to_collection_identifier=(p.generated_product_to_collection_identifier or []),
                auxiliary_product_to_collection_identifier=(p.auxiliary_product_to_collection_identifier or []),
                logging_level=p.logging_level,
            )
        except Exception:
            logger.exception(
                "call_dpr_flow failed for mission=%r, session=%r, processor=%r:%r",
                mission,
                session,
                p.processor_name,
                p.processor_version,
            )
            raise
        logger.info("call_dpr_flow completed successfully for mission=%r, session=%r", mission, session)

        logger.info(
            "S3 L0 products prepared for persisted flow result: count=%d, products=%r",
            len(s3_l0_result),
            s3_l0_result,
        )

        return s3_l0_result
