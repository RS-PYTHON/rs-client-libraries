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

"""sentinel 3 OLCI Level-1 processing."""

from typing import Any

from prefect import flow, get_run_logger, task
from pystac import Item

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs, FlowInputProduct
from rs_workflows.on_demand.common.types import Level1FlowParams
from rs_workflows.on_demand.sentinel3.s3_processing_utils import (
    build_olci_l1_input_products,
    read_s3_orchestration_settings,
)
from rs_workflows.utils.dpr import call_dpr_flow

OLCI_L0_PRODUCT_TYPE = "S03OLCL0_"
NAV_L0_PRODUCT_TYPE = "S03NATL0_"


async def build_olci_l1_inputs_from_catalog(
    owner_identifier: str,
    input_collection: str,
    timestamp: str,
) -> list[FlowInputProduct]:
    """Build OLCI L1 inputs by searching published L0 products in the catalog.

    This is an alternative, currently unused, to passing the persisted L0 flow
    result through shared storage. ``timestamp`` must be a STAC datetime value
    or interval (for example ``2026-06-30T11:40:00Z/2026-06-30T12:30:00Z``)
    that identifies the relevant L0 processing window. It prevents products
    belonging to older sessions in the same collection from being selected.

    The OLCI L1 processor expects the first three OLCI L0 products and the
    first navigation L0 product, ordered by catalog item ID.

    Args:
        owner_identifier: Catalog owner used to initialize the RS client.
        input_collection: Catalog collection where L0 published its products.
        timestamp: STAC datetime or interval used to restrict both searches.

    Returns:
        Three ``S3OLCIL0_*`` inputs followed by one ``S3NAVL0_1`` input.

    Raises:
        ValueError: If the catalog contains fewer than three matching OLCI
            products or no matching navigation product.
    """
    logger = get_run_logger()
    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    catalog_client = flow_env.rs_client.get_catalog_client()

    def search_product_type(product_type: str) -> list[Item]:
        logger.info(
            "Searching catalog collection=%r for product_type=%r, timestamp=%r",
            input_collection,
            product_type,
            timestamp,
        )
        found = catalog_client.search(
            method="POST",
            owner_id=owner_identifier,
            collections=[input_collection],
            timestamp=timestamp,
            stac_filter={"op": "=", "args": [{"property": "product:type"}, product_type]},
            max_items=100,
            limit=100,
        )
        items = sorted(found.items, key=lambda item: item.id) if found else []
        logger.info("Catalog search found %d product(s) for product_type=%r", len(items), product_type)
        return items

    olci_items = search_product_type(OLCI_L0_PRODUCT_TYPE)
    nav_items = search_product_type(NAV_L0_PRODUCT_TYPE)

    if len(olci_items) < 3:
        raise ValueError(f"Expected at least 3 {OLCI_L0_PRODUCT_TYPE} catalog products, found {len(olci_items)}")
    if not nav_items:
        raise ValueError(f"Expected at least 1 {NAV_L0_PRODUCT_TYPE} catalog product, found 0")

    input_products = [
        FlowInputProduct(
            name=f"S3OLCIL0_{index}",
            item_id=item.id,
            collection_name=input_collection,
        )
        for index, item in enumerate(olci_items[:3], start=1)
    ]
    input_products.append(
        FlowInputProduct(
            name="S3NAVL0_1",
            item_id=nav_items[0].id,
            collection_name=input_collection,
        ),
    )
    logger.info("Built OLCI L1 catalog inputs: %r", input_products)
    return input_products


@flow(
    name="process-s3-l1-olci",
    flow_run_name="s3-l1-olci-from-{source_l0_run_id}",
)
async def process_s3l1_olci(
    flow_params: Level1FlowParams | None = None,
    l0_products: list[dict[str, Any]] | None = None,
    source_l0_run_id: str = "manual",  # pylint: disable=unused-argument
) -> list[dict[str, Any]]:
    """
    Sentinel-3 OLCI L1 processing.
    The input_products should have been processed before by L0.

    ``l0_products`` is the raw product list emitted by S3 L0. When supplied by
    a Prefect Automation, it is converted here into the four processor inputs
    expected by OLCI L1. ``source_l0_run_id`` provides a short upstream
    reference used in the L1 flow-run name.
    """
    mission = "3"
    # how to use s3-l1-default-setting
    flow_parameters = await (flow_params or Level1FlowParams()).resolve(mission)

    if l0_products is not None:
        orchestration_settings = await read_s3_orchestration_settings()
        prepared_inputs = build_olci_l1_input_products(
            l0_products,
            orchestration_settings.s3_l0_output_collection,
        )
        flow_parameters.input_products = [FlowInputProduct.model_validate(product) for product in prepared_inputs]
        get_run_logger().info(
            "Built %d S3 L1 input product(s) from %d raw L0 product(s) received from Automation",
            len(flow_parameters.input_products),
            len(l0_products),
        )

    # Alternative catalog-based input resolution (currently paused; do not enable yet):
    #
    # - owner_identifier comes from the resolved ``common.owner_identifier`` setting;
    # - start_datetime and end_datetime come from the resolved ``l1`` settings or
    #   from explicit ``flow_params`` overrides supplied by the orchestrator;
    # - input_collection comes from
    #   ``s3-processing-default-setting.orchestration.s3_l0_output_collection``.
    #
    # raw_settings = await Variable.get("s3-processing-default-setting", default={})
    # input_collection = raw_settings["orchestration"]["s3_l0_output_collection"]
    # start_datetime = flow_parameters.start_datetime.isoformat().replace("+00:00", "Z")
    # end_datetime = flow_parameters.end_datetime.isoformat().replace("+00:00", "Z")
    # timestamp = f"{start_datetime}/{end_datetime}"
    # catalog_input_products = await build_olci_l1_inputs_from_catalog(
    #     owner_identifier=flow_parameters.owner_identifier,
    #     input_collection=input_collection,
    #     timestamp=timestamp,
    # )
    #
    # To switch from shared-result inputs to catalog inputs, pass
    # ``catalog_input_products`` to ``call_dpr_flow`` below instead of
    # ``flow_parameters.input_products``.

    # Call DPR flow
    return await call_dpr_flow(
        FlowEnvArgs(owner_id=flow_parameters.owner_identifier),
        input_products=flow_parameters.input_products,
        external_variables={
            "start_datetime": flow_parameters.start_datetime,
            "end_datetime": flow_parameters.end_datetime,
            "satellite": flow_parameters.satellite,
        },
        dask_cluster_label=flow_parameters.dask_cluster_label,
        processor_name=flow_parameters.processor_name,
        processor_version=flow_parameters.processor_version,
        pipeline=flow_parameters.pipeline,
        unit=flow_parameters.unit,
        priority=flow_parameters.priority,
        processing_mode=flow_parameters.processing_mode,
        workflow=flow_parameters.workflow,
        generated_product_to_collection_identifier=flow_parameters.generated_product_to_collection_identifier or [],
        auxiliary_product_to_collection_identifier=flow_parameters.auxiliary_product_to_collection_identifier or [],
        logging_level=flow_parameters.logging_level,
    )


@task(name="process-s3-l1-olci")
async def process_s3l1_olci_task(*args, **kwargs) -> list[dict[str, Any]]:
    """See: dpr_processing"""
    return await process_s3l1_olci.fn(*args, **kwargs)
