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

"""Common events of the processing chains."""

from typing import Any

from prefect import runtime
from prefect.events import emit_event

from rs_workflows.flow_utils import WorkflowType
from rs_workflows.utils.prefect import get_logger

PRODUCT_GENERATED_EVENT = "product.{product_type}.generated"


def product_generated_event_name(product_type: str) -> str:
    """Return the name of the event emitted for a generated product of the given type."""
    return PRODUCT_GENERATED_EVENT.format(product_type=product_type)


def emit_product_generated_events(
    owner_id: str,
    products: list[dict[str, Any]],
    workflow: WorkflowType,
) -> None:
    """
    Emit a `product.<product type>.generated` event for each generated product.

    The events are related to the workflow of the processing that generated the products (`workflow` role): the
    automations of the systematic processing start the next processing of the chain only from the events of a
    `systematic` workflow. Their payload contains the owner and the STAC item of the product, without its assets and
    links, because the Prefect event payloads are limited in size.
    """
    logger = get_logger()
    flow_run_id = str(runtime.flow_run.id or "unknown")
    workflow_name = WorkflowType(workflow).value
    for product in products:
        product_type = product["properties"].get("product:type")
        if not product_type:
            logger.warning(f"⚠️ No event emitted for product '{product['id']}' without product type")
            continue
        event_name = product_generated_event_name(product_type)
        related = [
            {
                "prefect.resource.id": workflow_name,
                "prefect.resource.role": "workflow",
            },
            {"prefect.resource.id": "prefect.tag.processing", "prefect.resource.role": "tag"},
        ]
        if platform := product["properties"].get("platform"):
            related.append({"prefect.resource.id": f"prefect.tag.{platform}", "prefect.resource.role": "tag"})
        event = emit_event(
            event=event_name,
            resource={
                "prefect.resource.id": f"prefect.flow-run.{flow_run_id}",
                "prefect.resource.name": product_type,
            },
            related=related,
            payload={
                "owner_id": owner_id,
                "product": {
                    "id": product["id"],
                    "geometry": product["geometry"],
                    "collection": product["collection"],
                    "properties": product["properties"],
                },
            },
        )

        if event is None:
            logger.warning(f"⚠️ Event '{event_name}' was not emitted")
        else:
            logger.info(f"Event '{event_name}' (id='{event.id}') emitted.")
