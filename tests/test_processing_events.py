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

"""Unit tests for the events of the processing chains (product and quicklook events)."""

from unittest.mock import MagicMock

import pytest

from rs_workflows.flow_utils import WorkflowType
from rs_workflows.on_demand.common import events
from rs_workflows.on_demand.sentinel3 import s3_processing_utils


def _product(product_type, item_id):
    """A published STAC item, with assets and links that must not be sent in the events."""
    return {
        "id": item_id,
        "collection": product_type.lower(),
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
        "properties": {
            "product:type": product_type,
            "platform": "sentinel-3a",
            "start_datetime": "2026-10-01T10:00:00Z",
            "end_datetime": "2026-10-01T10:03:00Z",
        },
        "assets": {"product": {"href": "s3://bucket/product.zarr"}},
        "links": [{"rel": "self", "href": "https://catalog/item"}],
    }


PRODUCTS = [_product("S03OLCL0_", "olcl0-item"), _product("S03NATL0_", "natl0-item")]


def test_product_generated_event_name():
    """The event name contains the product type."""
    assert events.product_generated_event_name("S03OLCEFR") == "product.S03OLCEFR.generated"


@pytest.mark.parametrize("emitted", [True, False])
@pytest.mark.parametrize("workflow", list(WorkflowType))
def test_emit_product_generated_events(mocker, emitted, workflow):
    """One event is emitted per product, related to the workflow, with the owner and the STAC item without its assets
    and links."""
    mocker.patch.object(events.runtime.flow_run, "id", "flow-run-id")
    emit_event = mocker.patch.object(events, "emit_event", return_value=MagicMock(id="event-id") if emitted else None)
    logger = mocker.patch.object(events, "get_logger").return_value

    events.emit_product_generated_events("owner", PRODUCTS, workflow)

    assert [call.kwargs for call in emit_event.call_args_list] == [
        {
            "event": f"product.{product['properties']['product:type']}.generated",
            "resource": {
                "prefect.resource.id": "prefect.flow-run.flow-run-id",
                "prefect.resource.name": product["properties"]["product:type"],
            },
            "related": [
                {"prefect.resource.id": workflow.value, "prefect.resource.role": "workflow"},
                {"prefect.resource.id": "prefect.tag.processing", "prefect.resource.role": "tag"},
                {"prefect.resource.id": "prefect.tag.sentinel-3a", "prefect.resource.role": "tag"},
            ],
            "payload": {
                "owner_id": "owner",
                "product": {
                    "id": product["id"],
                    "geometry": product["geometry"],
                    "collection": product["collection"],
                    "properties": product["properties"],
                },
            },
        }
        for product in PRODUCTS
    ]
    assert (logger.info if emitted else logger.warning).call_count == len(PRODUCTS)


def test_emit_product_generated_events_without_flow_run(mocker):
    """Outside of a flow run, the events are related to an unknown flow run."""
    mocker.patch.object(events.runtime.flow_run, "id", None)
    emit_event = mocker.patch.object(events, "emit_event")

    events.emit_product_generated_events("owner", PRODUCTS[:1], WorkflowType.ON_DEMAND)

    assert emit_event.call_args.kwargs["resource"]["prefect.resource.id"] == "prefect.flow-run.unknown"
    assert emit_event.call_args.kwargs["related"][0] == {
        "prefect.resource.id": "on-demand",
        "prefect.resource.role": "workflow",
    }


def test_emit_olci_quicklook_event_without_products(mocker):
    """No quicklook is requested when no product was generated."""
    emit_event = mocker.patch.object(s3_processing_utils, "emit_event")

    s3_processing_utils.emit_olci_quicklook_event("l1", "owner", [])

    emit_event.assert_not_called()


@pytest.mark.parametrize("level", ["l1", "l2"])
@pytest.mark.parametrize("emitted", [True, False])
def test_emit_olci_quicklook_event(mocker, level, emitted):
    """The quicklooks of all the generated products are requested, linked to the current flow run."""
    mocker.patch.object(s3_processing_utils.runtime.flow_run, "id", "flow-run-id")
    emit_event = mocker.patch.object(
        s3_processing_utils,
        "emit_event",
        return_value=MagicMock(id="event-id") if emitted else None,
    )
    logger = mocker.patch.object(s3_processing_utils, "get_logger").return_value

    s3_processing_utils.emit_olci_quicklook_event(level, "owner", PRODUCTS)

    emit_event.assert_called_once_with(
        event=f"rs-python.s3-{level}.quicklook-inputs-ready",
        resource={
            "prefect.resource.id": f"rs-python.s3-{level}-result.flow-run-id",
            "prefect.resource.name": f"S3 OLCI {level.upper()} products",
        },
        related=[{"prefect.resource.id": "prefect.flow-run.flow-run-id", "prefect.resource.role": "flow-run"}],
        payload={
            "owner_id": "owner",
            "published_items": [
                {"id": "olcl0-item", "collection": "s03olcl0_"},
                {"id": "natl0-item", "collection": "s03natl0_"},
            ],
        },
    )
    (logger.info if emitted else logger.warning).assert_called_once()
