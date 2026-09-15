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

"""Emit a dummy S3 L1 products-ready event without processing products."""

from typing import Any

from prefect import flow, get_run_logger, runtime
from prefect.events import emit_event

from rs_workflows.on_demand.common.events import products_ready_event_name


@flow(name="dummy-s3-l1-olci-event")
def emit_s3l1_olci_dummy_event(
    owner_id: str = "abutu",
    collection_name: str = "TEST_FLOW_1145",
) -> dict[str, Any]:
    """Emit the published-items payload and return it for inspection."""
    payload = {
        "owner_id": owner_id,
        "published_items": [
            {"id": "S03OLCERR_20250612T020913_0359_A046_T9A3.zarr", "collection": collection_name},
            {"id": "S03OLCEFR_20250612T020913_0359_A046_T65D.zarr", "collection": collection_name},
        ],
    }
    flow_run_id = str(runtime.flow_run.id or "unknown")
    event_name = products_ready_event_name(mission="3", level="1")
    event = emit_event(
        event=event_name,
        resource={
            "prefect.resource.id": f"rs-python.s3-l1-result.{flow_run_id}",
            "prefect.resource.name": "S3 OLCI L1 products",
        },
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{flow_run_id}",
                "prefect.resource.role": "flow-run",
            },
        ],
        payload=payload,
    )
    if event is None:
        raise RuntimeError(f"Products-ready event was not emitted: {event_name}")
    get_run_logger().info("Emitted event=%s, event_id=%s, payload=%s", event_name, event.id, payload)
    return payload


if __name__ == "__main__":
    emit_s3l1_olci_dummy_event()
