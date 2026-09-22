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

"""Emit dummy S3 L1 and L2 quicklook-inputs-ready events without processing products."""

import time
from typing import Any

from prefect import flow, get_run_logger, runtime
from prefect.events import emit_event

L1_EVENT_NAME = "rs-python.s3-l1.quicklook-inputs-ready"
L2_EVENT_NAME = "rs-python.s3-l2.quicklook-inputs-ready"


def emit_quicklook_event(event_name: str, level: str, payload: dict[str, Any]) -> None:
    """Emit one quicklook-inputs-ready event, with the resource id matched by the automations."""
    flow_run_id = str(runtime.flow_run.id or "unknown")
    event = emit_event(
        event=event_name,
        resource={
            "prefect.resource.id": f"rs-python.s3-{level}-result.{flow_run_id}",
            "prefect.resource.name": f"S3 OLCI {level.upper()} products",
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
        raise RuntimeError(f"Quicklook-inputs-ready event was not emitted: {event_name}")
    get_run_logger().info("Emitted event=%s, event_id=%s, payload=%s", event_name, event.id, payload)


@flow(name="dummy-s3-l1-l2-olci-events")
def emit_s3l1_l2_olci_dummy_events(
    owner_id: str = "alin",
    l1_collection_name: str = "TEST_OUTPUT_OLCI_S3L1",
    l2_collection_name: str = "TEST_OUTPUT_OLCI_S3L2",
    delay_seconds: int = 10,
) -> dict[str, Any]:
    """Emit the L1 event, wait, then emit the L2 event; return both payloads for inspection."""
    l1_payload = {
        "owner_id": owner_id,
        "published_items": [
            {"id": "S03OLCERR_20250612T020913_0359_A046_T9A3.zarr", "collection": l1_collection_name},
            {"id": "S03OLCEFR_20250612T020913_0359_A046_T65D.zarr", "collection": l1_collection_name},
        ],
    }
    l2_payload = {
        "owner_id": owner_id,
        "published_items": [
            {
                "id": "S03OLCLFR_20260910T140001_20260910T140301_20260910T161553_0179_143_381_1620_PS1_O_NR_003.zarr",
                "collection": l2_collection_name,
            },
            {
                "id": "S03OLCLFR_20260910T140301_20260910T140601_20260910T161553_0179_143_381_1800_PS1_O_NR_003.zarr",
                "collection": l2_collection_name,
            },
            {"id": "S03OLCLFR_20250612T020913_0359_A046_TCA3.zarr", "collection": l2_collection_name},
        ],
    }
    emit_quicklook_event(L1_EVENT_NAME, "l1", l1_payload)
    time.sleep(delay_seconds)
    emit_quicklook_event(L2_EVENT_NAME, "l2", l2_payload)
    return {"l1": l1_payload, "l2": l2_payload}


if __name__ == "__main__":
    emit_s3l1_l2_olci_dummy_events()
