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

"""STAC utilities"""

import json
from collections.abc import Callable
from typing import Any

from prefect import get_run_logger
from pystac import ItemCollection

from rs_client.stac.stac_base import StacBase
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs

# Selects the STAC client to use and any source-specific search arguments.
StacClientSelector = Callable[[FlowEnv], tuple[StacBase, dict[str, Any]]]


async def search(
    env: FlowEnvArgs,
    cql2: dict,
    span_name: str,
    stac_client_selector: StacClientSelector,
    error_if_empty: bool = False,
    start_log_message: str | None = None,
) -> ItemCollection | None:
    """
    Search items in a STAC catalogue.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        cql2: CQL2 filter read from the processor tasktable.
        span_name: Name of the OpenTelemetry span.
        stac_client_selector: Function receiving the flow environment and returning the STAC client
            plus source-specific search keyword arguments.
        error_if_empty: Raise a ValueError if the results are empty.
        start_log_message: Optional search start log message.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, span_name):

        logger.info(start_log_message or f"Start STAC search using CQL2: {cql2}")
        stac_client, search_kwargs = stac_client_selector(flow_env)
        found = stac_client.search(
            method="POST",
            stac_filter=cql2.get("filter"),
            max_items=cql2.get("limit"),
            collections=cql2.get("collections"),
            sortby=cql2.get("sortby"),
            timestamp=cql2.get("timestamp"),
            **search_kwargs,
        )
        if (not found) and error_if_empty:
            raise ValueError(
                f"No item found for CQL2: {json.dumps(cql2, indent=2)}",
            )
        logger.info(f"STAC search found {len(found)} result(s): {found.to_dict()}")
        return found
