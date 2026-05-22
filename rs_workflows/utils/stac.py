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

from prefect import get_run_logger
from pystac import ItemCollection

from rs_client.rs_client import RsClient
from rs_client.stac.stac_base import StacBase
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


async def search(
    env: FlowEnvArgs,
    cql2: dict,
    span_name: str,
    stac_client_getter: Callable[[RsClient], StacBase],
    error_if_empty: bool = False,
) -> ItemCollection | None:
    """
    Search items in a STAC catalogue.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        cql2: CQL2 filter read from the processor tasktable.
        stac_client_getter: Function receiving rs_client and returning a StacBase.
        error_if_empty: Raise a ValueError if the results are empty.
    """
    logger = get_run_logger()

    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, span_name):

        logger.info(f"Start STAC search using CQL2: {cql2}")
        stac_client: StacBase = stac_client_getter(flow_env.rs_client)
        found = stac_client.search(
            method="POST",
            stac_filter=cql2.get("filter"),
            max_items=cql2.get("limit"),
            sortby=cql2.get("sortby"),
        )
        if (not found) and error_if_empty:
            raise ValueError(
                f"No item found for CQL2: {json.dumps(cql2, indent=2)}",
            )
        logger.info(f"STAC search found {len(found)} result(s): {found.to_dict()}")
        return found
