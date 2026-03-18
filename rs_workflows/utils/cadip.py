# Copyright 2023-2026 Airbus
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

"""Helper task to interact with the rs-cadip."""

from rs_workflows.flow_utils import FlowEnv
from prefect import get_run_logger, task
import json
from pystac import ItemCollection
from rs_client.stac.cadip_client import CadipClient
import json
from datetime import datetime, timedelta, timezone

from prefect import (
    get_run_logger,
    task,
)

from rs_client.stac.cadip_client import CadipClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


def get_first_eviction_datetime(item) -> str | None:
    for asset in item.assets.values():
        if "eviction_datetime" in asset.extra_fields:
            return asset.extra_fields["eviction_datetime"]
    return None



@task(name="Search the cadip station that has got a session")
async def get_cadip_station(flow_env: FlowEnv, session: str, cadip_collections : list[str]) -> str|None:
    """ """
    logger = get_run_logger()
    result = None
    
    # Initialize flow environment and telemetry span
    cadip_client: CadipClient = flow_env.rs_client.get_cadip_client()

    # Log query for debugging
    logger.info(f"Search a cadip station between [{', '.join(cadip_collections)}] looking for the session'{session}'")

    # Execute search request
    item_col:ItemCollection = cadip_client.search(
        method="GET",
        ids=[session],
        collections=cadip_collections,
        max_items=1,
        limit=1,
    )    

    if len(item_col) == 1:
        # Check that the session has not been evicted
        eviction_date_str = get_first_eviction_datetime(item_col[0])
        eviction_date:datetime = datetime.fromisoformat(eviction_date_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if eviction_date <= now:
            logger.error(f"❌ The session '{session}' has been evicted (evicition date = {eviction_date_str}) ")
        else:        
            # Extract of the station name
            collection_links = [link for link in item_col[0].links if link.rel == "collection"]
            if collection_links:
                href = collection_links[0].href
                result = href.rstrip("/").split("/")[-1]
                logger.info(f"✔️ The session '{session}' is available at station {result}")
    if result is None:
        logger.info(f"❌ The session '{session}' can not be found on stations [{', '.join(cadip_collections)}]")

    return result



@task(name="Cadip session search")
async def cadip_session_search(env: FlowEnvArgs, cadip_collection_identifier: list[str], limit: int = 10) -> ItemCollection:
    """
    Search for CADIP sessions within a given time interval.

    Parameters:
        env:
            Flow environment arguments (e.g., owner_id, credentials).
        cadip_collection_identifier:
            CADIP collection identifier (e.g., "s1_sgs") to specify the station.
        limit:
            Number maximum of STAC items to be retrieved

    Returns:
        ItemCollection:
            A pystac ItemCollection containing the sessions found.
    """
    logger = get_run_logger()

    # Initialize flow environment and telemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "cadip-search"):

        cadip_client: CadipClient = flow_env.rs_client.get_cadip_client()

        # Current time in UTC
        end_datetime: datetime = datetime.now(timezone.utc)

        # Go back 10 hours
        start_datetime: datetime = end_datetime - timedelta(hours=10)

        # Format timestamps in ISO 8601 with Z suffix
        start_str = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_str = end_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        # Validate input datetimes
        if not start_str or not end_str:
            raise ValueError("start_datetime or end_datetime is not set properly")

        # Build CQL2 query for temporal intersection
        cadip_cql2_query = {
            "filter": {
                "op": "t_intersects",
                "args": [
                    {"property": "published"},
                    {"interval": [start_str, end_str]},
                ],
            },
            "limit": limit,
            "sortby": [{"field": "published", "direction": "desc"}],
        }

        # Log query for debugging
        logger.info(f"CQL2 query={json.dumps(cadip_cql2_query, indent=2)}")
        logger.info("Start request on CADIP station")

        # Execute search request
        found = cadip_client.search(
            method="POST",
            collections=[cadip_collection_identifier],
            stac_filter=cadip_cql2_query.get("filter"),
            max_items=cadip_cql2_query.get("limit"),
            sortby=cadip_cql2_query.get("sortby"),
        )

        return found

