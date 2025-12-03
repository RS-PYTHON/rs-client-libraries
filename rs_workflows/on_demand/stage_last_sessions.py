# Copyright 2025 Airbus defence And Space
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

"""Staging flow implementation"""

import json
from datetime import datetime, timedelta, timezone
from enum import Enum

from prefect import flow, get_run_logger, pause_flow_run, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, Field
from pystac import ItemCollection  # type: ignore

from rs_client.stac.cadip_client import CadipClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@task(name="create result artifact")
async def create_result_artifact(
    cadip_items: str,
    catalog_cadip_collection: str,
    duration: timedelta
) -> None:
    """

    """
    duration_str = str(duration)
    markdown_report = f"""# Staging result

| Parameter        | Value |
|:--------------|----------------------------:|
| Session id  | {cadip_items}     |
| Duration    | {duration_str}    |

"""
    await create_markdown_artifact(
        key="result",
        markdown=markdown_report,
        description="session staging output"
    )


@task(name="Cadip session search")
async def cadip_session_search(
    env: FlowEnvArgs,
    cadip_collection_identifier: str,
    start_datetime: str,
    end_datetime: str,
) -> ItemCollection:
    """
    Search for CADIP sessions within a given time interval.

    Parameters:
        env:
            Flow environment arguments (e.g., owner_id, credentials).
        cadip_collection_identifier:
            CADIP collection identifier (e.g., "s1_sgs") to specify the station.
        start_datetime:
            Start of the search interval in ISO 8601 format (string).
        end_datetime:
            End of the search interval in ISO 8601 format (string).

    Raises:
        ValueError:
            If start_datetime or end_datetime is not provided.

    Returns:
        ItemCollection:
            A pystac ItemCollection containing the sessions found.
    """
    logger = get_run_logger()

    # Initialize flow environment and telemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "cadip-search"):

        cadip_client: CadipClient = flow_env.rs_client.get_cadip_client()

        # Validate input datetimes
        if not start_datetime or not end_datetime:
            raise ValueError("start_datetime or end_datetime is not set properly")

        # Build CQL2 query for temporal intersection
        cadip_cql2_query = {
            "filter": {
                "op": "t_intersects",
                "args": [
                    {"property": "datetime"},
                    {"interval": [start_datetime, end_datetime]},
                ],
            },
            "limit": 10,
            "sortby": [{"field": "datetime", "direction": "desc"}],
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


@task(name="Cadip session stage")
async def cadip_session_stage(
    env: FlowEnvArgs,
    cadip_items: ItemCollection | str,
    catalog_cadip_collection: str,
) -> None:
    """
    Stage CADIP items into the target catalog collection.

    Parameters:
        env:
            Flow environment arguments (owner, credentials, etc.).
        cadip_items:
            Either a pystac.ItemCollection or a JSON string representing CADIP items.
        catalog_cadip_collection:
            Target catalog collection identifier where sessions will be staged.

    Returns:
        None. Side effects include triggering staging jobs and logging their status.
    """

    # Initialize flow environment and telemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "staging"):

        logger = get_run_logger()

        # Get staging client from environment
        staging_client = flow_env.rs_client.get_staging_client()

        # Convert ItemCollection into dictionary for staging
        if isinstance(cadip_items, ItemCollection):
            cadip_items = cadip_items.to_dict()

        # Trigger staging and wait for jobs to finish
        job_all_status = staging_client.run_staging(cadip_items, catalog_cadip_collection)
        staging_client.wait_for_jobs(
            job_all_status,
            logger,
            poll_interval=2,  # Poll every 2 seconds
        )


def make_session_enum(values: dict[str, str]) -> Enum:
    """
    Create a dynamic Enum class from a dictionary of session values.

    This function takes a dictionary mapping session identifiers to display names
    and returns a new Enum class with the mapping inverted (values become enum names,
    keys become enum values).

    Args:
        values: A dictionary where keys are session identifiers and values are
                session names/labels to be used as enum member names.

    Returns:
        A dynamically created Enum class where enum member names correspond to
        the input dictionary values, and enum member values correspond to the
        input dictionary keys.

    """
    return Enum("session_enum", {v: k for k, v in values.items()})  # type: ignore


class CadipCollections(str, Enum):
    """
    Enumeration of available CADIP (Copernicus Acquisition Data Information Processing) collections.

    This enum defines the supported satellite data collection identifiers that can be queried
    from the CADIP service, including collections from Sentinel-1, Sentinel-2, and Sentinel-3 missions.
    """

    S1_SGS = "s1_sgs"
    S1_MPS = "s1_mps"
    S1_MTI = "s1_mti"
    S2_SGS = "s2_sgs"
    S3_SGS = "s3_sgs"


@flow(name="select and stage a session")
async def stage_selected_session(cadip_collection: CadipCollections, owner_identifier: str = "pcuq"):
    """
    Stage a selected CADIP session for processing.

    This function searches for CADIP sessions within a 10-hour window from the current UTC time,
    presents the user with a list of available sessions, and stages the selected session for processing.

    Args:
        cadip_collection (CadipCollections): The CADIP collection to search within.
        owner_identifier (str, optional): The owner identifier for the flow environment. Defaults to "pcuq".

    Raises:
        ValueError: If no CADIP session is found within the specified time window.

    Returns:
        None
    """
    logger = get_run_logger()

    # Current time in UTC
    end_datetime: datetime = datetime.now(timezone.utc)

    # Go back 10 hours
    start_datetime: datetime = end_datetime - timedelta(hours=10)

    # Format timestamps in ISO 8601 with Z suffix
    start_str = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Search for CADIP sessions in the given time window
    session_found = await cadip_session_search(
        FlowEnvArgs(owner_id=owner_identifier),
        cadip_collection_identifier=cadip_collection,
        start_datetime=start_str,
        end_datetime=end_str,
    )

    if not session_found:
        raise ValueError(
            f"No Cadip session found for start_datetime={start_datetime!r} and end_datetime={end_datetime!r}",
        )

    # Build dictionary of sessions with descriptive keys
    session_list: dict[str, str] = {}
    for item_ in session_found.items:
        key = f"📡 {item_.id} 🕒 {item_.properties['published']} 🌍 {item_.properties['sat:absolute_orbit']}"
        session_list[key] = item_.id

    # Generate Enum dynamically from session list
    session_enum = make_session_enum(session_list)

    # Pydantic model for Prefect pause input
    class SessionSelection(BaseModel):
        """

        Args:
            BaseModel (_type_): _description_
        """

        selected: session_enum = Field(title="Session to stage")  # type: ignore

    # Pause Prefect flow to let user select a session
    selection = await pause_flow_run(wait_for_input=SessionSelection)

    logger.info(f"Internal identifier: {session_list[selection.selected.value]}")  # type: ignore

    # Build catalog collection name based on CADIP collection
    sat = cadip_collection[1]
    catalog_cadip_collection = f"s0{sat}-cadip-session"

    # Stage the selected session
    date1 = datetime.now(timezone.utc)
    await cadip_session_stage(
        FlowEnvArgs(owner_id=owner_identifier),
        cadip_items=f"https://rspy.ops.rs-python.eu/cadip/search?ids=\
            {session_list[selection.selected.value]}",  # type: ignore
        catalog_cadip_collection=catalog_cadip_collection,
    )
    date2 = datetime.now(timezone.utc)
    await create_result_artifact(session_list[selection.selected.value], catalog_cadip_collection, date2 - date1)