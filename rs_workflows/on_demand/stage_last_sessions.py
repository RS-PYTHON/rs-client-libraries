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
from urllib.parse import urlencode

from prefect import flow, get_run_logger, pause_flow_run, task
from prefect.artifacts import acreate_link_artifact, acreate_markdown_artifact
from pydantic import BaseModel, Field
from pystac import ItemCollection

from rs_client.stac.cadip_client import CadipClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


@task(name="create result artifact")
async def create_result_artifact(cadip_items: str, duration: timedelta) -> None:
    """
    Creates a result artifact in the form of a markdown report and a link to a Grafana dashboard.
    Parameters:
        cadip_items (str): A string representing the session ID or items to be included in the report.
        duration (timedelta): The duration of the session, used to display the time span in the report.
    Returns:
        None: This function does not return any value. It performs asynchronous operations to create artifacts.
    """
    duration_str = str(duration)
    markdown_report = f"""# Staging result

| Parameter        | Value |
|:--------------|:----------------------------|
| Session id  | {cadip_items}     |
| Duration    | {duration_str}    |

"""
    await acreate_markdown_artifact(key="result", markdown=markdown_report, description="session staging output")

    # Base Grafana URL
    base_url = "https://monitoring.ops.rs-python.eu/d/1a2758bd-a984-4dc8-9a6a-ee7694526850/2-stac-requests"

    # Calculate start and end datetimes
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=3)

    # ISO 8601 formatting with milliseconds and Z suffix
    def to_iso_z(dt: datetime) -> str:
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    params = {"from": to_iso_z(start_time), "to": to_iso_z(end_time)}

    # Build the encoded URL
    url = f"{base_url}?{urlencode(params)}"
    await acreate_link_artifact(key="grafana-dashboard", link=url, description="# see session item from the catalog")


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
    cadip_search_url: str,
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

        # Trigger staging and wait for jobs to finish
        job_all_status = staging_client.run_staging(cadip_search_url, catalog_cadip_collection)
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
    return Enum("session_enum", {v: k for k, v in values.items()})


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
    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    with flow_env.start_span(__name__, "stage_selected_session"):
        logger = get_run_logger()

        # Current time in UTC
        end_datetime: datetime = datetime.now(timezone.utc)

        # Go back 10 hours
        start_datetime: datetime = end_datetime - timedelta(hours=10)

        # Format timestamps in ISO 8601 with Z suffix
        start_str = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_str = end_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        # Search for CADIP sessions in the given time window
        session_found = cadip_session_search.submit(
            flow_env.serialize(),
            cadip_collection_identifier=cadip_collection,
            start_datetime=start_str,
            end_datetime=end_str,
        ).result()

        if not session_found:
            raise ValueError(
                f"No Cadip session found for start_datetime={start_datetime!r} and end_datetime={end_datetime!r}",
            )

        # Build dictionary of sessions with descriptive keys
        session_list: dict[str, str] = {}
        for item_ in session_found.items:  # type: ignore[attr-defined]
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

        selected_session: str = session_list[selection.selected.value]  # type: ignore
        logger.info(f"Internal identifier: {selected_session}")

        # Build catalog collection name based on CADIP collection
        sat = cadip_collection[1]
        catalog_cadip_collection = f"s0{sat}-cadip-session"

        # URL to search the STAC ItemCollection
        cadip_client = flow_env.rs_client.get_cadip_client()
        cadip_search_url = f"{cadip_client.href_service}/search?ids={selected_session}"

        # Stage the selected session
        date1 = datetime.now(timezone.utc)
        result_staging = cadip_session_stage.submit(
            flow_env.serialize(),
            cadip_search_url=cadip_search_url,
            catalog_cadip_collection=catalog_cadip_collection,
        )
        result_staging.result()  # type: ignore[unused-coroutine]
        date2 = datetime.now(timezone.utc)
        result_artifact = create_result_artifact.submit(
            selected_session,
            date2 - date1,
        )
        result_artifact.result()  # type: ignore[unused-coroutine]
