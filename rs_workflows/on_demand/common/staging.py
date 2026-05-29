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

"""staging flows."""

from datetime import datetime, timedelta, timezone
from enum import Enum
from urllib.parse import urlencode, urlparse
from venv import logger

from prefect import (
    apause_flow_run,
    flow,
    get_run_logger,
    task,
)
from prefect.artifacts import (
    acreate_link_artifact,
    acreate_markdown_artifact,
)
from pydantic import BaseModel, Field
from pystac import (
    Collection,
    Extent,
    SpatialExtent,
    TemporalExtent,
)

from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs
from rs_workflows.utils.artifact_verbose import ReportManager
from rs_workflows.utils.cadip import cadip_session_search


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
    artifact_key_name: str = "staging-result"
    await acreate_markdown_artifact(
        key=artifact_key_name,
        markdown=markdown_report,
        description="session staging output",
    )
    logger.info(f"📌 Artifact named '{artifact_key_name}' has been linked to this flow.")

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
    artifact_key_name: str = "monitoring-url"
    await acreate_link_artifact(key=artifact_key_name, link=url, description="# see session item from the catalog")
    logger.info(f"📌 Artifact named '{artifact_key_name}' has been linked to this flow.")


@task(name="Cadip session stage")
async def cadip_session_stage(env: FlowEnvArgs, cadip_search_url: str, catalog_cadip_collection: str) -> str:
    """
    Stage CADIP items into the target catalog collection.

    Parameters:
        env:
            Flow environment arguments (owner, credentials, etc.).
        cadip_search_url:
            URL for the item to be stagged.
        catalog_cadip_collection:
            Target catalog collection identifier where sessions will be staged.
    """

    # Initialize flow environment and telemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "staging"):

        logger = get_run_logger()

        # Get staging client from environment
        staging_client = flow_env.rs_client.get_staging_client()

        # Trigger staging and wait for jobs to finish
        logger.info(f"Start staging URL'{cadip_search_url}' on collection '{catalog_cadip_collection}'.")
        job_all_status = staging_client.run_staging(cadip_search_url, catalog_cadip_collection)
        result = staging_client.wait_for_jobs(
            job_all_status,
            logger,
            poll_interval=2,  # Poll every 2 seconds
        )
        parsed = urlparse(cadip_search_url)
        hostname = parsed.hostname
        return result[hostname].get("status", "")


def make_session_enum(values: dict[str, str]) -> Enum:
    """
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
    S1_NSG = "s1_nsg"
    S1_PAR = "s1_par"
    S1_INS = "s1_ins"
    S1_KSE = "s1_kse"
    S2_PAR = "s2_par"
    S2_SGS = "s2_sgs"
    S2_INS = "s2_ins"
    S2_KSE = "s2_kse"
    S3_SGS = "s3_sgs"


@flow(name="stage-cadip-selection")
async def stage_selected_session(cadip_collection: CadipCollections, owner_identifier: str = "copernicus"):
    """
    Stage a selected CADIP session for processing.

    This function searches for CADIP sessions within a 10-hour window from the current UTC time,
    presents the user with a list of available sessions, and stages the selected session for processing.

    Args:
        cadip_collection (CadipCollections): The CADIP collection to search within.
        owner_identifier (str, optional): The owner identifier for the flow environment. Defaults to "copernicus".

    Raises:
        ValueError: If no CADIP session is found within the specified time window.

    Returns:
        None
    """
    # Init flow environment and opentelemetry span
    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    with flow_env.start_span(__name__, "stage_selected_session"):
        logger = get_run_logger()

        # Search for CADIP sessions in the given time window
        session_found = cadip_session_search.submit(
            flow_env.serialize(),
            cadip_collection_identifier=[cadip_collection],
        ).result()

        if not session_found:
            raise ValueError(
                "No Cadip session found.",
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
        selection = await apause_flow_run(wait_for_input=SessionSelection)
        selected_session: str = session_list[selection.selected.value]  # type: ignore

        if selected_session is not None:
            logger.info(f"Session to be stagged: {selected_session}")
            # Build catalog collection name based on CADIP collection
            await stage_session_common(flow_env, cadip_collection, selected_session)
        else:
            logger.info("No session has been found.")


@flow(name="stage-cadip-latest")
async def stage_latest_session(
    cadip_collection: CadipCollections,
    owner_identifier: str = "copernicus",
    verbose: bool = False,
):
    """
    Stage the latest CADIP session from a selected station.

    This function searches for CADIP sessions within a 10-hour window and get the latest.

    Args:
        cadip_collection (CadipCollections): The CADIP collection to search within.
        owner_identifier (str, optional): The owner identifier for the flow environment. Defaults to "copernicus".

    Raises:
        ValueError: If no CADIP session is found within the specified time window.

    Returns:
        None
    """
    report_verbose = ReportManager() if verbose else None

    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    logger = get_run_logger()

    # Search for CADIP sessions in the given time window
    session_found = cadip_session_search.submit(
        flow_env.serialize(),
        cadip_collection_identifier=[cadip_collection],
        limit=1,
    ).result()

    if not session_found:
        if report_verbose is not None:
            report_verbose.failed_step(1, "No session has been found.")
        raise ValueError(
            "No Cadip session found.",
        )

    selected_session: str = session_found[0].id  # type: ignore
    if selected_session is not None:
        if report_verbose is not None:
            report_verbose.success_step(1, f"Session {selected_session} has been found.")
        logger.info(f"Session to be stagged: {selected_session}")
        # Build catalog collection name based on CADIP collection
        await stage_session_common(flow_env, cadip_collection, selected_session, report_verbose)
    else:
        logger.info("No session has been found.")
        if report_verbose is not None:
            report_verbose.failed_step(1, "No session has been found.")
    if report_verbose is not None:
        await report_verbose.push_report("test-report", "Step by step results")


@task(name="stage-cadip")
async def stage_session_common(
    flow_env: FlowEnv,
    cadip_collection: CadipCollections | str,
    selected_session: str,
    report_verbose: ReportManager | None = None,
) -> bool:
    """
    Stage a CADIP session by searching and staging it in the catalog.
    This asynchronous function stages a selected CADIP session by:
    Args:
        flow_env (FlowEnv): The flow environment containing the RS client and configuration.
        cadip_collection (CadipCollections): The CADIP collection identifier (used to determine satellite).
        selected_session (str): The session ID to be staged.
    Returns:
        None
    Raises:
        Exception: Any exceptions raised by the cadip_session_stage or create_result_artifact tasks.
    Notes:
        - The function uses Prefect's task submission pattern with result() calls to wait for completion.
        - Staging duration is calculated and included in the result artifact.
        - Requires an active logger context (from get_run_logger()).
    """
    logger = get_run_logger()

    # Build catalog collection name based on CADIP collection
    sat = cadip_collection[1]
    catalog_cadip_collection = f"s0{sat}-cadip-session"

    # Check that the collection exists. Otherwise create it.
    catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
    try:
        catalog_client.search(collections=[catalog_cadip_collection])
    except RuntimeError:
        # The collection is missing, we will create it
        logger.info(f"The collection {catalog_cadip_collection} is missing; it will be created.")
        spatial = SpatialExtent(bboxes=[[-94.6911621, 37.0332547, -94.402771, 37.1077651]])
        date_strings = ["2000-02-01T00:00:00Z", "2100-02-12T00:00:00Z"]
        date_objects: list[datetime | None] = [
            datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ") for date_str in date_strings
        ]
        temporal = TemporalExtent(intervals=date_objects)
        extent = Extent(spatial=spatial, temporal=temporal)
        new_collection = Collection(
            id=catalog_cadip_collection,
            description=f"{catalog_cadip_collection} collection",
            extent=extent,
        )
        catalog_client.add_collection(new_collection)

    # URL to search the STAC ItemCollection
    cadip_client = flow_env.rs_client.get_cadip_client()

    # Stage the selected session
    date_start = datetime.now(timezone.utc)
    result_staging = cadip_session_stage.submit(
        flow_env.serialize(),
        cadip_search_url=f"{cadip_client.href_service}/search?ids={selected_session}",
        catalog_cadip_collection=catalog_cadip_collection,
    )
    status = result_staging.result()

    result_artifact = create_result_artifact.submit(selected_session, datetime.now(timezone.utc) - date_start)
    result_artifact.result()  # type: ignore[unused-coroutine]

    if status == "successful":
        logger.info(f"✅ Session {selected_session} staged successfully.")
        if report_verbose is not None:
            report_verbose.success_step(2, "Staging completed successfully.")
        return True

    logger.error(f"❌ Session {selected_session} staged failed (status is '{status}').")
    if report_verbose is not None:
        report_verbose.failed_step(2, f"❌ Staging failed (status is '{status}').")
    return False
