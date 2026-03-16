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

"""sentinel 1 Level-0 processing."""

import json
import os
import time
from datetime import datetime, timedelta
from enum import Enum
from pprint import pprint
from prefect.artifacts import acreate_markdown_artifact

from dask_gateway import Gateway
from dask_gateway.auth import JupyterHubAuth
from faker import Faker
from prefect import flow, get_run_logger, task
from pystac import Item, ItemCollection

from rs_client.ogcapi.dpr_client import (
    ClusterInfo,
    DprClient,
    DprPipeline,
    DprProcessor,
)
from rs_client.stac.cadip_client import CadipClient
from rs_client.stac.catalog_client import CatalogClient
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    DprProcessIn,
    FlowEnv,
    FlowEnvArgs,
    GeneratedProduct,
    InputProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_workflows.on_demand.stage_last_sessions import stage_session_common
from rs_workflows.on_demand_processing import dpr_processing


class Collection(str, Enum):
    S1_SESSION = "s01-cadip-session"


@flow(name="process a sentinel-1 sessions")
async def s1l0_processing(
    session: str,
    owner_identifier: str = "copernicus",
    dask_cluster_label: str = "dask-cluster-gateway-small",
    verbose: bool = False,
):
    logger = get_run_logger()
    logger.info(f"Mode verbose is set to {verbose}")

    # Check S1 session name format
    if not session.startswith("S1"):
        logger.error("Bad sentinel-1 session name.")
        raise ValueError(f"Invalid session name : {session} does not start with 'S1'")
    if len(session) < 4 or session[3] != "_":
        logger.error("Bad sentinel-1 session name.")
        raise ValueError(f"The 4th character of '{session}' is not '_'")
    logger.info("Sentinel-1 session name is correct. ")

    flow_env = FlowEnv(FlowEnvArgs(owner_id=owner_identifier))
    with flow_env.start_span(__name__, "sentinel1-level0"):
        # Check that the chosen dask_cluster_label is deployed
        gateway = Gateway(
            address=os.environ["DASK_GATEWAY_ADDRESS"],
            auth=JupyterHubAuth(api_token=os.environ["JUPYTERHUB_API_TOKEN"]),
        )

        clusters = gateway.list_clusters()
        cluster_id = None
        cluster_list_name: str = ""
        for cluster in clusters:
            cluster_name = cluster.options.get("cluster_name")
            cluster_list_name += " '" + cluster_name + "'"
            if cluster_name == dask_cluster_label:
                cluster_id = cluster
        logger.info(f"Here is the list of deployed dask clusters:{cluster_list_name}.")

        # Provide information on the cluster
        if cluster_id is None:
            logger.error(f"'{dask_cluster_label}' is not part of deployed dask clusters.")
            raise ValueError(f"Unknown '{dask_cluster_label}''.")
        else:
            logger.info(f"'{dask_cluster_label}' is part of deployed dask clusters.")
            status_map = {0: "UNKNOWN", 1: "PENDING", 2: "RUNNING", 3: "STOPPING", 4: "STOPPED", 5: "FAILED"}
            if cluster_id.status != 2:
                logger.warning(f"Cluster status = {cluster_id.status} ({status_map.get(cluster_id.status)})")
            await acreate_markdown_artifact(
                markdown=f"{json.dumps(cluster_id.options, indent=2)}",
                key="dask-cluster-options",
                description="Auxiliary files added to catalog."
                )
            logger.info(
                        "You can monitor the execution from dask dashboard: "
                        f"{os.environ["DASK_GATEWAY_PUBLIC"]}/clusters/{cluster_id}/status",
                    )

        # Try to retrieve the session on the collection
        catalog_client: CatalogClient = flow_env.rs_client.get_catalog_client()
        logger.info("Search session on the rs-catalog.")
        item_collection: ItemCollection = catalog_client.search(
            method="POST",
            collections=[Collection.S1_SESSION.value],
            ids=[session],
            limit=1,
        )
        item_session: Item = None

        if item_collection is not None:
            count = len(item_collection.items)
        else:
            count = 0
        if count == 1:
            # The session was found on the rs-catalog
            logger.info(
                f"The session '{session}' has been found on the rs-catalog collection '{Collection.S1_SESSION.value}'.",
            )
            item_session = item_collection.items[0]

        else:
            # The session was not found on the rs-catalog
            logger.info(
                f"The session '{session}' has NOT been found on the rs-catalog collection '{Collection.S1_SESSION.value}'.",
            )
            logger.info("Try to stage it from all S1 stations.")

            # Try to find a cadip station with this session available
            item_col = await cadip_session_search_by_name(flow_env, session)
            count = len(item_col)
            found = False
            cadip_station = ""
            if count == 1:
                collection_links = [link for link in item_col[0].links if link.rel == "collection"]
                if collection_links:
                    found = True
                    href = collection_links[0].href
                    cadip_station = href.rstrip("/").split("/")[-1]
                    logger.info(f"The session '{session}' is available at station {cadip_station}")

            # Stage the session
            if found:
                await stage_session_common(flow_env, cadip_station, session)
                item_collection = catalog_client.search(
                    method="POST",
                    collections=[Collection.S1_SESSION.value],
                    ids=[session],
                    limit=1,
                )
                item_session = item_collection.items[0]

    # The satellite name can be retrieved from the 3 first caracters of the session name
    satellite_identifier = session[:3].upper()
    end_datetime = datetime.fromisoformat(item_session.properties.get("published"))
    start_datetime = end_datetime - timedelta(hours=12)
    await call_dpr_flow(
        owner_identifier,
        dask_cluster_label,
        item_session,
        start_datetime,
        end_datetime,
        satellite_identifier,
    )


async def call_dpr_flow(
    owner_id: str,
    dask_cluster_label: str,
    item_session: Item,
    start_datetime: datetime,
    end_datetime: datetime,
    satellite_identifier: str,
) -> None:
    """
    Compute common arguments for S1 L0 Processing.

    Args:
        owner_id (str): _description_
        dask_cluster_label (str): _description_
        item_session (Item): _description_
    """
    # TODO : use a local path on the share disk
    fake = Faker()
    s3_payload = f"s3://prip-rs-playground/{owner_id}/{time.strftime('%Y-%m-%d--%H-%M-%S')}-{fake.word().lower()}-{fake.word().lower()}"

    a_process_s1l0 = DprProcessIn(
        env=FlowEnvArgs(owner_id=owner_id),
        processor_name=DprProcessor.S1L0,
        processor_version="1.4.0",  # TODO: retrieve automatically
        dask_cluster_label=dask_cluster_label,
        s3_payload_file=f"{s3_payload}/payload_s1l0.yaml",
        pipeline=DprPipeline.S1L0FULL,
        unit=None,
        priority=Priority.LOW,  # TODO: expose priority
        workflow_type=WorkflowType.ON_DEMAND,
        input_products=[
            InputProduct(
                name="S1CADUS",
                cadip_session=item_session.id,
                collection_name=Collection.S1_SESSION.value,
            ),
        ],
        generated_product_to_collection_identifier=[
            # GeneratedProduct(
            #    name="S01SARRAW",
            #    product_type="*",
            #    collection_name="s01sarraw",
            # ),
            GeneratedProduct(
                name="S01GPSRAW",
                product_type="*",
                collection_name="s01gpsraw",
            ),
            GeneratedProduct(
                name="S01HKMRAW",
                product_type="*",
                collection_name="allproductions",
            ),
            # GeneratedProduct(
            #    name="S01AISRAW",
            #    product_type="*",
            #    collection_name="allproductions",
            # ),
        ],
        auxiliary_product_to_collection_identifier=[
            AuxiliaryProductMapping(
                product_type="MPL_ORBPRE",
                collection_name="s01-aux-mpl_orbpre",
            ),
            AuxiliaryProductMapping(
                product_type="MPL_ORBSCT",
                collection_name="s01-aux-mpl_orbpre",
            ),
        ],
        processing_mode=[ProcessingMode.ALWAYS],
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        satellite=satellite_identifier,
    )
    print(a_process_s1l0.model_dump_json(indent=2))
    await dpr_processing_task(a_process_s1l0)


@task(name="Cadip session search by name")
async def cadip_session_search_by_name(env: FlowEnv, session: str) -> ItemCollection:
    """ """
    logger = get_run_logger()

    # Initialize flow environment and telemetry span
    cadip_client: CadipClient = env.rs_client.get_cadip_client()

    # Log query for debugging
    logger.info("Start request on all S1 CADIP stations")

    # Execute search request
    found = cadip_client.search(
        method="GET",
        ids=[session],
        collections=["s1_ins", "s1_kse", "s1_mps", "s1_mti", "s1_nsg", "s1_sgs"],
        max_items=1,
        limit=1,
    )

    return found


@task(name="dpr processing")
async def dpr_processing_task(*args, **kwargs) -> tuple[bool, ItemCollection | None]:
    """See: dpr_processing"""
    return await dpr_processing.fn(*args, **kwargs)
