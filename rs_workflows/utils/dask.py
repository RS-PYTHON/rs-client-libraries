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

"""Helper task to interact with the Dask cluster."""

from dask_gateway import Gateway
from dask_gateway.auth import JupyterHubAuth
from prefect.artifacts import acreate_markdown_artifact
from rs_workflows.flow_utils import FlowEnv
from prefect import get_run_logger, task
import os
import json


@task(name="Check dask cluster status")
async def is_dask_cluster_running(
    dask_cluster_label: str
)->bool:
    
    result = False
    logger = get_run_logger()

    # Connect to the dask gateway
    gateway = Gateway(
        address=os.environ["DASK_GATEWAY_ADDRESS"],
        auth=JupyterHubAuth(api_token=os.environ["JUPYTERHUB_API_TOKEN"]),
    )

    # Find the cluster matching the label
    clusters = gateway.list_clusters()
    cluster_id = None
    for cluster in clusters:
        cluster_name = cluster.options.get("cluster_name")
        if cluster_name == dask_cluster_label:
            cluster_id = cluster
    cluster_names = [c.options.get("cluster_name", "<unknown>") for c in clusters]


    # Check status 
    if cluster_id is None:
        logger.error(f"❌ '{dask_cluster_label}' is not part of deployed dask clusters {cluster_names}.")
    else:
        logger.info(f"✔️ '{dask_cluster_label}' is part of deployed dask clusters {cluster_names}.")
        status_map = {0: "UNKNOWN", 1: "PENDING", 2: "RUNNING", 3: "STOPPING", 4: "STOPPED", 5: "FAILED"}
        if cluster_id.status == 2:
            result = True
        else:
            logger.warning(f"⚠️ Cluster status = {cluster_id.status} ({status_map.get(cluster_id.status)})")

        # Save artifact
        md = f"# Dask cluster option for {dask_cluster_label}\n\n```json\n" + json.dumps(cluster_id.options, indent=2) + "\n```"
        await acreate_markdown_artifact(
            markdown=md,
            key="dask-cluster-options",
            description=f"Options associated to the running dask cluster {dask_cluster_label}.",
        )
        logger.info(
            "📈 You can monitor the execution from dask dashboard: "
            f"{os.environ["DASK_GATEWAY_PUBLIC"]}/clusters/{cluster_id.name}/status",
        )
            
    return result