# Copyright 2026 Airbus defence And Space
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

"""OSAM flow implementation"""

import json
import os

import requests
from prefect import flow, task
from prefect.artifacts import acreate_markdown_artifact
from prefect.context import TaskRunContext

from rs_workflows.flow_utils import FlowEnv, FlowEnvArgs


class OSAMUserNotFoundError(Exception):
    """Raised when the OSAM user does not exist."""


class OSAMRequestError(Exception):
    """Raised when OSAM returns an unexpected HTTP status."""


@flow(
    name="OSAM synchronize accounts",
    description="Synchronize keycloak and object storage accounts.",
    log_prints=True,
    validate_parameters=True,
)
async def osam_synchronize_accounts(env: FlowEnvArgs = FlowEnvArgs(owner_id="operator-osam")) -> None:
    """
    Synchronize keycloak and object storage accounts.

    Args:
        env (FlowEnvArgs): user account that call the flow

    Raises:
        OSAMRequestError: error HTTP status error
    """
    print("Synchronize keycloak and object storage accounts.")

    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "OSAM-synchronize-accounts"):
        rs_server_href = os.getenv("RSPY_WEBSITE")
        request_url = f"{rs_server_href}/storage/accounts/update"
        test = flow_env.rs_client.apikey_headers
        print(f"Call request: {request_url} with {test} ")
        response = requests.post(request_url, **flow_env.rs_client.apikey_headers, timeout=30)
        if response.status_code != 200:
            raise OSAMRequestError(
                f"❌ Unexpected HTTP status {response.status_code} while synchronising accounts ({response.text}).",
            )
        print("✔️ The synchronization process is now running. Allow a few minutes before reviewing the changes.")


@task(name="create artifact with JSON OBS rights")
async def create_rights_artifact(rights: dict, username: str) -> None:
    """
    Register the JSON with OBS rights.

    Args:
        rights (dict)
        username (str)
    """
    pretty_json = json.dumps(rights, indent=2, ensure_ascii=False)
    markdown_report = f"""
## Object Storage rights for **{username}**

```json
{pretty_json}
"""
    await acreate_markdown_artifact(key="rights", markdown=markdown_report, description="session staging output")


@flow(name="OSAM update account", log_prints=True, validate_parameters=True)
async def osam_update_user(user_name: str, env: FlowEnvArgs = FlowEnvArgs(owner_id="operator-osam")):
    """
    Flow that update a single OBS account.

    Args:
        env (FlowEnvArgs): account that call the flow
        user_name (str): account to be updated
    """
    task_run_ctx = TaskRunContext.get()
    if task_run_ctx is not None:
        task_run_ctx.task_run.name = f"📦Update Object Storage rights for user '{user_name}'"
    print("Start update OSAM user rights.")

    # Initialize flow environment and telemetry span
    flow_env = FlowEnv(env)
    with flow_env.start_span(__name__, "OSAM-update-user"):

        # Retrieve the RS server URL from the environment variable
        rs_server_href = os.getenv("RSPY_WEBSITE")
        request_url = f"{rs_server_href}/storage/account/{user_name}/update"
        print(f"Call request: {request_url}")
        response = requests.post(request_url, **flow_env.rs_client.apikey_headers, timeout=30)

        if response.status_code == 404:
            raise OSAMUserNotFoundError(f"❌ User '{user_name}' does not exist in OSAM (HTTP 404): {response.text} .")

        if response.status_code != 200:
            raise OSAMRequestError(
                f"❌ Unexpected HTTP status {response.status_code} while updating user '{user_name}': {response.text}.",
            )
        print(f"✔️ Rights for user '{user_name}' successfully applied.")

        print("Regiser the new rights...")
        # Make the request for user's access rights
        request_url = f"{rs_server_href}/storage/account/{user_name}/rights"
        print(f"Call request: {request_url}")
        response = requests.get(request_url, **flow_env.rs_client.apikey_headers, timeout=30)

        if response.status_code != 200:
            raise OSAMRequestError(
                f"❌ Failed to retrieve rights for '{user_name}' (HTTP {response.status_code} {response.text}).",
            )
        rights = response.json()
        await create_rights_artifact(rights, user_name)  # type: ignore
