#!/bin/bash
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

"""
Call Prefect REST API from the command line of a Github CI/CD workflow or action.

See: https://docs.prefect.io/v3/api-ref/rest-api

NOTE:
The PREFECT_CONFIG_DEV and PREFECT_CONFIG_OPS variables are defined in
https://github.com/RS-PYTHON/rs-client-libraries/settings/variables/actions
It should have yaml content as:
prefect_public_url: https://<prefect-public-domain>

The PREFECT_CREDENTIALS_DEV and PREFECT_CREDENTIALS_OPS secrets are defined in
https://github.com/RS-PYTHON/rs-client-libraries/settings/secrets/actions
These values are set with the admin-dev account on https://admin.iam.example.com/admin/master/console for
the Prefect service.
It should have yaml content as:
keycloak_token_url: ***
client_id: ***
client_secret: ***
"""

import math
import time
from json import JSONDecodeError
from typing import Any

import requests
import yaml

config = dict()
credentials = dict()


def init(config_yaml: str, credentials_yaml: str):
    """
    Read configuration from github variable PREFECT_CONFIG_DEV or PREFECT_CONFIG_OPS,
    and credentials from github secret PREFECT_CREDENTIALS_DEV or PREFECT_CREDENTIALS_OPS.

    WARNING: don't print credential or token values because they could appear in clear in the ci/cd logs!
    """
    global config, credentials
    config = yaml.safe_load(config_yaml)
    credentials = yaml.safe_load(credentials_yaml)


def __read_access_token() -> str:
    """
    Read access token returned by KeyCloak.
    NOTE: private function, don't call it from the git console, to avoid leaking the token.

    Returns: dict {"Authorization": "Bearer <access_token>"}
    """
    response = requests.post(
        credentials["keycloak_token_url"],
        data={
            "grant_type": "client_credentials",
            "client_id": credentials["client_id"],
            "client_secret": credentials["client_secret"],
        },
    )
    response.raise_for_status()
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def prefect_url(suffix: str):
    """Return a Prefect API URL from its suffix."""
    return config["prefect_public_url"].strip("/") + "/" + suffix.strip("/")


def __get_deployment_id(deploy_name: str) -> str:
    """
    Return a Prefect deployment ID from its name as {flow_name}/{deployment_name}

    See: https://docs.prefect.io/v3/api-ref/rest-api/server/deployments/read-deployment-by-name
    """
    response = requests.get(
        prefect_url(f"api/deployments/name/{deploy_name}"),
        headers=__read_access_token(),
    )
    # NOTE: if this fails, check all available names from a Jupyter terminal by running: 'prefect deployment ls'
    response.raise_for_status()
    return response.json()["id"]


def trigger_flow_run(deploy_name: str, body: Any | None = None) -> str:
    """
    Trigger a Prefect deployment flow run, from the deployment name and any other parameters.

    Returns: Prefect flow run ID

    See: https://docs.prefect.io/v3/api-ref/rest-api/server/deployments/create-flow-run-from-deployment
    """
    deployment_id = __get_deployment_id(deploy_name)

    response = requests.post(
        prefect_url(f"api/deployments/{deployment_id}/create_flow_run"),
        headers=__read_access_token(),
        json=body,
    )
    response.raise_for_status()
    return response.json()["state"]["state_details"]["flow_run_id"]


def get_flow_run_url(flow_run_id: str) -> str:
    """Return the URL of the page of a Prefect flow run"""
    return f"{prefect_url('v2/runs/flow-run')}/{flow_run_id}"


def wait_flow_finish(flow_run_id: str, delay: float, timeout: float = math.inf):
    """
    Wait for a Prefect flow run to finish, with a timeout in seconds.

    See: https://docs.prefect.io/v3/api-ref/rest-api/server/flow-run-states/read-flow-run-states
    """
    # URL of the page of the Prefect flow run
    flow_run_url = get_flow_run_url(flow_run_id)

    last_status = "(NOT FOUND)"
    while True:

        # Get all states of the flow run
        response = requests.get(
            prefect_url(f"api/flow_run_states"),
            headers=__read_access_token(),
            params={"flow_run_id": flow_run_id},
        )
        response.raise_for_status()
        try:
            states = response.json()
        except JSONDecodeError:
            states = []

        # The states are sorted from oldest to latest. If we still don't have any states, wait a little bit.
        # Else check the last state status.
        if states:
            last_status = states[-1]["type"]
            if last_status == "COMPLETED":
                print(f"Flow run {last_status}: {flow_run_url}")
                return
            if last_status in ["FAILED", "CANCELLED", "CRASHED", "CANCELLING"]:
                raise RuntimeError(f"Flow run {last_status}: {flow_run_url}")

        if timeout <= 0:
            raise RuntimeError(f"Reached timeout for flow run {last_status}: {flow_run_url}")

        print(f"Wait for flow run {last_status}: {flow_run_url} ...")
        timeout -= delay
        time.sleep(delay)


def read_artifact(flow_run_id: str, artifact_key: str) -> Any:
    """
    Read artifact value from its flow run ID and key (=name).

    See: https://docs.prefect.io/v3/api-ref/rest-api/server/artifacts/read-artifacts
    """
    response = requests.post(
        prefect_url(f"api/artifacts/filter"),
        json={
            "artifacts": {
                "operator": "and_",
                "key": {"any_": [artifact_key]},
                "flow_run_id": {"any_": [flow_run_id]},
            },
        },
    )
    response.raise_for_status()
    artifacts = response.json()
    if not artifacts:
        raise RuntimeError(f"No artifact found for flow run ID: {flow_run_id!r} and artifact key: {artifact_key!r}")
    return artifacts[0]["data"]
