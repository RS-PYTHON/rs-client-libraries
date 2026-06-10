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

"""EarthDataHub flow implementation."""

from prefect import flow, task
from pystac import ItemCollection

from rs_client.stac.earthdatahub_client import EarthDataHubClient
from rs_workflows.flow_utils import FlowEnvArgs
from rs_workflows.utils import stac

######################
# EarthDataHub flows #
######################


@flow(name="search-earthdatahub")
async def search(
    env: FlowEnvArgs,
    edh_cql2: dict,
    error_if_empty: bool = False,
) -> ItemCollection | None:
    """
    Search STAC products in EarthDataHub catalogue.

    Args:
        env: Prefect flow environment (at least the owner_id is required)
        edh_cql2: CQL2 filter read from the processor tasktable.
        error_if_empty: Raise a ValueError if the results are empty.
    """
    return await stac.search(env, edh_cql2, "earthdatahub-search", lambda _: EarthDataHubClient(), error_if_empty)


###########################
# Call the flows as tasks #
###########################


@task(name="search-earthdatahub")
async def earthdatahub_search_task(*args, **kwargs) -> ItemCollection | None:
    """See: search"""
    return await search.fn(*args, **kwargs)
