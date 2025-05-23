# Copyright 2025 CS Group
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

"""Prefect flows and tasks for on-demand processing"""

from dataclasses import asdict, dataclass

from prefect import flow

from rs_workflows.cadip_flow import CadipFlow

# #
# # Implement flow parameters as dataclass classes.
# # NOTE: they must be implemented in this flow module to be usable in the prefect dashboard "custom run" page.

# @dataclass
# class CadipFlowParams:
#     """
#     Cadip flow parameters.

#     Attributes:
#         cadip_collection_identifier: CADIP collection identifier (to know the station)
#         session_identifier: Session identifier
#     """
#     cadip_collection_identifier: str
#     session_identifier: str


@flow(name="On-demand processing")
async def on_demand_processing(
    cadip_collection_identifier: str,
    session_identifier: str,
    catalog_collection_identifier: str,
):
    """
    Prefect flow for on-demand processing.

    Attributes:
        cadip_collection_identifier: CADIP collection identifier (to know the station)
        session_identifier: Session identifier
        catalog_collection_identifier: Catalog collection identifier where CADIP sessions and AUX data are staged
    """
    cadip_flow = CadipFlow(cadip_collection_identifier, session_identifier, catalog_collection_identifier)
    cadip_flow.myrun.submit().result()
