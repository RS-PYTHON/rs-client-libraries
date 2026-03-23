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

"""common types and class"""

from prefect.variables import Variable
from pydantic import BaseModel, Field

from rs_client.ogcapi.dpr_client import DprPipeline
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    FlowGeneratedProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)

DEFAULT_PREFECT_CONFIGURATION = "s{mission}-l{level}-default-setting"


class Level0FlowParams(BaseModel):
    """
    Parameters to override default Prefect variable 'sx-l0-default-setting'..

    There is no need to set all of them.
    Only the ones you want to override from default settings.
    optional type not used.
    """

    owner_identifier: str = Field(
        default="",
        title="Owner Identifier",
        description="Identifier of the user that run the flow",
        json_schema_extra={"order": 1},
    )

    dask_cluster_label: str = Field(
        default="",
        title="Dask Cluster Label",
        description="Name of the Dask cluster used for distributed execution.",
        json_schema_extra={"order": 2},
    )

    pipeline: DprPipeline | None = Field(
        default=None,
        title="Pipeline",
        description="DPR pipeline to use for processing.",
        json_schema_extra={"order": 3},
    )

    session_collection: str = Field(
        default="",
        title="Session Collection",
        description="CADIP collection name containing the Sentinel session.",
        json_schema_extra={"order": 4},
    )

    processor_name: str = Field(
        default="",
        title="Processor Name",
        description="Name of the processor used for Level-0 processing.",
        json_schema_extra={"order": 5},
    )

    processor_version: str = Field(
        default="",
        title="Processor Version",
        description="Version of the processor used for Level-0 processing.",
        json_schema_extra={"order": 6},
    )

    unit: str = Field(
        default="",
        title="Unit",
        description="Processing unit or internal identifier.",
        json_schema_extra={"order": 7},
    )

    priority: Priority | None = Field(
        default=None,
        title="Priority",
        description="Processing priority (low, normal, high).",
        json_schema_extra={"order": 8},
    )

    processing_mode: list[ProcessingMode] = Field(
        default_factory=list,
        title="Processing Mode",
        description="List of processing modes to apply.",
        json_schema_extra={"order": 9},
    )

    workflow: WorkflowType | None = Field(
        default=None,
        title="Workflow Type",
        description="Workflow type to execute (on-demand, scheduled, etc.).",
        json_schema_extra={"order": 10},
    )

    generated_product_to_collection_identifier: list[FlowGeneratedProduct] | None = Field(
        default=None,
        title="Generated Product Mapping",
        description="List of generated products and their target collections.",
        json_schema_extra={"order": 10},
    )

    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping] | None = Field(
        default=None,
        title="Auxiliary Product Mapping",
        description="List of auxiliary products and their target collections.",
        json_schema_extra={"order": 11},
    )

    cadip_collections: list[str] = Field(
        default_factory=list,
        title="CADIP Collections",
        description="List of CADIP collections to query for session retrieval.",
        json_schema_extra={"order": 12},
    )

    async def resolve(self, mission: str, level: str = "0") -> "Level0FlowParams":
        """
        Merge data from Prefect variable and parameters called.
        """
        var_name = DEFAULT_PREFECT_CONFIGURATION.format(mission=mission, level=level)
        settings = Variable.get(var_name)

        if settings is None:
            raise ValueError(f"❌ Prefect variable '{var_name}' not found.")

        return Level0FlowParams(
            owner_identifier=self.owner_identifier or settings.get("owner_identifier", ""),
            dask_cluster_label=self.dask_cluster_label or settings.get("dask_cluster_name", ""),
            session_collection=self.session_collection or settings.get("session_collection", ""),
            processor_name=self.processor_name or settings.get("processor", {}).get("name", ""),
            processor_version=self.processor_version or settings.get("processor", {}).get("version", ""),
            pipeline=self.pipeline or settings.get("pipeline", None),
            unit=self.unit or settings.get("unit", ""),
            priority=self.priority or settings.get("priority"),
            processing_mode=self.processing_mode or settings.get("processing_mode", []),
            workflow=self.workflow or settings.get("workflow"),
            generated_product_to_collection_identifier=(
                self.generated_product_to_collection_identifier
                or settings.get("generated_product_to_collection_identifier", [])
            ),
            auxiliary_product_to_collection_identifier=(
                self.auxiliary_product_to_collection_identifier
                or settings.get("auxiliary_product_to_collection_identifier", [])
            ),
            cadip_collections=settings.get("cadip_collections", []),
        )
