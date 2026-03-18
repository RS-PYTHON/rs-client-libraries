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

""" common types and class """

from prefect.variables import Variable
from pydantic import BaseModel, Field
from pystac import Item

from rs_client.ogcapi.dpr_client import DprPipeline
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    GeneratedProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)


DEFAULT_PREFECT_CONFIGURATION = "s{mission}-l{level}-default-setting"

class Level0FlowParams(BaseModel):
    owner_identifier: str = ""
    dask_cluster_label: str = ""
    session_collection: str = ""
    processor_name: str = ""
    processor_version: str = ""
    pipeline: DprPipeline | None = None
    unit: str = ""
    priority: Priority | None = None
    processing_mode: list[ProcessingMode] = Field(default_factory=list)
    workflow: WorkflowType | None = None
    generated_product_to_collection_identifier: list[GeneratedProduct] = Field(default_factory=list)
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping] = Field(default_factory=list)
    cadip_collections: list[str] = Field(default_factory=list)  # ajouté car tu l'utilises

    async def resolve(self, mission: str, level: str = "0") -> "Level0FlowParams":
        settings = await Variable.get(DEFAULT_PREFECT_CONFIGURATION.format(mission=mission, level=level))
        return Level0FlowParams(
            owner_identifier=self.owner_identifier or settings.get("owner_identifier", ""),
            dask_cluster_label=self.dask_cluster_label or settings.get("dask_cluster_name", ""),
            session_collection=self.session_collection or settings.get("session_collection", ""),
            processor_name=self.processor_name or settings.get("processor_name", ""),
            processor_version=self.processor_version or settings.get("processor_version", ""),
            pipeline=self.pipeline,
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
            cadip_collections=settings["cadip_collections"],
        )
