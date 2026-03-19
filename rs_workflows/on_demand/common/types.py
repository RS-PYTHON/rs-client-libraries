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

"""common types and class"""

from prefect.variables import Variable
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Annotated
from datetime import datetime


from rs_client.ogcapi.dpr_client import DprPipeline
from rs_workflows.flow_utils import (
    AuxiliaryProductMapping,
    GeneratedProduct,
    Priority,
    ProcessingMode,
    WorkflowType,
)
from rs_client.ogcapi.dpr_client import DprPipeline, DprProcessor

from rs_workflows.flow_utils import (
    DprProcessIn,
    FlowEnv,
    FlowEnvArgs,
)

DEFAULT_PREFECT_CONFIGURATION = "s{mission}-l{level}-default-setting"

class SentinelSatellite2(str, Enum):
    """Sentinel satellite name"""

    # String value = STAC standardized value
    S1A = "sentinel-1a"
    S1B = "sentinel-1b"
    S1C = "sentinel-1c"
    S2A = "sentinel-2a"
    S2B = "sentinel-2b"
    S2C = "sentinel-2c"
    S3A = "sentinel-3a"
    S3B = "sentinel-3b"

class InputProduct2(BaseModel):
    """Represents one input product for the processor."""

    name: str = Field(description="Input product name.")
    cadip_session: str = Field(description="STAC item identifier.")
    collection_name: str = Field(description="Collection name.")

class DprProcessIn2(BaseModel):
    """
    Input parameters for executing the 'dpr-process' flow.

    This model defines all the configuration needed to run a DPR processor,
    including input datasets, generated outputs, auxiliary data mapping,
    processing modes, and scheduling parameters.
    """

    env: FlowEnvArgs = Field(
        title="Flow Environment",
        description="Environment configuration for Prefect flow. Includes identifiers like owner_id.",
    )

    processor_name: str | DprProcessor = Field(
        title="DPR Processor Name",
        description="Name of the DPR processor to run. Can be a string or DprProcessor enum.",
    )

    processor_version: str = Field(
        title="Processor Version",
        description="Version of the processor. If not relevant, can be empty string.",
    )

    dask_cluster_label: str = Field(
        title="Dask Cluster Label",
        description='Label of the Dask cluster to use, e.g. "dask-l0" for local testing.',
    )

    s3_payload_file: str = Field(
        title="S3 Payload File",
        description="S3 path where the processor payload (JSON) will be written for execution.",
    )

    pipeline: str | DprPipeline | None = Field(
        default=None,
        title="Pipeline Name",
        description="Name of the processing pipeline. Must be provided if `unit` is not set.",
    )

    unit: str | None = Field(
        default=None,
        title="Unit Name",
        description="Processing unit name. Must be provided if `pipeline` is not set.",
    )

    priority: Priority = Field(
        default=Priority.LOW,
        title="Processing Priority",
        description="Priority to assign for processing on the Dask cluster.",
    )

    workflow_type: WorkflowType = Field(
        default=WorkflowType.ON_DEMAND,
        title="Workflow Type",
        description="Type of workflow: ON_DEMAND, BENCHMARKING, SYSTEMATIC.",
    )

    input_products: list[InputProduct2] = Field(
        title="Input Products",
        description=(
            "List of input products for the processor. Each item specifies the product name, "
            "the STAC item identifier, and the collection it belongs to."
        ),
        min_length=1,
    )

    generated_product_to_collection_identifier: list[GeneratedProduct] = Field(
        title="Generated Products",
        description=(
            "List of generated products. Each item specifies a name, the product type, "
            "and the collection where the output will be stored."
        ),
        min_length=1,
    )

    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping] = Field(
        title="Auxiliary Product Mapping",
        description=(
            "Mapping of auxiliary product types to collections. "
            "Use '*' as a wildcard to map all other auxiliary products."
        ),
        min_length=1,
    )

    processing_mode: list[ProcessingMode] = Field(
        default_factory=list,
        title="Processing Modes",
        description="List of processing modes that control DPR behavior, e.g., ALWAYS, CONDITIONAL.",
    )

    start_datetime: datetime | None = Field(
        default=None,
        title="Start Datetime",
        description="Start datetime for retrieving auxiliary data. ISO format.",
    )

    end_datetime: datetime | None = Field(
        default=None,
        title="End Datetime",
        description="End datetime for retrieving auxiliary data. ISO format.",
    )

    satellite: str | SentinelSatellite2 | None = Field(
        default=None,
        title="Satellite",
        description="Satellite identifier used in certain queries. Can be a string or SentinelSatellite enum.",
    )

    # -----------------------
    # Validators
    # -----------------------

    @field_validator("processor_name", mode="before")
    @classmethod
    def normalize_processor_name(cls, v):
        """Normalize processor name to string."""
        return v.value if hasattr(v, "value") else v

    @field_validator("satellite", mode="before")
    @classmethod
    def normalize_satellite_name(cls, v):
        """Normalize satellite name to string."""
        return v.value if hasattr(v, "value") else v

    @model_validator(mode="after")
    def check_model(self):
        """Ensure mutual exclusivity between pipeline and unit."""
        has_pipeline = bool(self.pipeline)
        has_unit = bool(self.unit)

        if has_pipeline == has_unit:
            raise ValueError("Exactly one of 'pipeline' or 'unit' must be provided.")

        return self


    
class testBaseM(BaseModel):
    """
    Parameters for testing Prefect BaseModel rendering.
    """

    model_config = {
        "json_schema_extra": {
            "title": "Test Parameters",
            "description": "Simple model to test Prefect UI rendering."
        }
    }

    field1: str = Field(
        title="Field 1",
        description="First field.",
    )

    field2: str | DprPipeline | None = Field(
        default=None,
        title="Field 2",
        description="Second field.",
    )




class Level0FlowParams(BaseModel):
    owner_identifier: str = Field(
        default="",
        title="Owner Identifier",
        description="Identifier of the data owner used for processing and configuration."
    )

    dask_cluster_label: str = Field(
        default="",
        title="Dask Cluster Label",
        description="Name of the Dask cluster used for distributed execution."
    )

    session_collection: str = Field(
        default="",
        title="Session Collection",
        description="CADIP collection name containing the Sentinel session."
    )

    processor_name: str = Field(
        default="",
        title="Processor Name",
        description="Name of the processor used for Level-0 processing."
    )

    processor_version: str = Field(
        default="",
        title="Processor Version",
        description="Version of the processor used for Level-0 processing."
    )

    pipeline: Optional[DprPipeline] = Field(
        default=None,
        title="Pipeline",
        description="DPR pipeline to use for processing."
    )

    unit: str = Field(
        default="",
        title="Unit",
        description="Processing unit or internal identifier."
    )

    priority: Optional[Priority] = Field(
        default=None,
        title="Priority",
        description="Processing priority (low, normal, high)."
    )

    processing_mode: List[ProcessingMode] = Field(
        default_factory=list,
        title="Processing Mode",
        description="List of processing modes to apply."
    )

    workflow: Optional[WorkflowType] = Field(
        default=None,
        title="Workflow Type",
        description="Workflow type to execute (on-demand, scheduled, etc.)."
    )

    generated_product_to_collection_identifier: List[GeneratedProduct] = Field(
        default_factory=list,
        title="Generated Product Mapping",
        description="List of generated products and their target collections."
    )

    auxiliary_product_to_collection_identifier: List[AuxiliaryProductMapping] = Field(
        default_factory=list,
        title="Auxiliary Product Mapping",
        description="List of auxiliary products and their target collections."
    )

    cadip_collections: List[str] = Field(
        default_factory=list,
        title="CADIP Collections",
        description="List of CADIP collections to query for session retrieval."
    )

    async def resolve(self, mission: str, level: str = "0") -> "Level0FlowParams":
        var_name = DEFAULT_PREFECT_CONFIGURATION.format(mission=mission, level=level)
        settings: dict = await Variable.get(var_name)

        if settings is None:
            raise ValueError(f"❌ Prefect variable '{var_name}' not found")

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
