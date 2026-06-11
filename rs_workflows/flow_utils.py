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

"""Utility module for the Prefect flows."""

import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

from opentelemetry import trace
from opentelemetry.trace import Span, SpanContext
from opentelemetry.util._decorator import _agnosticcontextmanager
from prefect import get_run_logger
from pydantic import BaseModel, Field, field_validator, model_validator
from pystac import Item

from rs_client.ogcapi.dpr_client import DprPipeline, DprProcessor
from rs_client.rs_client import RsClient
from rs_common import init_opentelemetry, prefect_utils

ARCHIVE_SUFFIXES = (".zip", ".tar", ".tgz", ".tar.gz")


class Priority(str, Enum):
    """
    Priority for the cluster dask to be able to prioritise task execution.
    """

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class WorkflowType(str, Enum):
    """
    Workflow type.
    """

    BENCHMARKING = "benchmarking"
    ON_DEMAND = "on-demand"
    SYSTEMATIC = "systematic"


class ProcessingMode(str, Enum):
    """
    List of mode to be applied when calling the DPR processor.
    """

    NRT = "nrt"
    NTC = "ntc"
    REPROCESSING = "reprocessing"
    SUBS = "subs"
    ALWAYS = "always"


class InstrumentMode(str, Enum):
    """Instrument mode"""

    EW = "EW"
    IW = "IW"
    SM = "SM"


class SentinelSatellite(str, Enum):
    """Sentinel satellite name"""

    # String value = STAC standardized value
    S1A = "sentinel-1a"
    S1B = "sentinel-1b"
    S1C = "sentinel-1c"
    S1D = "sentinel-1d"
    S2A = "sentinel-2a"
    S2B = "sentinel-2b"
    S2C = "sentinel-2c"
    S3A = "sentinel-3a"
    S3B = "sentinel-3b"


class AdfType(str, Enum):
    """ADF type"""

    S00__ADF_ECMWA = "S00__ADF_ECMWA"
    S00__ADF_ECMWF = "S00__ADF_ECMWF"
    S00__ADF_GETAS = "S00__ADF_GETAS"
    S00__ADF_WATER = "S00__ADF_WATER"
    S03_ADF_OLCAL = "S03_ADF_OLCAL"
    S03_ADF_OLEOP = "S03_ADF_OLEOP"
    S03_ADF_OLINS = "S03_ADF_OLINS"
    S03_ADF_OLLUT = "S03_ADF_OLLUT"
    S03_ADF_OLPRG = "S03_ADF_OLPRG"
    S03_ADF_OLRAC = "S03_ADF_OLRAC"
    S03_ADF_OLSPC = "S03_ADF_OLSPC"


class LoggingLevel(str, Enum):
    """Logging level allowed by eopf.logging module"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AuxiliarySource(str, Enum):
    """STAC source for auxiliary product search."""

    PRIP = "prip"
    CADIP = "cadip"
    AUXIP = "auxip"
    CDSE = "cdse"
    CATALOG = "catalog"


class FlowEnvArgs(BaseModel):
    """
    Prefect flow environment arguments.

    Attributes:
        owner_id: User/owner ID (necessary to retrieve the user info: API key and OAuth2 cookie)
        from the right Prefect block. NOTE: may be useless after each user has their own prefect
        server because there will be only one block.
        calling_span (tuple): Serialized OpenTelemetry span of the calling flow, if any.
        service_name: OpenTelemetry service name
    """

    owner_id: str = Field(
        description="User/owner ID (necessary to retrieve the user info) from the right Prefect block",
    )
    calling_span: tuple[int, int, bool] | None = Field(
        default=None,
        description="Serialized OpenTelemetry span of the calling flow, if any",
    )
    service_name: str = Field(default="rs.workflows", description="OpenTelemetry service name")


class FlowEnv:
    """
    Prefect flow environment and reusable objects.

    Attributes:
        owner_id (str): User/owner ID
        calling_span (SpanContext | None): OpenTelemetry span of the calling flow, if any.
        this_span (SpanContext | None): Current OpenTelemetry span.
        rs_client (RsClient): RsClient instance
    """

    def __init__(self, args: FlowEnvArgs):
        """Constructor."""
        self.owner_id: str = args.owner_id
        self.calling_span: SpanContext | None = None
        self.this_span: SpanContext | None = None

        # Deserialize the calling span, if any
        if args.calling_span:
            self.calling_span = SpanContext(*args.calling_span)

        # Read prefect blocks into env vars
        prefect_utils.read_prefect_blocks(self.owner_id, _sync=True)  # type: ignore

        # Init opentelemetry traces
        init_opentelemetry.init_traces(args.service_name)

        # Init the RsClient instance from the env vars
        self.rs_client = RsClient(
            rs_server_href=os.getenv("RSPY_WEBSITE"),
            rs_server_api_key=os.getenv("RSPY_APIKEY"),
            owner_id=self.owner_id,
            logger=get_run_logger(),  # type: ignore
        )

    def serialize(self) -> FlowEnvArgs:
        """Serialize this object with Pydantic."""

        # The serialized object will be used by a new opentelemetry span.
        # Its calling span will be either the current span, or the current calling span.
        new_calling_span = self.this_span or self.calling_span
        if new_calling_span:
            # Only keep the first n attributes, the other need custom serialization
            serialized_span = tuple(new_calling_span)[:3]
        else:
            serialized_span = None

        return FlowEnvArgs(owner_id=self.owner_id, calling_span=serialized_span)  # type: ignore

    @_agnosticcontextmanager
    def start_span(
        self,
        instrumenting_module_name: str,
        name: str,
    ) -> Iterator[Span]:
        """
        Context manager for creating a new main or child OpenTelemetry span and set it
        as the current span in this tracer's context.

        Args:
            instrumenting_module_name: Caller module name, just pass __name__
            name: The name of the span to be created (use a custom name)

        Yields:
            The newly-created span.
        """
        # Create new span and save it
        with init_opentelemetry.start_span(  # pylint: disable=contextmanager-generator-missing-cleanup
            instrumenting_module_name,
            name,
            self.calling_span,
        ) as span:
            self.this_span = trace.get_current_span().get_span_context()
            yield span


class FlowInputProduct(BaseModel):
    """Represents one input product for the processor."""

    name: str = Field(description="Input product name.")
    item_id: str = Field(description="STAC item identifier.")
    collection_name: str = Field(description="Collection name.")

    def items(self):
        """Helper method to return the model fields as items, useful for logging."""
        return self.model_dump().items()


class FlowGeneratedProduct(BaseModel):
    """Represents one generated output product."""

    name: str = Field(description="Output product name.")
    product_type: str = Field(description="Product type.")
    collection_name: str | None = Field(
        default=None,
        description="Collection name. If not provided, it defaults to product_type.",
    )

    def items(self):
        """Helper method to return the model fields as items, useful for logging."""
        return self.model_dump().items()


class AuxiliaryProductMapping(BaseModel):
    """Represents mapping for auxiliary products."""

    product_type: str = Field(description="Product type or '*' wildcard.")
    collection_name: str = Field(description="Collection name.")
    source: AuxiliarySource = Field(
        default=AuxiliarySource.AUXIP,
        description="STAC source where auxiliary products are searched.",
    )
    selected_assets: list[str] | None = Field(
        default=None,
        description="Optional asset keys to stage.",
    )

    def items(self):
        """Helper method to return the model fields as items, useful for logging."""
        return self.model_dump().items()


class DprProcessIn(BaseModel):
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

    dask_cluster_instance: str | None = Field(
        default=None,
        title="Dask Cluster Instance",
        description="Optional Dask cluster instance ID used to build a direct dashboard URL.",
    )

    logging_level: LoggingLevel = Field(
        default=LoggingLevel.INFO,
        title="Overall EOPF logging level",
        description="Overall EOPF logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
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

    input_products: list[FlowInputProduct] = Field(
        title="Input Products",
        description=(
            "List of input products for the processor. Each item specifies the product name, "
            "the STAC item identifier, and the collection it belongs to."
        ),
        min_length=1,
    )

    generated_product_to_collection_identifier: list[FlowGeneratedProduct] = Field(
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

    satellite: str | SentinelSatellite | None = Field(
        default=None,
        title="Satellite",
        description="Satellite identifier used in certain queries. Can be a string or SentinelSatellite enum.",
    )

    reference_date: date | None = Field(
        default=None,
        title="Reference Date",
        description="Date used to identify a specific reference/master input product within the list of inputs.",
    )

    instrument_mode: str | InstrumentMode | None = Field(
        default=None,
        title="Instrument Mode",
        description="Instrument mode used in certain queries. Can be a string or InstrumentMode enum.",
    )

    edh_api_key: str | None = Field(
        default=None,
        title="EarthDataHub Standard API key",
        description="Destination Earth / EarthDataHub standard API key used to access Copernicus DEM",
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


class AdfProcessIn(BaseModel):
    """
    Input parameters for executing the 'adf_conversion' flow.

    This model defines all the configuration needed to run an ADF conversion script.
    """

    env: FlowEnvArgs = Field(
        title="Flow Environment",
        description="Environment configuration for Prefect flow. Includes identifiers like owner_id.",
    )
    adf_type: str | AdfType = Field(
        title="ADF Type",
        description="Name of the ADF type to generate. Can be a string or AdfType enum.",
    )
    auxiliary_product_to_collection_identifier: list[AuxiliaryProductMapping] = Field(
        title="Auxiliary Product Mapping",
        description=(
            "Mapping of auxiliary product types to collections. "
            "Use '*' as a wildcard to map all other auxiliary products."
        ),
        min_length=1,
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
    satellite: str | SentinelSatellite | None = Field(
        default=None,
        title="Satellite",
        description="Satellite identifier used in certain queries. Can be a string or SentinelSatellite enum.",
    )

    @field_validator("adf_type", mode="before")
    @classmethod
    def normalize_adf_type(cls, v):
        """Normalize ADF type to string."""
        return v.value if hasattr(v, "value") else v

    @field_validator("satellite", mode="before")
    @classmethod
    def normalize_satellite_name(cls, v):
        """Normalize satellite name to string."""
        return v.value if hasattr(v, "value") else v


@dataclass
class DprProcessOut:
    """
    Output parameters for the 'dpr-process' flow
    """

    status: bool
    product_identifier: list[Item] = field(default_factory=list)


@dataclass
class DprProcessedItemMetadata:
    """Metadata for a DPR processed item."""

    output_product_id: str
    product_type: str | None
    stac_item: Item


class RetryConfig(BaseModel):
    """
    Args:
        staging_retries: Number of retry attempts for staging operations
        staging_retry_delay: Delay in seconds between retry attempts.
    """

    staging_retries: int = Field(3, description="Number of retry attempts for staging operations.")
    staging_retry_delay: int = Field(60, description="Delay in seconds between retry attempts.")


class ConversionIn(BaseModel):
    """
    Input parameters for executing the 'on_demand_conversion' flow.

    This model defines all the configuration needed to run an on-demand conversion flow,
    including input datasets, generated outputs, and scheduling parameters.
    """

    env: FlowEnvArgs = Field(
        title="Flow Environment",
        description="Environment configuration for Prefect flow. Includes identifiers like owner_id.",
    )
    stac_input: str | dict = Field(
        title="STAC Input Product",
        description=("Input product for the conversion. Specifies the STAC item or href."),
    )
    generated_product_to_collection_identifier: FlowGeneratedProduct = Field(
        title="Generated Product",
        description=(
            "Generated product. Specifies a name, the product type, and the collection where the output will be stored."
        ),
    )
    owner_id: str = Field(
        title="Owner ID",
        description="User/owner ID necessary to retrieve the user info from the right Prefect block.",
    )
    dask_cluster_label: str = Field(
        title="Dask Cluster Label",
        description="Label of the Dask cluster to use for SAFE conversion.",
    )
    dask_cluster_instance: str | None = Field(
        default=None,
        title="Dask Cluster Instance",
        description="Optional Dask cluster instance ID used by the DPR conversion service.",
    )

    selected_assets: list[str] | None = Field(
        default=None,
        title="Selected Assets",
        description=("Set of selected asset keys to stage. If not provided, all assets will be converted"),
    )
