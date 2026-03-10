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
from datetime import datetime
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


class SentinelSatellite(str, Enum):
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


class FlowEnvArgs(BaseModel):
    """
    Prefect flow environment arguments.

    Attributes:
        owner_id: User/owner ID (necessary to retrieve the user info: API key and OAuth2 cookie)
        from the right Prefect block. NOTE: may be useless after each user has their own prefect
        server because there will be only one block.
        calling_span (tuple): Serialized OpenTelemetry span of the calling flow, if any.
    """

    owner_id: str = Field(
        description="User/owner ID (necessary to retrieve the user info) from the right Prefect block",
    )
    calling_span: tuple[int, int, bool] | None = Field(
        default=None,
        description="Serialized OpenTelemetry span of the calling flow, if any",
    )


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
        init_opentelemetry.init_traces("rs.client")

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


class DprProcessIn(BaseModel):  # pylint: disable=too-many-instance-attributes
    """
    Input parameters for the 'dpr-process' flow

    Attributes:
        env: Prefect flow environment
        processor_name: DPR processor name
        processor_version: DPR processor version
        dask_cluster_label: Dask cluster label e.g. "dask-l0"
        dask_cluster_instance: Optional Dask cluster instance ID used to build a direct dashboard URL.
        s3_payload_file: S3 path where the processor payload will be written
        pipeline: Processor pipeline name. The task table propose one or several pipelines.
          Mandatory if unit is not provided.
        unit: Processor unit name. Advanced users can call directly a single unit of the task table.
          Mandatory if pipeline is not provided.
        priority: Priority for the cluster dask to be able to prioritise task execution. By default is "low".
        workflow_type: Workflow type (benchmarking, on-demand, systematic). By default is "on-demand".
        input_products: List of input products for the processor, structured as follows:
          * input_products.name
          * (stac item identifier, collection name)
          Example: [( "S1CADUS", ["S1A1234", "s01-cadip-session"])]
        generated_product_to_collection_identifier: List of output products for the processor, structured as follows:
          * output_products.name
          * (product:type, collection name)
          or
          * product:type
          When the collection name is not specified, it is equal to product:type.
          Example: [( "SRAL0", "s03sral0_" ),( "MWRL0", "s03mwrl0", "my-collection" )]
        auxiliary_product_to_collection_identifier: Collection name where to push each auxiliary file (in rs-catalog).
          To apply the same treatment to all product types simultaneously, a "*" wildcard can be used.
          By default (when no input is provided), the collection name is set to <mission>-aux-<product:type>
        processing_mode: List of modes to be applied when calling the DPR processor.
        start_datetime: Date that can be used to retrieve auxiliary data on the right time frame.
        end_datetime: Date that can be used to retrieve auxiliary data on the right time frame.
        satellite: In certain CQL2 queries from task tables, the <satellite> parameter must be provided,
          as some auxiliary files depend on the satellite.
    """

    env: FlowEnvArgs = Field(description="Prefect flow environment")
    processor_name: DprProcessor | str = Field(description="DPR processor name")
    processor_version: str = Field(description="DPR processor version")
    dask_cluster_label: str = Field(description='Dask cluster label e.g. "dask-l0"')
    dask_cluster_instance: str | None = Field(
        default=None,
        description="Optional Dask cluster instance ID used to build a public dashboard URL.",
    )
    s3_payload_file: str = Field(description="S3 path where the processor payload will be written")
    # 'pipeline' or 'unit' must be provided
    pipeline: DprPipeline | str | None = Field(
        default=None,
        description="Processor pipeline name. The task table propose one or several pipelines. "
        "Mandatory if unit is not provided.",
    )
    unit: str | None = Field(
        default=None,
        description="Processor unit name. Advanced users can call directly a single unit of the task table. "
        "Mandatory if pipeline is not provided.",
    )

    priority: Priority = Field(
        default=Priority.LOW,
        description="Priority for the cluster dask to be able to prioritise task execution. Default: `low`.",
    )
    workflow_type: WorkflowType = Field(
        default=WorkflowType.ON_DEMAND,
        description="Workflow type (benchmarking, on-demand, systematic). Default: `on-demand`.",
    )

    input_products: list[dict[str, tuple[str, str]]] = Field(
        description="List of input products for the processor, structured as follows: "
        "`input_products.name, (stac item identifier, collection name)`. "
        'Example: `[( "S1CADUS", ["S1A1234", "s01-cadip-session"])]`',
    )
    generated_product_to_collection_identifier: list[dict[str, str | tuple[str, str]]] = Field(
        description="List of output products for the processor, structured as follows: "
        "`output_products.name, (product:type, collection name)` "
        "or "
        "`product:type`. "
        "When the collection name is not specified, it is equal to `product:type`. "
        'Example: `[( "SRAL0", "s03sral0_" ),( "MWRL0", "s03mwrl0", "my-collection" )]`',
    )
    auxiliary_product_to_collection_identifier: dict[str, str] = Field(
        default_factory=dict,
        description="Collection name where to push each auxiliary file (in rs-catalog). "
        "To apply the same treatment to all product types simultaneously, a `*` wildcard can be used. "
        "By default (when no input is provided), the collection name is set to `<mission>-aux-<product:type>`",
    )

    processing_mode: list[ProcessingMode] = Field(
        default_factory=list,
        description="List of modes to be applied when calling the DPR processor.",
    )
    start_datetime: datetime | None = Field(
        default=None,
        description="Date that can be used to retrieve auxiliary data on the right time frame.",
    )
    end_datetime: datetime | None = Field(
        default=None,
        description="Date that can be used to retrieve auxiliary data on the right time frame.",
    )
    satellite: SentinelSatellite | str | None = Field(
        default=None,
        description="In certain CQL2 queries from task tables, the `<satellite>` parameter must be provided, "
        "as some auxiliary files depend on the satellite.",
    )

    @field_validator("processor_name", mode="before")
    @classmethod
    def normalize_processor_name(cls, v):
        """Normalize the processor name to a string."""
        return v.value if isinstance(v, DprProcessor) else v

    @field_validator("satellite", mode="before")
    @classmethod
    def normalize_satellite_name(cls, v):
        """Normalize the satellite name to a string."""
        return v.value if isinstance(v, SentinelSatellite) else v

    @model_validator(mode="after")
    def check_model(self):
        """
        Ensure required inputs are not empty and that exactly one of 'pipeline' or 'unit' is provided.

        The caller must specify either a pipeline or a unit, but not both
        and not neither.
        """
        has_pipeline = bool(self.pipeline)
        has_unit = bool(self.unit)
        if has_pipeline == has_unit:
            raise ValueError("Exactly one of 'pipeline' or 'unit' must be provided.")

        if not self.input_products:
            raise ValueError("'input_products' must contain at least one pystac.Item.")

        if not self.generated_product_to_collection_identifier:
            raise ValueError("'generated_product_to_collection_identifier' must not be empty.")

        if not self.auxiliary_product_to_collection_identifier:
            raise ValueError("'auxiliary_product_to_collection_identifier' must not be empty.")

        return self


@dataclass
class DprProcessOut:
    """
    Output parameters for the 'dpr-process' flow
    """

    status: bool
    product_identifier: list[Item] = field(default_factory=list)
