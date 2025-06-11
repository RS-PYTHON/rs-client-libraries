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

"""Utility module for the Prefect flows."""

import os
from collections.abc import Iterator
from enum import Enum

from opentelemetry import trace
from opentelemetry.trace import Span, SpanContext
from opentelemetry.util._decorator import _agnosticcontextmanager
from prefect import get_run_logger
from pydantic import BaseModel, Field

from rs_client.rs_client import RsClient
from rs_common import init_opentelemetry, prefect_utils


class ProcessorEnum(str, Enum):
    """DPR processor name"""

    # String value = resource name in the rs-dpr-service
    S1L0 = "s1_l0"
    S3L0 = "s3_l0"


class FlowEnvArgs(BaseModel):
    """
    Prefect flow environment, as a Pydantic serializable object.

    Attributes:
        owner_id: User/owner ID (necessary to retrieve the user info: API key and OAuth2 cookie)
        from the right Prefect block. NOTE: may be useless after each user has their own prefect
        server because there will be only one block.
        calling_span (tuple): Serialized OpenTelemetry span of the calling flow, if any.
    """

    owner_id: str = Field(description="User/owner ID")
    calling_span: tuple[int, int, bool] | None = Field(description="OpenTelemetry info", default=None)


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
