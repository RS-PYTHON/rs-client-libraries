# Copyright 2024 CS Group
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

"""OpenTelemetry utility"""

import inspect
import os
import pkgutil
import sys

import opentelemetry.instrumentation
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
from opentelemetry.instrumentation.aws_lambda import AwsLambdaInstrumentor
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from rs_workflows.utils.logging import Logging

logger = Logging.default(__name__)


def init_traces(service_name: str):
    """
    Init instrumentation of OpenTelemetry traces.

    Args:
        service_name (str): service name
    """

    # See: https://github.com/softwarebloat/python-tracing-demo/tree/main

    tempo_endpoint = os.getenv("TEMPO_ENDPOINT")
    if not tempo_endpoint:
        return

    otel_resource = Resource(attributes={"service.name": service_name})
    otel_tracer = TracerProvider(resource=otel_resource)
    trace.set_tracer_provider(otel_tracer)
    otel_tracer.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=tempo_endpoint)))

    # Instrument all the dependencies under opentelemetry.instrumentation.*
    # NOTE: we need 'poetry run opentelemetry-bootstrap -a install' to install these.

    package = opentelemetry.instrumentation
    prefix = package.__name__ + "."
    classes = set()

    # We need an empty PYTHONPATH if the env var is missing
    os.environ["PYTHONPATH"] = os.getenv("PYTHONPATH", "")

    # Recursively find all package modules
    for _, module_str, _ in pkgutil.walk_packages(path=package.__path__, prefix=prefix, onerror=None):
        # Don't instrument these modules, they have errors, maybe we should see why
        if module_str in ["opentelemetry.instrumentation.tortoiseorm"]:
            continue

        # Import and find all module classes
        __import__(module_str)
        for _, _class in inspect.getmembers(sys.modules[module_str]):
            if (not inspect.isclass(_class)) or (_class in classes):
                continue

            # Save the class (classes are found several times when imported by other modules)
            classes.add(_class)

            # Don't instrument these classes, they have errors, maybe we should see why
            if _class in [AsyncioInstrumentor, AwsLambdaInstrumentor, BaseInstrumentor]:
                continue

            # If the "instrument" method exists, call it
            _instrument = getattr(_class, "instrument", None)
            if callable(_instrument):
                _class_instance = _class()
                if not _class_instance.is_instrumented_by_opentelemetry:
                    _class_instance.instrument(tracer_provider=otel_tracer)
                # name = f"{module_str}.{_class.__name__}".removeprefix(prefix)
                # logger.debug(f"OpenTelemetry instrumentation of {name!r}")
