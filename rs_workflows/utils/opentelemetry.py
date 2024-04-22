"""OpenTelemetry utility"""

import inspect
import os
import pkgutil
import sys

import opentelemetry.instrumentation
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
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

    # Don't instrument asyncio (it has error, we should see why)
    ignored_classes = [AsyncioInstrumentor]

    package = opentelemetry.instrumentation
    prefix = package.__name__ + "."
    classes = set()

    # We need an empty PYTHONPATH if the env var is missing
    os.environ["PYTHONPATH"] = os.getenv("PYTHONPATH", "")

    # Recursively find all package modules
    for _, module_str, _ in pkgutil.walk_packages(path=package.__path__, prefix=prefix, onerror=None):
        try:
            # Import and find all module classes
            __import__(module_str)
            for _, _class in inspect.getmembers(sys.modules[module_str]):
                if inspect.isclass(_class) and (_class not in classes):
                    # Save the class (classes are found several times when imported by other modules)
                    classes.add(_class)

                    if _class in ignored_classes:
                        continue

                    # If the "instrument" method exists
                    _instrument = getattr(_class, "instrument", None)
                    if callable(_instrument):
                        # Call it with the same arguments than FastAPI
                        _class_instance = _class()
                        if not _class_instance.is_instrumented_by_opentelemetry:
                            _class_instance.instrument(tracer_provider=otel_tracer)
                        # name = f"{module_str}.{_class.__name__}".removeprefix(prefix)
                        # logger.debug(f"OpenTelemetry instrumentation of {name!r}")

        # Ignore these exceptions raised by some dependency, don't load the faulty module
        except (TypeError, AttributeError, ModuleNotFoundError):
            pass
