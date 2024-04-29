"""rs-workflows main module."""

# Set automatically by running `poetry dynamic-versioning`
__version__ = "0.0.0"

# Configure OpenTelemetry
from rs_workflows.utils import opentelemetry

opentelemetry.init_traces("rs.client")
