"""Common modules."""

# Configure OpenTelemetry
from rs_common import opentelemetry

opentelemetry.init_traces("rs.client")
