"""Optional OpenTelemetry tracing for API requests.

Tracing stays off unless ``OTEL_ENABLED`` is set. Export failures are logged by
the SDK and do not change the HTTP response.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI

from titan_api.config import ApiSettings

logger = logging.getLogger(__name__)


def maybe_enable_tracing(app: FastAPI, settings: ApiSettings) -> None:
    if not settings.otel_enabled:
        return
    endpoint = settings.otel_exporter_otlp_endpoint.strip()
    if not endpoint:
        logger.warning("OpenTelemetry is enabled but no exporter endpoint is set")
        return
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": "titan-api"}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, timeout=2)))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
    logger.info("OpenTelemetry export enabled for titan-api")
