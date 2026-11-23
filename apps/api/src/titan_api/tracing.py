"""Optional OpenTelemetry tracing for API requests.

Tracing stays off unless ``OTEL_ENABLED`` is set. The sampler keeps a trace
when the parent was sampled, and samples a local root. Export failures are
logged by the SDK and do not change the HTTP response.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter

from titan_api.config import ApiSettings

logger = logging.getLogger(__name__)

_provider: TracerProvider | None = None


def maybe_enable_tracing(app: FastAPI, settings: ApiSettings) -> None:
    if not settings.otel_enabled:
        return
    endpoint = settings.otel_exporter_otlp_endpoint.strip()
    if not endpoint:
        logger.warning("OpenTelemetry is enabled but no exporter endpoint is set")
        return
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    enable_tracing(app, OTLPSpanExporter(endpoint=endpoint, timeout=2), batch=True)
    logger.info("OpenTelemetry export enabled for titan-api")


def enable_tracing(app: FastAPI, exporter: SpanExporter, *, batch: bool) -> TracerProvider:
    """Install one tracer provider and W3C propagation for this process."""
    global _provider
    from opentelemetry import trace
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ParentBased
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    set_global_textmap(TraceContextTextMapPropagator())
    current = trace.get_tracer_provider()
    if not isinstance(current, trace.ProxyTracerProvider):
        FastAPIInstrumentor.instrument_app(app)
        logger.warning("OpenTelemetry provider is already configured; leaving it in place")
        if isinstance(current, TracerProvider):
            return current
        raise RuntimeError("OpenTelemetry provider is already configured")
    provider = TracerProvider(
        resource=Resource.create({"service.name": "titan-api"}),
        sampler=ParentBased(ALWAYS_ON),
    )
    processor = BatchSpanProcessor(exporter) if batch else SimpleSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
    _provider = provider
    return provider


def shutdown_tracing() -> None:
    global _provider
    provider = _provider
    _provider = None
    if provider is not None:
        provider.shutdown()
