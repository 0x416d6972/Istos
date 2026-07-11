"""OpenTelemetry tracing integration (optional dependency)."""

from __future__ import annotations

from typing import Any, Optional

from istos.middleware.base import HandlerCallable, RequestScope

_tracer: Any = None


def configure_tracing(
    service_name: str = "istos",
    endpoint: Optional[str] = None,
) -> bool:
    """
    Configure OpenTelemetry tracing. Returns True if configured, False if
    opentelemetry is not installed.
    """
    global _tracer
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    except ImportError:
        return False

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    if endpoint:
        exporter = OTLPSpanExporter(endpoint=endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("istos")
    return True


class TracingMiddleware:
    """Creates an OpenTelemetry span per request."""

    async def __call__(
        self,
        scope: RequestScope,
        call_next: HandlerCallable,
    ) -> Any:
        if _tracer is None:
            return await call_next(scope)

        with _tracer.start_as_current_span(
            f"istos.{scope.operation}",
            attributes={
                "istos.prefix": scope.prefix,
                "istos.operation": scope.operation,
                "istos.correlation_id": scope.context.correlation_id,
            },
        ):
            return await call_next(scope)
