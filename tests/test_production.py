"""Tests for production-readiness features."""

import pytest
from istos import Istos, IstosError, IstosTestClient
from istos.errors import ErrorResponse
from istos.validation import SchemaValidationError
from istos.http.health import HealthState
from istos.observability.metrics import MetricsCollector, PrometheusMiddleware
from istos.middleware.base import MiddlewareStack


@pytest.mark.asyncio
async def test_testclient_query():
    istos = Istos(enable_health=False, enable_metrics=False)

    @istos.handle("robot/move")
    async def move(distance: int):
        return {"moved": distance}

    client = IstosTestClient(istos)
    result = await client.query("robot/move", distance=10)
    assert result == {"moved": 10}


@pytest.mark.asyncio
async def test_testclient_publish_subscribe():
    istos = Istos(enable_health=False, enable_metrics=False)
    received = []

    @istos.subscribe("events/data")
    async def on_data(data):
        received.append(data)

    client = IstosTestClient(istos)
    await client.publish("events/data", {"value": 42})
    assert received == [{"value": 42}]


def test_exception_handler_registry():
    istos = Istos(enable_health=False, enable_metrics=False)

    @istos.exception_handler(IstosError)
    def handle_istos_error(exc: IstosError) -> ErrorResponse:
        return ErrorResponse(
            error=exc.code,
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )

    response = istos._exception_registry.resolve(
        IstosError("test error", code="test_code")
    )
    assert response.code == "test_code"


def test_exception_handler_validation_error():
    istos = Istos(enable_health=False, enable_metrics=False)
    exc = SchemaValidationError([{"msg": "bad"}], message="invalid")
    response = istos._exception_registry.resolve(exc)
    assert response.code == "validation_error"


@pytest.mark.asyncio
async def test_health_state():
    state = HealthState()
    state.ready = True
    liveness = await state.liveness()
    assert liveness["status"] == "alive"
    assert "uptime_seconds" in liveness

    readiness = await state.readiness()
    assert readiness["status"] == "ready"


def test_metrics_collector():
    collector = MetricsCollector()
    collector.increment("requests_total", {"operation": "handle"})
    collector.observe("request_duration_seconds", 0.05, {"operation": "handle"})
    output = collector.export_prometheus()
    assert "istos_requests_total" in output
    assert "_count" in output
    assert "request_duration_seconds" in output


@pytest.mark.asyncio
async def test_prometheus_middleware():
    collector = MetricsCollector()
    middleware = PrometheusMiddleware(collector)
    stack = MiddlewareStack([middleware])

    async def handler(scope):
        return "ok"

    from istos.middleware.base import RequestScope
    scope = RequestScope(prefix="test/handler", operation="handle")
    result = await stack.invoke(scope, handler)
    assert result == "ok"
    assert collector._counters


def test_builtin_handlers_registered():
    istos = Istos(enable_health=True, enable_metrics=True)
    istos._register_builtin_handlers()
    prefixes = [h.prefix for h in istos._handlers]
    assert ".istos/health" in prefixes
    assert ".istos/ready" in prefixes
    assert ".istos/metrics" in prefixes
