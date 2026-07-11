# Observability

Istos ships structured logging, Zenoh-addressable health/readiness/metrics endpoints, and optional OpenTelemetry tracing.

## Structured logging

```python
from istos import Istos

istos = Istos(
    configure_logging=True,
    log_level="INFO",
    json_logs=True,  # JSON for Loki / ELK / Datadog
)
```

By default Istos follows the stdlib convention: it logs under `istos.*` and does not reconfigure your app's root logger unless you opt in with `configure_logging=True` or call `configure_logging()` yourself.

```python
from istos import get_logger

log = get_logger("myapp.orders")
log.info("order created", extra={"order_id": 42})
```

## Health and readiness

When `enable_health=True` (default), Istos registers:

| Endpoint | Purpose |
|----------|---------|
| `.istos/health` | Liveness — process is alive |
| `.istos/ready` | Readiness — ready for traffic (+ custom checks) |

```python
health = await istos.query_once(".istos/health")
# {"status": "alive", "uptime_seconds": 12.3}

ready = await istos.query_once(".istos/ready")
# {"status": "ready", "checks": {...}}
```

### Custom readiness checks

```python
async def check_database():
    await db.ping()
    return {"status": "ok", "connections": 5}

istos.add_health_check("database", check_database)
```

A check must return a dict with `"status": "ok"` when healthy. Any other status (or a raised exception) marks the service `not_ready`.

!!! note "Authorization"
    Built-in endpoints inherit the app-wide `authorizer`. See [Security](security.md).

## Prometheus metrics

When `enable_metrics=True`:

| Endpoint | Purpose |
|----------|---------|
| `.istos/metrics` | Prometheus text exposition |

```python
istos = Istos(enable_metrics=True)

# Or export in-process:
print(istos.metrics.export_prometheus())
```

Handler latency and request counts are recorded via the metrics middleware when metrics are enabled.

## OpenTelemetry tracing

```bash
pip install 'istos[otel]'
```

```python
istos = Istos(
    enable_tracing=True,
    tracing_endpoint="http://otel-collector:4317",
    service_name="robot-fleet",
)
```

Spans are created around handler invocations when tracing middleware is active.

## Production bundle

```python
from istos import Istos
from istos.communication.sessions import IstosZenohConfig

istos = Istos(
    config=IstosZenohConfig(),
    log_level="INFO",
    json_logs=True,
    enable_health=True,
    enable_metrics=True,
    enable_tracing=True,
    tracing_endpoint="http://otel-collector:4317",
    service_name="robot-fleet",
)
```

## Next Steps

- [Deployment](deployment.md) — Docker Compose and Kubernetes probes
- [Middleware](middleware.md) — custom cross-cutting logic
- [Recipe: Health checks](../recipes/health-checks.md)
