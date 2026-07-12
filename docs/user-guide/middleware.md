# Middleware

Middleware wraps `@handle` invocations for cross-cutting concerns: logging, correlation IDs, metrics, tracing, and custom logic.

Istos installs a default stack (correlation ID + logging). When enabled, Prometheus and OpenTelemetry middleware are added automatically.

## Adding middleware

```python
from istos import Istos
from istos.middleware import RequestScope
from istos.middleware.base import HandlerCallable

istos = Istos()

class TimingMiddleware:
    async def __call__(self, scope: RequestScope, call_next: HandlerCallable):
        # before handler
        result = await call_next(scope)
        # after handler
        return result

istos.add_middleware(TimingMiddleware())
```

Middleware runs outermost-first for the request path (last added is outermost).

## Request scope

Each invocation receives a `RequestScope`:

| Field | Meaning |
|-------|---------|
| `prefix` | Handler key expression |
| `operation` | `"handle"`, `"subscribe"`, `"publish"`, or `"query"` |
| `params` | Parsed query / call parameters |
| `context` | Request context (includes `correlation_id`) |

## Correlation IDs

`CorrelationIdMiddleware` (built-in) sets a per-request correlation ID on the context. Structured logs include it when you use Istos logging.

```python
from istos.context import get_request_context

@istos.handle("orders/create")
async def create(order_id: int):
    ctx = get_request_context()
    # ctx.correlation_id is available for your own logs / downstream calls
    return {"order_id": order_id, "correlation_id": ctx.correlation_id}
```

## Exception handlers

Register typed exception handlers:

```python
from istos import Istos, IstosError

istos = Istos()

class QuotaExceeded(IstosError):
    pass

@istos.exception_handler(QuotaExceeded)
async def on_quota(exc: QuotaExceeded):
    return {"error": "quota_exceeded", "message": str(exc)}

@istos.handle("jobs/enqueue")
async def enqueue(job_id: str):
    if over_quota(job_id):
        raise QuotaExceeded("daily limit reached")
    return {"queued": job_id}
```

Unhandled exceptions become a standardized `ErrorResponse` on the wire (`code`, `message`, `correlation_id`, `details`).

## Rate limiting

`RateLimitMiddleware` enforces a token bucket per key and raises `RateLimitError`
(429) when it's empty:

```python
from istos.middleware import RateLimitMiddleware

istos.add_middleware(RateLimitMiddleware(rate=10, per=1.0))            # 10/s per identity
istos.add_middleware(RateLimitMiddleware(rate=100, per=60,
                                         key=lambda s: s.prefix))       # 100/min per endpoint
```

By default it keys on the authenticated principal (`principal.id`), so each
caller gets their own quota; pass `key=` to limit per endpoint, per tenant, or
globally. `burst` (default `rate`) sets how many requests may arrive at once
before the steady rate applies. The raised error carries `details.retry_after`,
and over the HTTP gateway it maps to a 429.

## Scope note

Built-in middleware focuses on **handlers** (`@handle`). For pub/sub, prefer dependencies, lifespan resources, or logic inside the subscriber/publisher.

## Next Steps

- [Observability](observability.md) — metrics and tracing middleware
- [Recipe: Custom middleware](../recipes/custom-middleware.md)
