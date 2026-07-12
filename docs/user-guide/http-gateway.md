---
title: HTTP Gateway & Probes
---

# HTTP Gateway, Health Probes & Metrics

Istos speaks Zenoh natively, but real deployments also need an HTTP surface:
non-Zenoh callers (a FastAPI frontend, a browser, an external partner) have to
reach your services, Kubernetes needs HTTP health probes, and Prometheus needs a
scrape endpoint. One embedded server provides all three.

Enable it with a port:

```python
from istos import Istos

app = Istos(http_port=8080)
```

That starts an aiohttp server exposing:

| Path | Purpose |
|---|---|
| `GET /livez`, `GET /healthz` | liveness probe (process is up) |
| `GET /readyz` | readiness probe (200 when ready, 503 otherwise) |
| `GET /metrics` | Prometheus text-format metrics |
| your gateway routes | HTTP → handler bridge (below) |

## HTTP ingress gateway (call Istos from FastAPI, browsers, curl)

Mark a handler with `http=` and it's reachable over HTTP as well as Zenoh:

```python
from pydantic import BaseModel
from istos import Istos, Principal, Depends, current_principal

app = Istos(http_port=8080, authorizer=my_authorizer)

class MoveRequest(BaseModel):
    distance: int

@app.handle("robot/move", http=True)   # POST /robot/move
async def move(req: MoveRequest, user: Principal = Depends(current_principal)):
    return {"moved": req.distance, "by": user.id}
```

Now any HTTP client can call it:

```bash
curl -X POST http://localhost:8080/robot/move \
     -H "Authorization: Bearer <token>" \
     -d '{"distance": 5}'
```

Under the hood the request is translated into a **Zenoh query** against
`robot/move`, so it runs through the *entire* handler pipeline — authorization,
validation, DI, middleware — with nothing bypassed. The request body and query
string become the handler's params; the reply becomes the JSON response.

`http=` forms:

- `http=True` → `POST /<prefix>`
- `http="GET /things"` → explicit method + path
- `http="/custom/path"` → `POST /custom/path`

### Auth is forwarded

The HTTP `Authorization` header is passed through as the Zenoh query
**attachment** — exactly where the authorizer reads the token (`current_token`).
So your authorizer gate and `Principal` work across the HTTP boundary: a request
with no/invalid token is denied with the mapped HTTP status, and a valid one
injects the resolved identity into the handler.

Istos error codes map to HTTP status: `unauthorized`→401, `not_found`→404,
`validation_error`→400, `rate_limit_exceeded`→429, other errors→500. A handler
that never replies yields 504.

### Calling Istos from FastAPI

Because it's just HTTP, a FastAPI service needs no Zenoh dependency:

```python
# In the FastAPI app — plain HTTP client:
async def move_robot(token: str):
    async with httpx.AsyncClient() as c:
        r = await c.post("http://istos-node:8080/robot/move",
                         json={"distance": 5},
                         headers={"Authorization": f"Bearer {token}"})
        return r.json()
```

!!! note "HTTP interop, not fabric membership"
    The gateway lets external callers *invoke* Istos handlers over HTTP. It does
    not make them Zenoh peers — they don't join the brokerless pub/sub fabric or
    get durable replay. If the other service is Python and you want full fabric
    membership (pub/sub, durability), embed the Zenoh client directly instead of
    going through HTTP.

### Streaming over HTTP (Server-Sent Events)

A `@stream` handler emits many chunks over one call — ideal for SLM/LLM token
streaming. Mark it with `http=` and the gateway bridges it to
`text/event-stream` (SSE), so a browser `EventSource` or a FastAPI proxy can
consume the tokens live:

```python
@app.stream("llm/generate", http=True)          # GET /llm/generate
async def generate(prompt: str):
    async for token in model.stream(prompt):
        yield token
```

Each yielded chunk becomes one SSE `data:` frame; the stream closes with an
`event: end` frame, or `event: error` carrying `{code, message}` if the handler
raises. SSE routes default to **`GET`** (what `EventSource` uses); pass an
explicit method to override (`http="POST /generate"`). `http_timeout_s` bounds
the whole stream (default 60s for long inference).

```javascript
// In the browser — no Zenoh, no framework:
const es = new EventSource("http://istos-node:8080/llm/generate?prompt=hello");
es.onmessage = (e) => append(e.data);            // each token
es.addEventListener("end",   () => es.close());
es.addEventListener("error", (e) => es.close());
```

The `Authorization` header and W3C trace headers (`traceparent`,
`X-Correlation-ID`) cross into the Zenoh envelope just as for one-shot routes, so
the stream's authorizer gate runs and correlation/trace propagate from the HTTP
edge. Consuming a stream **inside** the fabric (Python peer) still uses
[`stream_query`](rpc.md); SSE is the external-caller path.

## Kubernetes health probes

Point the kubelet at the HTTP probes:

```yaml
livenessProbe:
  httpGet: { path: /livez, port: 8080 }
readinessProbe:
  httpGet: { path: /readyz, port: 8080 }
```

`/readyz` returns 503 until the service has finished binding its handlers and
subscribers (and returns 503 again during shutdown), so Kubernetes only routes
traffic when the node is actually ready. Register custom readiness checks with
`app.add_health_check(name, check)` and they are reported in the `/readyz` body.

## Metrics

`GET /metrics` returns the built-in `MetricsCollector` in Prometheus text format
(request counts and latency histograms via the `PrometheusMiddleware`). Scrape it
directly — no extra dependency required.

## Next steps

- [Authorization](authorization.md) — the gate the gateway forwards tokens to
- [Observability](observability.md) — tracing and metrics
- [Deployment](deployment.md)
