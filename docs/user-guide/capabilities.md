---
title: Capability Discovery
---

# Capability Discovery

Agents and orchestrators need to know **what tools a peer exposes** — not only
whether it is alive ([liveliness](liveliness.md)). Istos publishes a
machine-readable manifest at `.istos/capabilities` (enabled by default).

## Enable / disable

```python
from istos import Istos

app = Istos(enable_discovery=True)   # default — registers .istos/capabilities
# app = Istos(enable_discovery=False)
```

## What is advertised

Every `@handle` and `@stream` endpoint is listed with its key expression and
JSON Schema derived from type hints / Pydantic models (same source as AsyncAPI).

```python
manifest = app.export_capabilities()
# {
#   "service": "istos",
#   "capabilities": [
#     {"key_expr": "robot/move", "kind": "handle", "input_schema": {...}, ...},
#     ...
#   ]
# }
```

Query another node on the fabric:

```python
caps = await app.query_once(".istos/capabilities")
# or fleet-wide: session.get("**/.istos/capabilities")
```

## Agent / SLM pattern

1. Discover online peers with [liveliness](liveliness.md).
2. Query each peer's `.istos/capabilities` (or a wildcard) for tools.
3. Invoke tools with `@query` / `query_once` (or HTTP via the [gateway](http-gateway.md)).
4. Stream model output with `@stream` / SSE when the tool is progressive.

Polyglot peers can implement the same key and JSON shape — see the
[Wire Protocol](../reference/wire-protocol.md) reference.

## Authorization

Like other built-in endpoints, `.istos/capabilities` inherits the app-wide
`authorizer`. Gate discovery in production the same way you gate health/metrics.

## Related

- [HTTP Gateway](http-gateway.md) — expose selected tools over HTTP/SSE
- [Authorization](authorization.md) — JWT / token gates
- [Wire Protocol](../reference/wire-protocol.md) — normative manifest shape
