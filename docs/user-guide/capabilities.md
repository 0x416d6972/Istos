---
title: Capability Discovery
---

# Capability Discovery

Liveliness tells you a peer is up. `.istos/capabilities` tells you what it
exposes — which `@handle` / `@stream` / `@channel` / `@publish` / `@subscribe`
keys, plus JSON Schema from type hints where they exist. On by default.

```python
from istos import Istos

app = Istos()                          # enable_discovery=True
# app = Istos(enable_discovery=False)  # turn it off
```

In-process:

```python
print(app.export_capabilities())
```

Over the fabric:

```python
caps = await app.query_once(".istos/capabilities")
# or: session.get("**/.istos/capabilities")
```

## Shape

Each entry has `prefix`, `kind`, optional `description` (docstring), and
`params_schema` / `return_schema` when types exist (same schemas AsyncAPI uses).

| `kind` | Source |
|--------|--------|
| `handle` | `@handle` |
| `stream` | `@stream` |
| `channel` | `@channel` — may also include `websocket` (path) when `ws=` is set |
| `publish` | `@publish` |
| `subscribe` | `@subscribe` |

`.istos/*` built-ins are hidden. Client-only decorators (`@query`,
`@stream_client`, `@channel_client`), liveliness callbacks, and `persist()`
roles are not listed.

Non-Python peers can serve the same key — see
[Wire Protocol](../reference/wire-protocol.md).

The endpoint uses the app-wide `authorizer`, same as health/metrics. Lock it
down if you don't want strangers enumerating your tools.

!!! note "MCP is a different surface"
    `enable_mcp=True` exposes `@handle` endpoints as MCP tools over HTTP.
    Capability discovery lists streams and channels too; MCP does not — see
    [HTTP Gateway](http-gateway.md#mcp-tools).

See also: [Liveliness](liveliness.md), [Channels](channels.md),
[HTTP Gateway](http-gateway.md), [Authorization](authorization.md).
