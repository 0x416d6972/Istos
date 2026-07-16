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

Over the fabric, one service at a time:

```python
manifest = await app.query_once(".istos/capabilities/clients")
```

Or the whole fleet:

```python
fleet = await app.discover_capabilities()
# {"clients": {...}, "cdc": {...}}
for service, manifest in fleet.items():
    for tool in manifest["capabilities"]:
        print(service, tool["prefix"], tool.get("params_schema"))
```

That is the tool catalogue an agent needs: teach the fleet a new key and the
agent can find it with nothing but a schema.

## Two keys, and why

| Key | Answers |
|---|---|
| `.istos/capabilities/<service>` | that service |
| `.istos/capabilities` | **one** node, whichever Zenoh picked |

The bare key is the same on every node, and `@handle` declares its queryable
`complete=True`, meaning one responder can answer the whole key. Zenoh takes that
at its word and asks exactly one node; the others are never reached. A wildcard
does not help, because the key is byte-identical everywhere and still resolves to
that one key expression. It is kept for callers written against it, and it is fine
for a single node — but it cannot describe a fleet.

Per-service keys are distinct, which is the only thing Zenoh fans out over: the
same reason `*/health` reaches both `a/health` and `b/health`.

!!! note "Name your services"

    The key is the service name, so two services sharing one name share a key and
    only one of them answers. `Istos(service_name="clients")`, not the default.
    Replicas of a single service are *meant* to share a key: the manifest
    describes the service, not the process.

!!! warning "Fan-out needs `consolidate_replies=False`"

    Zenoh consolidates replies by default and drops some even when they arrive on
    different keys. `discover_capabilities()` handles this, but a hand-rolled
    wildcard needs it too:

    ```python
    await app.query_once(".istos/capabilities/*", consolidate_replies=False)
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
