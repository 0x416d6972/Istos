---
title: Capability Discovery
---

# Capability Discovery

Liveliness tells you a peer is up. `.istos/capabilities` tells you what it
exposes — which `@handle` / `@stream` keys, plus JSON Schema from their type
hints. On by default.

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

Shape of each entry: `prefix`, `kind`, optional `description` (docstring),
`params_schema` / `return_schema` when types exist. Same schemas AsyncAPI uses.
Non-Python peers can serve the same key — see
[Wire Protocol](../reference/wire-protocol.md).

The endpoint uses the app-wide `authorizer`, same as health/metrics. Lock it
down if you don't want strangers enumerating your tools.

See also: [Liveliness](liveliness.md), [HTTP Gateway](http-gateway.md),
[Authorization](authorization.md).
