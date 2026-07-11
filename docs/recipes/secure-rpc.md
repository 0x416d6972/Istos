# Recipe: Secure RPC

Combine transport config with a token authorizer.

```python
from istos import Istos, TokenAuthorizer
from istos.communication.sessions import IstosZenohConfig

# Prefer env / .env in real deployments:
# ISTOS_ZENOH_MODE=client
# ISTOS_ZENOH_CONNECT_ENDPOINTS=["tls/zenoh-router:7447"]
# ISTOS_ZENOH_USERNAME=...
# ISTOS_ZENOH_PASSWORD=...
# ISTOS_ZENOH_ROOT_CA_CERTIFICATE=...

config = IstosZenohConfig(
    mode="client",
    connect_endpoints=["tcp/127.0.0.1:7447"],  # use tls/... in production
)

istos = Istos(
    config=config,
    authorizer=TokenAuthorizer("super-secret-token"),
)

@istos.handle("fleet/status")
async def status():
    return {"ok": True}

# Caller (after session is open):
# await istos.query_once("fleet/status", attachment="super-secret-token")
```

Per-handler override:

```python
from istos import AuthContext

def admins_only(ctx: AuthContext) -> bool:
    return ctx.token in {"alice-key", "bob-key"}

@istos.handle("fleet/shutdown", authorizer=admins_only)
async def shutdown():
    return {"stopping": True}
```

See [Security & TLS](../user-guide/security.md).
