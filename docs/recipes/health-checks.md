# Recipe: Health checks

Expose liveness/readiness and add a custom readiness probe.

```python
from istos import Istos

istos = Istos(enable_health=True)

async def check_cache():
    # Return {"status": "ok"} when healthy
    return {"status": "ok", "entries": 12}

istos.add_health_check("cache", check_cache)

@istos.handle("service/ping")
async def ping():
    return {"pong": True}

# From another peer (or ops tooling) after the fabric is up:
# await istos.query_once(".istos/health")
# await istos.query_once(".istos/ready")
```

If you set an app-wide `authorizer`, callers must pass `token=` for these endpoints too.

See [Observability](../user-guide/observability.md) and [Deployment](../user-guide/deployment.md).
