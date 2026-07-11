# Recipe: Production service

A compact production-shaped service: client mode, Redis ledger, JSON logs, health, metrics, and tracing.

```bash
pip install 'istos[redis,otel]'
# docker compose up -d zenoh-router redis
```

```python
from istos import Istos, TokenAuthorizer
from istos.communication.sessions import IstosZenohConfig
from istos.consistency import RedisStoragePlugin

config = IstosZenohConfig(
    mode="client",
    connect_endpoints=["tcp/127.0.0.1:7447"],
    # Production: tls/... endpoints, username/password, CA, multicast_scouting=False
)

istos = Istos(
    config=config,
    storage=RedisStoragePlugin(url="redis://127.0.0.1:6379/0"),
    authorizer=TokenAuthorizer("ops-token"),
    configure_logging=True,
    log_level="INFO",
    json_logs=True,
    enable_health=True,
    enable_metrics=True,
    enable_tracing=True,
    tracing_endpoint="http://127.0.0.1:4317",
    service_name="robot-fleet",
)

@istos.handle("fleet/status")
async def status():
    return {"service": "robot-fleet", "status": "ok"}

async def check_redis():
    return {"status": "ok"}

istos.add_health_check("redis", check_redis)

if __name__ == "__main__":
    istos.serve_docs(web_port=8080)  # protect via app authorizer
    istos.run()
```

See [Deployment](../user-guide/deployment.md), [Observability](../user-guide/observability.md), and [Security](../user-guide/security.md).
