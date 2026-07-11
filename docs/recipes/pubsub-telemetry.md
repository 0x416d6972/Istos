# Recipe: Pub/Sub telemetry

Broadcast telemetry and react to it on the same fabric.

```python
from contextlib import asynccontextmanager
from istos import Istos

istos = Istos()

@istos.subscribe("drone/telemetry")
async def on_telemetry(data: dict):
    print(f"battery={data['battery']} altitude={data['altitude']}")

@istos.publish("drone/telemetry")
async def emit_telemetry(battery: int, altitude: int):
    return {"battery": battery, "altitude": altitude}

@asynccontextmanager
async def on_start(app):
    await emit_telemetry(battery=85, altitude=120)
    # or: await app.publish_once("drone/telemetry", {"battery": 85, "altitude": 120})
    yield

istos.lifespan = on_start

if __name__ == "__main__":
    istos.run()
```

For continuous publishing, schedule work from lifespan (background task) or call `publish_once` from a timer — always after the session is open.

See [Publish & Subscribe](../user-guide/pubsub.md).
