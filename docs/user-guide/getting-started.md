# Getting Started with Istos

Build your first distributed service with Istos in a few minutes, then follow the tutorial track below.

## Installation

```bash
uv pip install istos
```

Or scaffold a project:

```bash
istos new my-service
cd my-service
uv pip install istos
python main.py
```

See the [CLI](cli.md) for more commands.

## Tutorial track

Work through these guides in order:

| Step | Guide | You will learn |
|------|-------|----------------|
| 1 | **This page** | Handlers, queries, pub/sub, validation |
| 2 | [Handlers & Queries (RPC)](rpc.md) | Request/reply and `@stream` in depth |
| 3 | [Channels & Agent Sessions](channels.md) | Duplex agents, WebSocket, SessionStore |
| 4 | [Publish & Subscribe](pubsub.md) | Event streaming |
| 5 | [Brokerless Durable Messaging](durable-messaging.md) | Late-join replay without a broker |
| 6 | [HTTP Gateway](http-gateway.md) | HTTP / SSE / MCP / FastAPI co-host |
| 7 | [Security & TLS](security.md) | Transport auth + handler authorization |
| 8 | [Deployment](deployment.md) | Docker, health, metrics, production config |

Supporting topics (any time after step 1): [Validation](validation.md), [Dependency Injection](dependency-injection.md), [Application Databases](application-databases.md), [Middleware](middleware.md), [Observability](observability.md), [Storage](storage.md), [Testing](testing.md), [CLI](cli.md), [Recipes](../recipes/index.md).

The full map of guides and APIs lives on the [Home](../index.md) page.

## Core Concepts

Istos provides a decorator-based API that maps directly to network operations:

| Decorator | Pattern | Direction | Description |
|-----------|---------|-----------|-------------|
| `@handle` | RPC | Receive | Listens for queries and replies |
| `@query` | RPC | Send | Sends queries and receives replies |
| `@stream` | Streaming RPC | Receive | Streams chunked replies (SLM/LLM tokens) |
| `@stream_client` | Streaming RPC | Send | Consumes a `@stream` (decorator form of `stream_query`) |
| `@channel` | Duplex | Receive | Interactive agent sessions (`send` / `receive`) |
| `@channel_client` | Duplex | Send | Opens a remote `@channel` (decorator form of `open_channel`) |
| `@subscribe` | Pub/Sub | Receive | Listens for published events |
| `@publish` | Pub/Sub | Send | Broadcasts events to the network |
| `@on_liveliness` | Discovery | Receive | Monitors node health |

## Your First Service

### Step 1: Create a Handler

Handlers sit on the network and respond to incoming queries. Istos automatically parses query parameters into your function's arguments.

```python
from istos import Istos

istos = Istos()

@istos.handle("robot/move")
async def move(distance: int, speed: str = "normal"):
    """Called when a query hits 'robot/move'."""
    return {"status": "success", "distance": distance, "speed": speed}

if __name__ == "__main__":
    istos.run()
```

### Step 2: Query the Handler

Queries use the service's shared Zenoh session. Call them only after the session is open — for example from a **lifespan** hook, another handler, or after `run_async()` has started.

```python
from contextlib import asynccontextmanager
from istos import Istos

istos = Istos()

@istos.query("robot/move")
async def query_robot(result):
    return result

@asynccontextmanager
async def on_start(app):
    reply = await query_robot(distance=15, speed="fast")
    print(f"Robot replied: {reply}")
    yield

istos.lifespan = on_start

if __name__ == "__main__":
    istos.run()
```

Or use the imperative API once the app is running:

```python
# Inside a handler or lifespan — not at import time
reply = await istos.query_once("robot/move", distance=15, speed="fast")
```

!!! warning "Session must be running"
    There is no per-call transient session. Calling `@query` / `@publish` (or `query_once` / `publish_once`) before `istos.run()` / `run_async()` raises `RuntimeError`.

!!! tip "Smart Selectors"
    `query_once("robot/move", distance=15, speed="fast")` becomes the Zenoh selector `robot/move?distance=15&speed=fast`. Your handler receives these as typed Python arguments.

### Step 3: Add Pub/Sub

React to real-time events. Publish from a lifespan (or handler) after the session is open:

```python
from contextlib import asynccontextmanager
from istos import Istos

istos = Istos()

@istos.subscribe("drone/telemetry")
def on_telemetry(data):
    print(f"Received telemetry: {data}")

@istos.publish("drone/telemetry")
async def get_telemetry():
    return {"battery": 85, "altitude": 120}

@asynccontextmanager
async def on_start(app):
    await get_telemetry()  # publishes after the session is open
    yield

istos.lifespan = on_start

if __name__ == "__main__":
    istos.run()
```

### Step 4: Add Liveliness Tracking

Detect when nodes come online or crash:

```python
istos.declare_liveliness("robot/camera1")

@istos.on_liveliness("robot/**")
def status_changed(key_expr: str, is_alive: bool):
    if is_alive:
        print(f"Node connected: {key_expr}")
    else:
        print(f"Node disconnected: {key_expr}")
```

## Schema Validation

Istos supports three validation modes:

=== "Type Hints"

    ```python
    @istos.handle("robot/move")
    async def move(distance: int, speed: str = "normal"):
        # distance="15" (string) is automatically cast to int(15)
        # distance="hello" → rejected with validation error
        return {"moved": distance}
    ```

=== "Pydantic Models"

    ```python
    from pydantic import BaseModel

    class MoveRequest(BaseModel):
        distance: int
        speed: str = "normal"

    @istos.handle("robot/move")
    async def move(request: MoveRequest):
        # Fully validated Pydantic object with defaults applied
        return {"moved": request.distance}
    ```

=== "No Validation"

    ```python
    @istos.handle("robot/echo")
    async def echo(message):
        # Raw passthrough — no type checking
        return {"echo": message}
    ```

## Built-in Documentation

Istos can auto-generate and serve an AsyncAPI documentation dashboard:

```python
istos.serve_docs(
    prefix=".istos/docs",
    title="My Robot Network",
    version="1.0.0",
    web_port=8080,
)
```

Then visit `http://localhost:8080` to see your live API documentation.

## Next Steps

Continue the tutorial track:

1. [Handlers & Queries (RPC)](rpc.md)
2. [Publish & Subscribe](pubsub.md)
3. [Brokerless Durable Messaging](durable-messaging.md)
4. [Security & TLS](security.md)
5. [Deployment](deployment.md)

Or jump to a [recipe](../recipes/index.md).
