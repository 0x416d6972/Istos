# Istos

<p align="center">
  <em>A unified Python framework for building robust distributed systems and multi-agent applications over the <a href="https://zenoh.io/">Eclipse Zenoh</a> protocol.</em>
</p>

**Istos** provides a ridiculously simple, decorator-based API that strips away the complexities of networking, serialization, and state distribution. By extending Zenoh's high-performance publish/subscribe and query functionalities, Istos helps you quickly wire up event-driven microservices or distributed agents in native Python.

---

##  Key Features

- **Decorators First**: Write clean business logic. Turn any Python function into a network-addressable handler, subscriber, or publisher using intuitive decorators (`@istos.handle`, `@istos.publish`, `@istos.subscribe`).
- **Smart Selectors & RPC**: Automatically map Zenoh query parameters (e.g., `?limit=5&role=admin`) directly to your function's Python arguments.
- **Schema Validation**: Automatic type coercion and Pydantic model validation at the network boundary — bad data is rejected before your code runs.
- **Retry Policies**: Built-in exponential backoff retries for queries and subscribers via a simple `retry=5` parameter.
- **Pub/Sub Made Easy**: Broadcast real-time state changes and react instantly across your network with minimal boilerplate.
- **Async & Sync Compatibility**: First-class support for asynchronous `asyncio` code with automatic looping for synchronous environments (`istos.run()` vs `await istos.run_async()`).
- **Pluggable Architecture**: Inject custom behavior via simple abstractions:
  - **Storage:** Use `InMemoryStoragePlugin` or drop in a SQLite backend.
  - **Serialization:** Built-in `JsonSerializer` powered by Pydantic and msgpack integrations.

##  The Mental Model
The framework abstracts network topology into three clear concepts:
- **`@handle` & `@query`**: 1-to-1 RPC (Request & Reply)
- **`@publish` & `@subscribe`**: 1-to-Many Streaming (Fire and Forget)
- **`@on_liveliness`**: Infrastructure Awareness (Node Discovery & Health)

##  Installation

This project uses modern Python packaging via [`uv`](https://github.com/astral-sh/uv).

```bash
# Standard installation
uv pip install istos

# Or install from source:
git clone https://github.com/your-repo/istos.git
cd istos
uv pip install -e .

# Install with optional SQLite support
uv pip install -e ".[sqlite]"
```

##  Quick Start

### 1. Registering a Handler
Handlers sit on the network and respond to incoming queries. Istos automatically parses query parameters into your function's arguments.

```python
import asyncio
from istos import Istos

istos = Istos()

@istos.handle(prefix="robot/move")
async def move(distance: int, speed: str = "normal"):
    """
    Called when a Zenoh Query hits 'robot/move'.
    E.g., Querying 'robot/move?distance=10&speed=fast' automatically binds:
    distance=10, speed='fast'
    """
    return {"status": "success", "distance": int(distance), "speed": speed}

if __name__ == "__main__":
    # Blocks and listens for queries
    istos.run()
```

### 2. Querying the Network
You can easily query handlers registered anywhere on the Zenoh network using `kwargs` to build Zenoh Selectors.

```python
import asyncio
from istos import Istos

istos = Istos()

async def query_robot():
    # Translates strictly to -> GET "robot/move?distance=15&speed=fast"
    result = await istos.query_once("robot/move", distance=15, speed="fast")
    print(f"Robot replied: {result}")

if __name__ == "__main__":
    asyncio.run(query_robot())
```

### 3. Publishing & Subscribing (Event-Driven)
React to real-time events efficiently.

```python
from istos import Istos

istos = Istos()

# --- Subscriber ---
@istos.subscribe("drone/telemetry")
def on_telemetry(data):
    # Triggered automatically when data is pushed to "drone/telemetry"
    print(f"Received telemetry via network: {data}")

# --- Publisher ---
@istos.publish("drone/telemetry")
def get_telemetry():
    # The return value is automatically published to the network!
    return {"battery": 85, "altitude": 120}

if __name__ == "__main__":
    # Call the wrapped publisher function to publish the result
    get_telemetry()
    
    # Or publish arbitrary data independently
    # await istos.publish_once("drone/telemetry", {"battery": 80})

    istos.run()
```

### 4. Liveliness Tracking (Heartbeats)
Detect instantly when nodes connect or drop off the network without pinging.

```python
# Announce that this node is alive on the network
istos.declare_liveliness("robot/camera1")

# Listen to the network for connection state changes
@istos.on_liveliness("robot/**")
def status_changed(key_expr: str, is_alive: bool):
    if is_alive:
        print(f"Node connected: {key_expr}")
    else:
        print(f"ALERT: Node crashed/disconnected -> {key_expr}")
```

### 5. One-Shot Commands & State Clearing
Use raw async functions when you want to act imperatively rather than relying on events.

```python
# Quickly shoot out a piece of data
await istos.publish_once("fast/data/pulse", {"system": "ok"})

# Clear/erase network states, especially useful if using persistent StoragePlugins
await istos.delete_once("robot/cache/old_logs")
```

### 6. High-Performance Shared Memory (Zero-Copy)
When sending massive data arrays (like HD video frames) between handlers residing on the same hardware, drastically improve performance by enabling POSIX shared memory allocations.

```python
@istos.publish("video/feed", use_shm=True)
def send_frame():
    return large_data_array

# The framework automatically manages Zenoh ShmProviders natively!
```

### 7. Dependency Injection & Pluggability
Swap out underlying components entirely on startup:

```python
from istos import Istos
from istos.consistency.storage import InMemoryStoragePlugin
from istos.messages.serialization import JsonSerializer

istos = Istos(
    storage=InMemoryStoragePlugin(),
    serializer=JsonSerializer()
)
```

### 8. Retry Policies
Add automatic retries with exponential backoff to any query or subscriber. Pass a simple integer or a full `RetryPolicy` for fine-grained control.

```python
from istos.core.retry import RetryPolicy

# Simple — retry up to 5 times with default exponential backoff
@istos.query("weather/forecast", retry=5)
def get_forecast(result):
    return result

# Subscriber with retries — if processing crashes, it retries before giving up
@istos.subscribe("sensor/readings", retry=3)
def on_reading(data):
    save_to_database(data)  # retried automatically on transient failures

# Advanced — full control over backoff timing and failure handling
@istos.query("weather/forecast", retry=RetryPolicy(
    max_retries=10,
    delay=1.0,
    backoff_factor=3.0,
    on_failure=lambda e: print(f"Dead letter: {e}")
))
def get_forecast(result):
    return result
```

### 9. Schema Validation
Istos automatically validates and type-coerces incoming parameters at the network boundary — before your business logic runs. Supports three modes:

```python
from pydantic import BaseModel

# Mode 1: Type hints → auto-coercion
# Zenoh sends distance="15" (string), Istos casts it to int(15)
# Zenoh sends distance="hello" → rejected with a validation error reply
@istos.handle("robot/move")
async def move(distance: int, speed: str = "normal"):
    return {"moved": distance, "speed": speed}

# Mode 2: Pydantic BaseModel → full schema validation
class MoveRequest(BaseModel):
    distance: int
    speed: str = "normal"

@istos.handle("robot/move")
async def move(request: MoveRequest):
    # request is a fully validated Pydantic object with defaults applied
    return {"moved": request.distance}

# Mode 3: No type hints → passthrough (backward compatible)
@istos.handle("robot/echo")
async def echo(message):
    return {"echo": message}
```

### 10. Transport Security & Authentication
Secure your distributed system easily without polluting your code. `IstosZenohConfig` instantly loads properties from your environment (`.env` file or environment variables) using `pydantic-settings` under the hood.

```env
# .env file
ISTOS_ZENOH_MODE=client
ISTOS_ZENOH_CONNECT_ENDPOINTS=["tls/zenoh-router.local:7447"]

# Basic Auth
ISTOS_ZENOH_USERNAME=robot_1
ISTOS_ZENOH_PASSWORD=super_secret

# TLS / mTLS
ISTOS_ZENOH_ROOT_CA_CERTIFICATE=/path/to/ca.pem
# ISTOS_ZENOH_LISTEN_CERTIFICATE=/path/to/cert.pem
# ISTOS_ZENOH_LISTEN_PRIVATE_KEY=/path/to/key.pem
# ISTOS_ZENOH_ENABLE_MTLS=true
```

Then cleanly pass it to Istos:

```python
from istos import Istos
from istos.communication.sessions import AsyncZenohSession, IstosZenohConfig

# Auto-populates from .env!
config = IstosZenohConfig()

istos = Istos(
    session_manager=AsyncZenohSession(config.build())
)
```

#### Advanced Security: Vault & Secret Managers (Programmatic Raw PEM)
For enterprise zero-trust environments, Zenoh magically accepts **raw multiline PEM strings** natively, meaning you don't need to write files to disk. You can completely bypass `.env` and pull secrets dynamically into `IstosZenohConfig` during startup:

```python
# 1. Fetch from your favorite Secrets Manager (HashiCorp Vault, AWS, LDAP, etc.)
secrets = vault.get_secret("istos/prod")

# 2. Inject raw strings directly into the config builder
config = IstosZenohConfig(
    mode="client",
    connect_endpoints=["tls/zenoh-router.local:7447"],
    username=secrets["zenoh_user"],
    password=secrets["zenoh_pass"],
    root_ca_certificate=secrets["raw_ca_pem_string"] # 👈 Raw Multiline PEM!
)

istos = Istos(session_manager=AsyncZenohSession(config.build()))
```

##  Testing

Istos comes with a comprehensive suite of asynchronous tests utilizing `pytest` and `pytest-asyncio`. Run them easily:

```bash
uv pip install -e ".[dev]"
pytest tests/
```

## 👨‍💻 Contributing
Contributions and pull requests are welcome! Ensure tests pass and type hints are satisfied.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---
**License**: GPLv3
