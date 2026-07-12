# Dependency Injection

Istos resolves `Depends` on `@handle`, `@stream`, `@subscribe`, `@publish`, `@query`, and `@on_liveliness`, and supports pluggable serializers, storage, sessions, and routers.

## `Depends`

```python
from typing import Annotated
from istos import Istos, Depends

istos = Istos()

async def get_db():
    conn = await open_conn()
    try:
        yield conn  # torn down after the invocation
    finally:
        await conn.close()

@istos.handle("orders/create")
async def create(order_id: int, db: Annotated[object, Depends(get_db)]):
    return await db.insert(order_id)
```

You can also use the default-argument form: `db=Depends(get_db)`.

### Behavior

| Rule | Detail |
|------|--------|
| Cache | Dependencies are cached per invocation (`use_cache=True` by default) |
| Sync deps | Offloaded to a thread so they do not block the event loop |
| Generators | `yield` deps run teardown via `AsyncExitStack` |
| Cycles | Circular graphs raise `DependencyCycleError` |
| Overrides | `istos.dependency_overrides[dep] = fake` for tests |

### Streaming note

On `@stream`, the dependency scope stays open for the **whole stream** (teardown
runs after the last chunk). On `@subscribe` / `@publish`, dependencies resolve
**per message**. For expensive shared resources (DB pool, socket), create them
once in `lifespan` and inject a cheap `Depends` that returns the shared instance.

### Named databases

For SQLAlchemy app DBs, prefer `Depends(istos.db_session("app"))` — see [Application Databases](application-databases.md).

## Per-decorator serialization

```python
from istos.messages.serialization import MsgPackSerializer

@istos.handle("sensor/data", serializer=MsgPackSerializer())
async def sensor(data):
    return {"processed": True}
```

| Serializer | Format |
|-----------|--------|
| `JsonSerializer` | JSON (default) |
| `MsgPackSerializer` | MessagePack |
| `RawSerializer` | bytes/str passthrough |
| `PydanticSerializer` | Pydantic-focused |
| `ProtobufSerializer` | Protobuf |
| `YamlSerializer` | YAML |
| `Base64Serializer` | Base64 wrapper |

There is **no** pickle serializer (security). Implement the `Serialize` protocol for custom codecs — see [Serialization API](../api/messages/serialization.md).

## Storage & sessions

```python
from istos.consistency import InMemoryStoragePlugin
from istos.communication.sessions import IstosZenohConfig

istos = Istos(
    storage=InMemoryStoragePlugin(),
    config=IstosZenohConfig(),  # or session_manager=...
)
```

A handler that declares `db: StoragePlugin` receives the app-wide storage
backend. See [Storage](storage.md).

## Modular routes with `IstosRouter`

```python
from istos import Istos, IstosRouter

robot = IstosRouter(prefix="fleet")

@robot.handle("move")
async def move(distance: int):
    return {"moved": distance}

istos = Istos()
istos.include_router(robot)  # → fleet/move
```

!!! note "Router parity"
    Prefer registering durable/authz options on `Istos` decorators when a router path does not expose the same kwargs yet.

## Next Steps

- [Application Databases](application-databases.md)
- [Middleware](middleware.md)
- [Testing](testing.md) — `dependency_overrides`
- [API: Depends](../api/di/depends.md)
