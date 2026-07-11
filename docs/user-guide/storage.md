# Storage

Istos uses a pluggable **storage** layer for handler durability metadata, event logs, and idempotency (exactly-once processing). This is separate from [brokerless durable pub/sub](durable-messaging.md) (`durable=True` on `@publish` / `@subscribe`).

## Backends

| Plugin | Use case | Install |
|--------|----------|---------|
| `InMemoryStoragePlugin` | Dev / tests | Built-in |
| `RedisStoragePlugin` | Shared ledger across processes | `pip install 'istos[redis]'` |
| `SqlAlchemyStoragePlugin` | Any SQL DB via async URL | `pip install 'istos[sqlalchemy]'` + driver |

## In-memory (default)

```python
from istos import Istos
from istos.consistency import InMemoryStoragePlugin

istos = Istos(storage=InMemoryStoragePlugin())
```

## Redis

```bash
pip install 'istos[redis]'
```

```python
from istos import Istos
from istos.consistency import RedisStoragePlugin

istos = Istos(
    storage=RedisStoragePlugin(
        url="redis://localhost:6379/0",
        prefix="istos:",
    )
)
```

With Docker Compose from the repo:

```bash
docker compose up -d redis
# redis://127.0.0.1:6379/0
```

## SQLAlchemy (any SQL database)

```bash
pip install 'istos[sqlalchemy]' asyncpg   # example: Postgres
```

```python
from istos import Istos
from istos.consistency import SqlAlchemyStoragePlugin

storage = SqlAlchemyStoragePlugin(
    "postgresql+asyncpg://user:pass@db:5432/istos"
)
istos = Istos(storage=storage)
```

Other URL examples: `sqlite+aiosqlite:///./istos.db`, `mysql+asyncmy://...`.

## Handler durability modes

```python
@istos.handle("payments/charge", durability="exactly_once")
async def charge(payment_id: str, amount: float):
    return {"charged": amount}
```

| Mode | Behavior |
|------|----------|
| `at_most_once` | Default — no idempotency ledger |
| `at_least_once` | Events logged to storage |
| `exactly_once` | Idempotency key + cached result |

All handlers share the app-wide durability ledger configured on the `Istos`
instance (`storage=` / `storage_config=` / `storage_database=`); its lifecycle
(connection pools, engines) is managed and disposed on shutdown.

## Config helpers

Use `DatabaseConfig` (env prefix `ISTOS_DB_`) when you prefer settings objects:

```python
from istos import Istos
from istos.consistency import DatabaseConfig, SqlAlchemyStoragePlugin

cfg = DatabaseConfig(
    backend="postgresql",
    driver="asyncpg",
    host="db",
    database="istos",
    username="svc",
    password="s3cret",
)
istos = Istos(storage=SqlAlchemyStoragePlugin.from_config(cfg))
```

Named application databases (not the durability ledger) use `databases=` and `app.db_session("name")` — see [Dependency Injection](dependency-injection.md).

## Next Steps

- [Brokerless Durable Messaging](durable-messaging.md) — peer replay without Redis/Kafka
- [Deployment](deployment.md) — Redis/Postgres in Compose
- [Recipe: Redis storage](../recipes/redis-storage.md)
