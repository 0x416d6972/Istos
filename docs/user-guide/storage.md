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
| `exactly_once` | Idempotency key claimed before execution + cached result |

### How `exactly_once` holds

The idempotency key is **claimed before the handler runs**, in one atomic
operation against the ledger, and completed with the result afterwards. That
ordering is the guarantee: checking a "already processed?" flag first and setting
it after would leave the whole handler body between the two, so every concurrent
redelivery would pass the check and run the side effects.

There are three outcomes for a call:

- **No claim yet** — this call owns the key and executes.
- **Claim held by another call** — raises `ConflictError` (`conflict`, 409).
  Nothing executes. It is retryable: once the first call finishes, the retry gets
  its cached result.
- **Already finished** — returns the cached result without running the handler.

A claim carries a **lease** (default 300s, `@handle(idempotency_lease_s=...)`) so
a node that dies mid-handler does not strand the key; after it lapses another
delivery takes over. Raise it above your handler's worst-case runtime. A finished
result is terminal — no lease expires it.

If a handler raises, the claim is released and the work stays retryable. Only a
*completed* call is deduplicated.

Custom storage plugins need `claim_processed()` and `release_claim()` (see
`StoragePlugin`). A plugin that predates them still works, but degrades to
at-least-once with a result cache and warns on first use.

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
