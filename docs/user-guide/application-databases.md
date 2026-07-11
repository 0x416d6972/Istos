# Application Databases

Named SQLAlchemy engines for **your application data**, separate from the [durability storage ledger](storage.md).

Configure them on `Istos(databases=...)` and inject sessions with `Depends(app.db_session("name"))`.

## Setup

```bash
pip install 'istos[sqlalchemy]' asyncpg   # or aiosqlite, asyncmy, …
```

```python
from typing import Annotated
from istos import Istos, Depends
from istos.consistency import DatabaseConfig

istos = Istos(
    databases={
        "app": DatabaseConfig(
            backend="postgresql",
            driver="asyncpg",
            host="db",
            database="myapp",
            username="svc",
            password="s3cret",
        ),
    }
)

@istos.handle("items/create")
async def create(
    name: str,
    db: Annotated[object, Depends(istos.db_session("app"))],
):
    # `db` is an AsyncSession for this request; closed after the handler returns
    db.add(...)
    await db.commit()
    return {"name": name}
```

## Environment variables

`DatabaseConfig` loads from `ISTOS_DB_*` when constructed without fields:

```env
ISTOS_DB_BACKEND=postgresql
ISTOS_DB_DRIVER=asyncpg
ISTOS_DB_HOST=db
ISTOS_DB_DATABASE=myapp
ISTOS_DB_USERNAME=svc
ISTOS_DB_PASSWORD=s3cret
```

For multiple named DBs, pass explicit `DatabaseConfig(...)` objects in the `databases=` mapping (each can still be built from secrets at startup).

## Durability ledger vs app DB

| Concern | API |
|---------|-----|
| Idempotency / event log for handlers | `storage=` / `storage_config=` / `storage_database=` |
| Business SQL access in handlers | `databases=` + `db_session(name)` |

Reuse one named DB as the ledger:

```python
istos = Istos(
    databases={"ledger": DatabaseConfig(...), "app": DatabaseConfig(...)},
    storage_database="ledger",
)
```

## Testing overrides

```python
istos.dependency_overrides[istos.db_session("app")] = fake_session_dep
```

## Lifecycle

Engines are created for the app lifetime and disposed on shutdown (`run_async` cleanup). Prefer lifespan for one-time migrations; use `db_session` for per-request work.

## Next Steps

- [Dependency Injection](dependency-injection.md)
- [Storage](storage.md)
- [API: Database Registry](../api/consistency/databases.md)
