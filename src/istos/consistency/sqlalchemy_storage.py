"""SQLAlchemy-backed storage plugin — one durability ledger for any SQLAlchemy database."""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

try:
    from sqlalchemy import (
        URL,
        Column,
        Float,
        Integer,
        LargeBinary,
        MetaData,
        String,
        Table,
        delete,
        insert,
        make_url,
        select,
        update,
    )
    from sqlalchemy.exc import IntegrityError, NoSuchModuleError
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except ImportError:  # pragma: no cover - exercised only when the extra is absent
    URL = None  # type: ignore
    MetaData = None  # type: ignore
    AsyncEngine = Any  # type: ignore
    NoSuchModuleError = Exception  # type: ignore

from istos.consistency.storage import DEFAULT_CLAIM_LEASE_S, Claim, ClaimState

if TYPE_CHECKING:
    from istos.consistency.config import DatabaseConfig

# ``status`` on an idempotency row. NULL is a row from an older Istos, which
# only ever stored finished results, so NULL reads as done.
_PENDING = "pending"
_DONE = "done"


def _driver_name(url: "Union[str, URL]") -> Optional[str]:
    """Best-effort driver name from a URL (str or URL), for error messages."""
    try:
        return make_url(url).get_driver_name()
    except Exception:
        return None


def create_async_engine_checked(url: "Union[str, URL]", **engine_kwargs: Any) -> "AsyncEngine":
    """
    ``create_async_engine`` that turns a missing driver into an actionable error.

    The DBAPI driver is imported here, so an uninstalled driver fails now (at
    construction) rather than on the first query — with a message naming the
    package to install instead of SQLAlchemy's bare ImportError.
    """
    if MetaData is None:
        raise ImportError(
            "SQLAlchemy is not installed. Install with: pip install 'istos[sqlalchemy]'"
        )
    try:
        return create_async_engine(url, **engine_kwargs)
    except (ModuleNotFoundError, NoSuchModuleError) as exc:
        driver = _driver_name(url)
        hint = f" Install it, e.g.: pip install {driver}" if driver else ""
        raise ModuleNotFoundError(
            f"The async database driver for this connection is not installed.{hint} "
            f"(SQLAlchemy: {exc})"
        ) from exc


def _build_schema():
    """The fixed durability schema, mirroring the other backends."""
    metadata = MetaData()
    kv = Table(
        "istos_kv_store",
        metadata,
        Column("key", String, primary_key=True),
        Column("value", LargeBinary),
    )
    event_log = Table(
        "istos_event_log",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("key", String, nullable=False, index=True),
        Column("value", LargeBinary),
        Column("idempotency_key", String, unique=True, nullable=True),
        Column("timestamp", Float, nullable=False),
    )
    idempotency = Table(
        "istos_idempotency",
        metadata,
        Column("idempotency_key", String, primary_key=True),
        Column("result", LargeBinary),
        Column("created_at", Float, nullable=False),
        # Nullable so they can be added to a table an older Istos created —
        # ALTER TABLE ADD COLUMN without a default is portable everywhere.
        Column("status", String, nullable=True),
        Column("lease_expires_at", Float, nullable=True),
    )
    return metadata, kv, event_log, idempotency


class SqlAlchemyStoragePlugin:
    """
    Durability ledger backed by SQLAlchemy's async engine, so a single backend
    works on any SQLAlchemy-supported database (PostgreSQL, MySQL/MariaDB,
    SQLite, MSSQL, ...). Pass an async URL or a pre-built ``AsyncEngine``:

        SqlAlchemyStoragePlugin("postgresql+asyncpg://user:pass@host/db")
        SqlAlchemyStoragePlugin("mysql+asyncmy://user:pass@host/db")
        SqlAlchemyStoragePlugin("sqlite+aiosqlite:///istos.db")
        SqlAlchemyStoragePlugin(existing_async_engine)              # reuse your engine
        SqlAlchemyStoragePlugin.from_config(StorageConfig(...))     # structured creds

    Tables are created lazily on first use, so the plugin is safe to construct
    anywhere (no running event loop required at __init__).

    Install with: pip install 'istos[sqlalchemy]'

    Note: this is the framework's *durability ledger*. To use your application's
    database inside a handler or publisher, inject a session with Depends(...).
    """

    def __init__(
        self,
        url: "Union[str, URL, AsyncEngine]",
        *,
        engine_kwargs: Optional[Dict[str, Any]] = None,
    ):
        if MetaData is None:
            raise ImportError(
                "SQLAlchemy is not installed. Install with: pip install 'istos[sqlalchemy]'"
            )
        self._metadata, self._kv, self._event_log, self._idempotency = _build_schema()
        if isinstance(url, AsyncEngine):
            # A pre-built engine — we borrow it and never dispose it.
            self._engine: AsyncEngine = url
            self._owns_engine = False
        else:
            # A URL (str or sqlalchemy.URL) — we create and therefore own the engine.
            self._engine = create_async_engine_checked(url, **(engine_kwargs or {}))
            self._owns_engine = True
        self._ready = False
        self._init_lock = asyncio.Lock()

    @classmethod
    def from_config(cls, config: "DatabaseConfig") -> "SqlAlchemyStoragePlugin":
        """Build a plugin (and its owned engine) from structured connection settings."""
        return cls(config.build_url(), engine_kwargs=config.engine_kwargs())

    async def _ensure_ready(self) -> None:
        """Create tables on first use — idempotent and concurrency-safe."""
        if self._ready:
            return
        async with self._init_lock:
            if self._ready:
                return
            async with self._engine.begin() as conn:
                await conn.run_sync(self._metadata.create_all)
                await conn.run_sync(self._add_missing_columns)
            self._ready = True

    def _add_missing_columns(self, sync_conn: Any) -> None:
        """Bring an idempotency table created by an older Istos up to date.

        ``create_all`` skips tables that already exist, so a ledger written
        before claims would be missing ``status`` / ``lease_expires_at`` and
        every claim would fail on an unknown column.
        """
        from sqlalchemy import inspect as sa_inspect

        insp = sa_inspect(sync_conn)
        present = {c["name"] for c in insp.get_columns(self._idempotency.name)}
        for column in (self._idempotency.c.status, self._idempotency.c.lease_expires_at):
            if column.name in present:
                continue
            col_type = column.type.compile(dialect=sync_conn.dialect)
            sync_conn.exec_driver_sql(
                f"ALTER TABLE {self._idempotency.name} ADD COLUMN {column.name} {col_type}"
            )

    @staticmethod
    def _serialize(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode()
        return json.dumps(value).encode()

    @staticmethod
    def _deserialize(raw: Any) -> Any:
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            return raw

    # ---- Core key-value ----

    async def put(self, key: str, value: Any) -> None:
        await self._ensure_ready()
        payload = self._serialize(value)
        # UPDATE first (dialect-agnostic upsert); INSERT only if the row is new.
        async with self._engine.begin() as conn:
            result = await conn.execute(
                update(self._kv).where(self._kv.c.key == key).values(value=payload)
            )
            if result.rowcount:
                return
        try:
            async with self._engine.begin() as conn:
                await conn.execute(insert(self._kv).values(key=key, value=payload))
        except IntegrityError:
            # A concurrent writer inserted the row first — update it instead.
            async with self._engine.begin() as conn:
                await conn.execute(
                    update(self._kv).where(self._kv.c.key == key).values(value=payload)
                )

    async def get(self, key: str) -> Optional[Any]:
        await self._ensure_ready()
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    select(self._kv.c.value).where(self._kv.c.key == key)
                )
            ).first()
        return self._deserialize(row[0]) if row is not None else None

    async def delete(self, key: str) -> None:
        await self._ensure_ready()
        async with self._engine.begin() as conn:
            await conn.execute(delete(self._kv).where(self._kv.c.key == key))

    # ---- Event log ----

    async def log(self, key: str, value: Any, idempotency_key: Optional[str] = None) -> None:
        await self._ensure_ready()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    insert(self._event_log).values(
                        key=key,
                        value=self._serialize(value),
                        idempotency_key=idempotency_key,
                        timestamp=time.time(),
                    )
                )
        except IntegrityError:
            pass  # duplicate idempotency_key — already logged, skip

    async def get_log(self, key: str, limit: int = 100) -> List[Any]:
        await self._ensure_ready()
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(
                        self._event_log.c.value,
                        self._event_log.c.timestamp,
                        self._event_log.c.idempotency_key,
                    )
                    .where(self._event_log.c.key == key)
                    .order_by(self._event_log.c.id.desc())
                    .limit(limit)
                )
            ).all()
        return [
            {"value": self._deserialize(r[0]), "timestamp": r[1], "idempotency_key": r[2]}
            for r in rows
        ]

    # ---- Idempotency ----

    async def claim_processed(
        self, idempotency_key: str, *, lease_s: float = DEFAULT_CLAIM_LEASE_S
    ) -> Claim:
        await self._ensure_ready()
        idemp = self._idempotency
        # Two attempts: the row can be released between the failed insert and
        # the lookup, and then the insert would have succeeded.
        for _ in range(2):
            now = time.time()
            # The primary key makes the insert the claim: one concurrent caller
            # lands it, the rest get IntegrityError.
            try:
                async with self._engine.begin() as conn:
                    await conn.execute(
                        insert(idemp).values(
                            idempotency_key=idempotency_key,
                            result=None,
                            created_at=now,
                            status=_PENDING,
                            lease_expires_at=now + lease_s,
                        )
                    )
                return Claim(ClaimState.CLAIMED)
            except IntegrityError:
                pass  # already owned — find out in what state

            async with self._engine.connect() as conn:
                row = (
                    await conn.execute(
                        select(idemp.c.status, idemp.c.result, idemp.c.lease_expires_at).where(
                            idemp.c.idempotency_key == idempotency_key
                        )
                    )
                ).first()
            if row is None:
                continue  # released in the meantime — try to claim it ourselves
            status, result, lease_expires_at = row
            if status != _PENDING:  # done, or NULL from an older Istos
                return Claim(ClaimState.DONE, self._deserialize(result))
            if lease_expires_at is not None and lease_expires_at > now:
                return Claim(ClaimState.IN_FLIGHT)
            # Lease lapsed. One UPDATE both tests and takes it, so of several
            # nodes racing to take over exactly one gets a non-zero rowcount.
            async with self._engine.begin() as conn:
                taken = await conn.execute(
                    update(idemp)
                    .where(
                        idemp.c.idempotency_key == idempotency_key,
                        idemp.c.status == _PENDING,
                        idemp.c.lease_expires_at <= now,
                    )
                    .values(lease_expires_at=now + lease_s, created_at=now)
                )
            return Claim(ClaimState.CLAIMED if taken.rowcount else ClaimState.IN_FLIGHT)
        return Claim(ClaimState.IN_FLIGHT)

    async def release_claim(self, idempotency_key: str) -> None:
        await self._ensure_ready()
        idemp = self._idempotency
        async with self._engine.begin() as conn:
            await conn.execute(
                delete(idemp).where(
                    idemp.c.idempotency_key == idempotency_key,
                    idemp.c.status == _PENDING,
                )
            )

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        await self._ensure_ready()
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    select(self._idempotency.c.result, self._idempotency.c.status).where(
                        self._idempotency.c.idempotency_key == idempotency_key
                    )
                )
            ).first()
        if row is None or row[1] == _PENDING:
            return None
        return self._deserialize(row[0])

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        await self._ensure_ready()
        idemp = self._idempotency
        now = time.time()
        payload = self._serialize(result)
        # Filtering on _PENDING is what makes the first result win — once a row
        # is done, no later writer moves it.
        async with self._engine.begin() as conn:
            completed = await conn.execute(
                update(idemp)
                .where(idemp.c.idempotency_key == idempotency_key, idemp.c.status == _PENDING)
                .values(result=payload, status=_DONE, lease_expires_at=None, created_at=now)
            )
        if completed.rowcount:
            return
        # No pending row: already done, or marked without a claim.
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    insert(idemp).values(
                        idempotency_key=idempotency_key,
                        result=payload,
                        created_at=now,
                        status=_DONE,
                    )
                )
        except IntegrityError:
            pass  # already processed — first result wins

    async def close(self) -> None:
        """Dispose the engine — only if this plugin created it."""
        if self._owns_engine and self._engine is not None:
            await self._engine.dispose()
