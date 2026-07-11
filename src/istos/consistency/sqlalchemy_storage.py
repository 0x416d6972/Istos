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

if TYPE_CHECKING:
    from istos.consistency.config import DatabaseConfig


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
            self._ready = True

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

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        await self._ensure_ready()
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    select(self._idempotency.c.result).where(
                        self._idempotency.c.idempotency_key == idempotency_key
                    )
                )
            ).first()
        return self._deserialize(row[0]) if row is not None else None

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        await self._ensure_ready()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    insert(self._idempotency).values(
                        idempotency_key=idempotency_key,
                        result=self._serialize(result),
                        created_at=time.time(),
                    )
                )
        except IntegrityError:
            pass  # already processed — first result wins

    async def close(self) -> None:
        """Dispose the engine — only if this plugin created it."""
        if self._owns_engine and self._engine is not None:
            await self._engine.dispose()
