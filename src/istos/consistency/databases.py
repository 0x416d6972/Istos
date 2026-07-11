"""Named async SQLAlchemy databases: app-lifetime engines, per-request sessions.

A ``DatabaseRegistry`` holds several named databases for one service. Each named
config becomes exactly one ``AsyncEngine`` (a connection pool) created lazily and
disposed on shutdown — the *app-level* tier. A ``Depends`` provider then leases one
``AsyncSession`` per request from that pool — the *per-request* tier.

    databases = DatabaseRegistry({
        "ledger": DatabaseConfig(backend="postgresql", driver="asyncpg", host="pg", database="ledger"),
        "app":    DatabaseConfig(backend="postgresql", driver="asyncpg", host="pg", database="app"),
    })

    # one entry backs the durability ledger:
    storage = SqlAlchemyStoragePlugin(databases.engine("ledger"))

    # another is injected into handlers per request:
    @app.handle("orders/create")
    async def create(item: str, db = Depends(databases.session_dependency("app"))):
        ...
"""

from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator, Callable, Dict, Mapping

from istos.consistency.sqlalchemy_storage import create_async_engine_checked

try:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
except ImportError:  # pragma: no cover - exercised only when the extra is absent
    AsyncEngine = object  # type: ignore
    AsyncSession = object  # type: ignore
    async_sessionmaker = None  # type: ignore

if TYPE_CHECKING:
    from istos.consistency.config import DatabaseConfig


class DatabaseRegistry:
    """Named databases with app-lifetime engines and per-request sessions."""

    def __init__(self, configs: Mapping[str, "DatabaseConfig"]):
        self._configs: Dict[str, "DatabaseConfig"] = dict(configs)
        self._engines: Dict[str, "AsyncEngine"] = {}
        self._sessionmakers: Dict[str, "async_sessionmaker"] = {}
        # Providers are cached per name so the same callable is returned every
        # time — required for `dependency_overrides` to match by identity.
        self._providers: Dict[str, Callable[[], AsyncIterator["AsyncSession"]]] = {}

    def __contains__(self, name: str) -> bool:
        return name in self._configs

    def names(self) -> list:
        return list(self._configs)

    def _require(self, name: str) -> "DatabaseConfig":
        if name not in self._configs:
            raise KeyError(
                f"No database named {name!r}. Configured databases: {sorted(self._configs)}"
            )
        return self._configs[name]

    def _ensure_built(self, name: str) -> None:
        """Create the engine + sessionmaker for `name` once (lazy, no I/O yet)."""
        if name in self._engines:
            return
        config = self._require(name)
        engine = create_async_engine_checked(config.build_url(), **config.engine_kwargs())
        self._engines[name] = engine
        self._sessionmakers[name] = async_sessionmaker(engine, expire_on_commit=False)

    def engine(self, name: str) -> "AsyncEngine":
        """The shared, app-lifetime engine (connection pool) for `name`."""
        self._ensure_built(name)
        return self._engines[name]

    def session_dependency(self, name: str) -> Callable[[], AsyncIterator["AsyncSession"]]:
        """
        A stable, cached ``Depends`` provider that yields one ``AsyncSession`` per
        request from the named database's pool, committing on success and rolling
        back on error, then returning the connection to the pool.
        """
        self._require(name)  # fail early on an unknown name
        if name not in self._providers:
            async def _provide() -> AsyncIterator["AsyncSession"]:
                self._ensure_built(name)
                async with self._sessionmakers[name]() as session:
                    async with session.begin():
                        yield session
            _provide.__name__ = f"db_session[{name}]"
            _provide.__qualname__ = _provide.__name__
            self._providers[name] = _provide
        return self._providers[name]

    async def dispose_all(self) -> None:
        """Dispose every engine — called once on service shutdown."""
        for engine in self._engines.values():
            await engine.dispose()
        self._engines.clear()
        self._sessionmakers.clear()
