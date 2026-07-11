"""Named database registry: engines, per-request sessions, ledger wiring, DI."""

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import text  # noqa: E402

from istos import Istos, Depends  # noqa: E402
from istos.consistency import DatabaseConfig, DatabaseRegistry, SqlAlchemyStoragePlugin  # noqa: E402
from istos.testing import IstosTestClient  # noqa: E402


def _cfg(tmp_path, name):
    return DatabaseConfig(backend="sqlite", driver="aiosqlite", database=str(tmp_path / f"{name}.db"))


def _app(tmp_path, **kw):
    return Istos(enable_health=False, enable_metrics=False, **kw)


def test_engine_is_shared_and_cached(tmp_path):
    reg = DatabaseRegistry({"a": _cfg(tmp_path, "a")})
    assert reg.engine("a") is reg.engine("a")   # one pool per name, reused


def test_session_provider_is_stable_per_name(tmp_path):
    reg = DatabaseRegistry({"a": _cfg(tmp_path, "a")})
    # Same callable each time → dependency_overrides can match by identity.
    assert reg.session_dependency("a") is reg.session_dependency("a")


def test_unknown_database_name_raises(tmp_path):
    reg = DatabaseRegistry({"a": _cfg(tmp_path, "a")})
    with pytest.raises(KeyError, match="No database named 'missing'"):
        reg.engine("missing")


def test_storage_database_selects_named_ledger(tmp_path):
    app = _app(tmp_path, databases={"ledger": _cfg(tmp_path, "ledger")}, storage_database="ledger")
    assert isinstance(app._storage, SqlAlchemyStoragePlugin)
    # storage borrows the registry's engine (registry owns disposal)
    assert app._storage._owns_engine is False
    assert app._storage._engine is app.databases.engine("ledger")


def test_storage_database_must_exist(tmp_path):
    with pytest.raises(ValueError, match="not in `databases`"):
        _app(tmp_path, databases={"ledger": _cfg(tmp_path, "l")}, storage_database="nope")


def test_ledger_sources_are_mutually_exclusive(tmp_path):
    with pytest.raises(ValueError, match="at most one"):
        _app(
            tmp_path,
            databases={"ledger": _cfg(tmp_path, "l")},
            storage_database="ledger",
            storage_config=_cfg(tmp_path, "other"),
        )


@pytest.mark.asyncio
async def test_app_db_injected_per_request(tmp_path):
    app = _app(tmp_path, databases={"app": _cfg(tmp_path, "app")})

    @app.handle("app/write")
    async def write(val: str, db=Depends(app.db_session("app"))):
        await db.execute(text("CREATE TABLE IF NOT EXISTS t (v TEXT)"))
        await db.execute(text("INSERT INTO t (v) VALUES (:v)"), {"v": val})
        return "ok"

    @app.handle("app/read")
    async def read(db=Depends(app.db_session("app"))):
        return list((await db.execute(text("SELECT v FROM t"))).scalars().all())

    client = IstosTestClient(app)
    assert await client.query("app/write", val="x") == "ok"
    assert await client.query("app/read") == ["x"]   # committed by the per-request session
    await app.databases.dispose_all()


@pytest.mark.asyncio
async def test_db_session_is_overridable(tmp_path):
    app = _app(tmp_path, databases={"app": _cfg(tmp_path, "app")})

    @app.handle("who")
    async def who(db=Depends(app.db_session("app"))):
        return "real"

    class Fake:
        pass

    app.dependency_overrides[app.db_session("app")] = lambda: "fake-session"

    @app.handle("who2")
    async def who2(db=Depends(app.db_session("app"))):
        return db

    client = IstosTestClient(app)
    assert await client.query("who2") == "fake-session"
    await app.databases.dispose_all()


@pytest.mark.asyncio
async def test_dispose_all_is_idempotent(tmp_path):
    reg = DatabaseRegistry({"a": _cfg(tmp_path, "a")})
    reg.engine("a")            # build it
    await reg.dispose_all()
    await reg.dispose_all()    # second call is a no-op, must not raise
