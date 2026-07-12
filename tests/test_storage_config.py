"""StorageConfig: URL building, validation, env vars, and lifecycle teardown."""

import asyncio

import pytest

pytest.importorskip("sqlalchemy")

from pydantic import ValidationError  # noqa: E402

from istos import Istos  # noqa: E402
from istos.consistency import (  # noqa: E402
    StorageConfig,
    SqlAlchemyStoragePlugin,
    InMemoryStoragePlugin,
)


def test_builds_full_url_from_fields():
    cfg = StorageConfig(backend="postgresql", driver="asyncpg", host="db",
                        database="istos", username="svc", password="secret")
    url = cfg.build_url().render_as_string(hide_password=False)
    assert url == "postgresql+asyncpg://svc:secret@db/istos"


def test_driver_is_required():
    # No default is applied — you must name the driver you installed.
    with pytest.raises(ValidationError):
        StorageConfig(backend="postgresql", host="db", database="istos")


def test_driver_choice_and_password_escaping():
    cfg = StorageConfig(backend="postgresql", host="db", database="istos",
                        username="svc", password="p@ss:w/rd", driver="psycopg")
    url = cfg.build_url().render_as_string(hide_password=False)
    assert url.startswith("postgresql+psycopg://")
    assert "p%40ss%3Aw%2Frd" in url  # special chars are URL-escaped


def test_missing_driver_package_raises_actionable_error():
    # asyncpg is not installed in the test env — construction must fail fast
    # with a message telling the user to install it.
    cfg = StorageConfig(backend="postgresql", driver="asyncpg", host="db", database="istos")
    with pytest.raises(ModuleNotFoundError, match="pip install asyncpg"):
        SqlAlchemyStoragePlugin.from_config(cfg)


def test_server_backend_requires_host_and_database():
    with pytest.raises(ValidationError):
        StorageConfig(backend="mysql", driver="asyncmy", username="u")  # no host/database


def test_sqlite_requires_database_path():
    with pytest.raises(ValidationError):
        StorageConfig(backend="sqlite", driver="aiosqlite")  # no file path


def test_reads_from_environment(monkeypatch):
    monkeypatch.setenv("ISTOS_DB_BACKEND", "postgresql")
    monkeypatch.setenv("ISTOS_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("ISTOS_DB_HOST", "envhost")
    monkeypatch.setenv("ISTOS_DB_DATABASE", "envdb")
    monkeypatch.setenv("ISTOS_DB_PASSWORD", "envpass")
    cfg = StorageConfig()  # all fields from ISTOS_DB_* env vars
    assert cfg.host == "envhost" and cfg.database == "envdb" and cfg.driver == "asyncpg"
    assert cfg.password.get_secret_value() == "envpass"


def test_istos_builds_and_owns_storage_from_config(tmp_path):
    cfg = StorageConfig(backend="sqlite", driver="aiosqlite", database=str(tmp_path / "l.db"))
    app = Istos(storage_config=cfg, enable_health=False, enable_metrics=False)
    assert isinstance(app._storage, SqlAlchemyStoragePlugin)
    assert app._storage._owns_engine is True   # Istos created it → Istos disposes it


def test_storage_and_storage_config_are_mutually_exclusive():
    with pytest.raises(ValueError, match="at most one"):
        Istos(storage=InMemoryStoragePlugin(),
              storage_config=StorageConfig(backend="sqlite", driver="aiosqlite", database=":memory:"))


@pytest.mark.asyncio
async def test_storage_closed_on_shutdown(mocker):
    """run_async must dispose the storage backend when the service stops."""
    # Bare app (no built-in handlers) so the mocked session binds no queryables.
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    app._storage = mocker.AsyncMock()             # has an awaitable close()
    app._session_manager = mocker.AsyncMock()
    app._session_manager.__aenter__.return_value = mocker.AsyncMock()

    task = asyncio.create_task(app.run_async())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    app._storage.close.assert_awaited()           # teardown ran on shutdown
