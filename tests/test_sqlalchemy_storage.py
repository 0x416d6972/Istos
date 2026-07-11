"""Durability-protocol conformance tests for SqlAlchemyStoragePlugin (over SQLite)."""

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402

from istos.consistency import SqlAlchemyStoragePlugin  # noqa: E402


@pytest.fixture
def db_url(tmp_path):
    return f"sqlite+aiosqlite:///{tmp_path / 'ledger.db'}"


def test_construct_without_event_loop(db_url):
    """Constructing must not require a running loop (lazy table creation)."""
    plugin = SqlAlchemyStoragePlugin(db_url)  # would raise if it touched the DB now
    assert plugin._ready is False


@pytest.mark.asyncio
async def test_kv_roundtrip(db_url):
    plugin = SqlAlchemyStoragePlugin(db_url)
    try:
        assert await plugin.get("missing") is None
        await plugin.put("k", {"a": 1})
        assert await plugin.get("k") == {"a": 1}
        await plugin.put("k", {"a": 2})       # upsert overwrites
        assert await plugin.get("k") == {"a": 2}
        await plugin.delete("k")
        assert await plugin.get("k") is None
    finally:
        await plugin.close()


@pytest.mark.asyncio
async def test_event_log_and_idempotent_dedup(db_url):
    plugin = SqlAlchemyStoragePlugin(db_url)
    try:
        await plugin.log("evt", {"n": 1})
        await plugin.log("evt", {"n": 2}, idempotency_key="idem-1")
        await plugin.log("evt", {"n": 3}, idempotency_key="idem-1")  # duplicate → skipped

        entries = await plugin.get_log("evt")
        assert len(entries) == 2                       # third was deduped
        assert [e["value"] for e in entries] == [{"n": 2}, {"n": 1}]  # newest first
    finally:
        await plugin.close()


@pytest.mark.asyncio
async def test_idempotency_cache(db_url):
    plugin = SqlAlchemyStoragePlugin(db_url)
    try:
        assert await plugin.check_processed("job-1") is None
        await plugin.mark_processed("job-1", {"result": 42})
        assert await plugin.check_processed("job-1") == {"result": 42}
        await plugin.mark_processed("job-1", {"result": 99})  # first result wins
        assert await plugin.check_processed("job-1") == {"result": 42}
    finally:
        await plugin.close()


@pytest.mark.asyncio
async def test_borrowed_engine_not_disposed(db_url):
    """When handed an existing engine, close() must not dispose it."""
    engine = create_async_engine(db_url)
    plugin = SqlAlchemyStoragePlugin(engine)
    assert plugin._owns_engine is False
    await plugin.put("k", {"v": 1})
    await plugin.close()                    # should be a no-op for the borrowed engine
    # engine is still usable after the plugin closed
    plugin2 = SqlAlchemyStoragePlugin(engine)
    assert await plugin2.get("k") == {"v": 1}
    await engine.dispose()


@pytest.mark.asyncio
async def test_persistence_across_reopen(db_url):
    """Data written by one plugin/engine survives a fresh engine on the same file."""
    plugin1 = SqlAlchemyStoragePlugin(db_url)
    await plugin1.put("system/config", {"version": "2.0"})
    await plugin1.close()                       # disposes its own engine

    plugin2 = SqlAlchemyStoragePlugin(db_url)    # brand new engine, same DB file
    assert await plugin2.get("system/config") == {"version": "2.0"}
    await plugin2.close()


@pytest.mark.asyncio
async def test_conforms_to_storage_protocol(db_url):
    from istos.consistency import StoragePlugin
    plugin = SqlAlchemyStoragePlugin(db_url)
    assert isinstance(plugin, StoragePlugin)   # runtime_checkable Protocol
    await plugin.close()
