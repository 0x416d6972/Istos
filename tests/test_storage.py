import pytest
import pytest_asyncio
import asyncio
import os
from istos.consistency.storage import SQLiteStoragePlugin

@pytest_asyncio.fixture
async def sqlite_storage(tmp_path):
    db_path = tmp_path / "test_storage.db"
    storage = SQLiteStoragePlugin(db_path=str(db_path))
    # Give it a moment to initialize the task
    await storage._ensure_db()
    
    yield storage

    # Cleanup
    await storage.close()

@pytest.mark.asyncio
async def test_sqlite_storage_put_and_get_string(sqlite_storage):
    """Test storing and retrieving a string value."""
    await sqlite_storage.put("sensor/temp1", "25.4")
    
    result = await sqlite_storage.get("sensor/temp1")
    # Result should be bytes encoding the string
    assert result == b"25.4"

@pytest.mark.asyncio
async def test_sqlite_storage_put_and_get_bytes(sqlite_storage):
    """Test storing and retrieving raw bytes."""
    fake_payload = b"\x01\x02\x03\x04"
    await sqlite_storage.put("binary/data", fake_payload)
    
    result = await sqlite_storage.get("binary/data")
    assert result == fake_payload

@pytest.mark.asyncio
async def test_sqlite_storage_get_missing_key(sqlite_storage):
    """Test retrieving a key that does not exist."""
    result = await sqlite_storage.get("does/not/exist")
    assert result is None

@pytest.mark.asyncio
async def test_sqlite_storage_overwrite_value(sqlite_storage):
    """Test overwriting an existing key."""
    await sqlite_storage.put("robot/state", "idle")
    await sqlite_storage.put("robot/state", "moving")
    
    result = await sqlite_storage.get("robot/state")
    assert result == b"moving"

@pytest.mark.asyncio
async def test_sqlite_storage_delete(sqlite_storage):
    """Test deleting a key."""
    await sqlite_storage.put("temporary/key", "temp_value")
    
    # Exists before deletion
    result = await sqlite_storage.get("temporary/key")
    assert result == b"temp_value"
    
    # Delete it
    await sqlite_storage.delete("temporary/key")
    
    # Does not exist after deletion
    result_after = await sqlite_storage.get("temporary/key")
    assert result_after is None

@pytest.mark.asyncio
async def test_sqlite_storage_durability(tmp_path):
    """Test that data survives when the plugin is closed and reopened."""
    db_path = tmp_path / "test_durability.db"
    
    # Instance 1: Write data
    storage1 = SQLiteStoragePlugin(db_path=str(db_path))
    await storage1.put("system/config", "version=2.0")
    await storage1.close()
    
    # Instance 2: Read data (simulating a crash/restart)
    storage2 = SQLiteStoragePlugin(db_path=str(db_path))
    result = await storage2.get("system/config")
    await storage2.close()
    
    assert result == b"version=2.0"
