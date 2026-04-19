from typing import Protocol, Any, Optional, runtime_checkable, runtime_checkable
from abc import ABC, abstractmethod
import asyncio

try:
    import aiosqlite
except ImportError:
    aiosqlite = None # type: ignore


@runtime_checkable
class StoragePlugin(Protocol):
    """
    Interface for attaching 'Storages' to save messages, states or historical data.
    Different type of Storages like Databases could be used.
    """
    async def put(self, key: str, value: Any) -> None:
        """
        Intercept put messages and writes them to disk.
        """
        ...

    async def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value by key.
        """
        ...

    async def delete(self, key: str) -> None:
        """
        Delete a key from storage.
        """
        ...



class InMemoryStoragePlugin:
    """
    A simple thread-safe in-memory key-value store.
    Useful for storing temporary node metadata, states, or registry information.
    """
    def __init__(self):
        self._store: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def put(self, key: str, value: Any) -> None:
        async with self._lock:
            self._store[key] = value

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            return self._store.get(key)

    async def delete(self, key: str) -> None:
        async with self._lock:
            if key in self._store:
                del self._store[key]


class SQLiteStoragePlugin:
    """
    A persistent key-value store using SQLite and `aiosqlite`.
    Useful for storing state and events that must survive restarts.
    Values are stored as serialized strings or blobs.
    """
    def __init__(self, db_path: str = "istos_storage.db"):
        if aiosqlite is None:
            raise ImportError(
                "aiosqlite is not installed. "
                "Please install istos with the sqlite extra: pip install 'istos[sqlite]'"
            )
        self.db_path = db_path
        self._db: Optional[Any] = None # using Any so we don't break type hinting if aiosqlite missing
        self._init_task = asyncio.create_task(self._init_db())

    async def _init_db(self) -> None:
        """Initialize the database table if it doesn't exist."""
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute(
            '''
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value BLOB
            )
            '''
        )
        await self._db.commit()

    async def _ensure_db(self):
        """Wait for the DB to be ready before querying."""
        if self._db is None:
            await self._init_task

    async def put(self, key: str, value: Any) -> None:
        await self._ensure_db()
        
        # Ensure value is bytes for SQLite BLOB storage
        if isinstance(value, str):
            value = value.encode('utf-8')
        elif not isinstance(value, bytes):
            value = str(value).encode('utf-8')

        await self._db.execute( # type: ignore
            'INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)',
            (key, value)
        )
        await self._db.commit() # type: ignore

    async def get(self, key: str) -> Optional[Any]:
        await self._ensure_db()
        async with self._db.execute( # type: ignore
            'SELECT value FROM kv_store WHERE key = ?', 
            (key,)
        ) as cursor:
            row = await cursor.fetchone()
            if row is not None:
                return row[0] # Returns BLOB as bytes
            return None

    async def delete(self, key: str) -> None:
        await self._ensure_db()
        await self._db.execute( # type: ignore
            'DELETE FROM kv_store WHERE key = ?',
            (key,)
        )
        await self._db.commit() # type: ignore

    async def close(self) -> None:
        """Close the database connection cleanly."""
        if self._db:
            await self._db.close()
            self._db = None
