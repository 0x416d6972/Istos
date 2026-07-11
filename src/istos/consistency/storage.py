from typing import Protocol, Any, Optional, List, runtime_checkable
from enum import Enum
import asyncio
import time


class Durability(str, Enum):
    """
    Delivery semantics for handler execution.

    - AT_MOST_ONCE:  Fire-and-forget. No logging, no dedup. Fastest.
    - AT_LEAST_ONCE: Logs every call to event_log. Retries may cause duplicates.
    - EXACTLY_ONCE:  Logs + idempotency. Duplicate calls return cached result.
    """
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@runtime_checkable
class StoragePlugin(Protocol):
    """
    Unified interface for storage backends.
    Every storage must support all operations — the handler's `durability`
    parameter decides which ones are actually called.
    """

    # ---- Core key-value (always used) ----

    async def put(self, key: str, value: Any) -> None:
        """Write or overwrite a value by key."""
        ...

    async def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by key."""
        ...

    async def delete(self, key: str) -> None:
        """Delete a key from storage."""
        ...

    # ---- Event log (used by AT_LEAST_ONCE and EXACTLY_ONCE) ----

    async def log(self, key: str, value: Any, idempotency_key: Optional[str] = None) -> None:
        """Append an event to the durable log. Skips duplicates by idempotency_key."""
        ...

    async def get_log(self, key: str, limit: int = 100) -> List[Any]:
        """Retrieve event log entries for a key, newest first."""
        ...

    # ---- Idempotency (used by EXACTLY_ONCE) ----

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        """Check if already processed. Returns cached result or None."""
        ...

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        """Mark as processed and cache the result."""
        ...


class InMemoryStoragePlugin:
    """
    Thread-safe in-memory storage with full durability support.
    Data is lost on restart — use for testing and development.
    """
    def __init__(self):
        self._store: dict[str, Any] = {}
        self._event_log: dict[str, List[dict]] = {}
        self._processed: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    # ---- Core key-value ----

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

    # ---- Event log ----

    async def log(self, key: str, value: Any, idempotency_key: Optional[str] = None) -> None:
        async with self._lock:
            if idempotency_key and idempotency_key in self._processed:
                return
            if key not in self._event_log:
                self._event_log[key] = []
            self._event_log[key].append({
                "value": value,
                "timestamp": time.time(),
                "idempotency_key": idempotency_key,
            })

    async def get_log(self, key: str, limit: int = 100) -> List[Any]:
        async with self._lock:
            entries = self._event_log.get(key, [])
            return list(reversed(entries[-limit:]))

    # ---- Idempotency ----

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        async with self._lock:
            return self._processed.get(idempotency_key)

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        async with self._lock:
            self._processed[idempotency_key] = result

