"""Durable conversation log for resumable @channel sessions.

Keyed by conversation id over the app's StoragePlugin (InMemory / Redis / SQL),
so a session survives a disconnect: the handler reloads history on reconnect and
continues where it left off.
"""

import time
from typing import Any, List


class SessionStore:
    def __init__(self, storage: Any) -> None:
        self._storage = storage

    @staticmethod
    def _key(conversation_id: str) -> str:
        return f"conv:{conversation_id}"

    async def append(self, conversation_id: str, direction: str, data: Any) -> None:
        await self._storage.log(
            self._key(conversation_id),
            {"dir": direction, "data": data, "ts": time.time()},
        )

    async def history(self, conversation_id: str, limit: int = 1000) -> List[dict]:
        """Prior messages oldest-first: ``[{dir, data, ts}, ...]``."""
        entries = await self._storage.get_log(self._key(conversation_id), limit=limit)
        return [e["value"] for e in reversed(entries)]
