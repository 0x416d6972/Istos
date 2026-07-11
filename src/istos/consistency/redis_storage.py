"""Redis-backed storage plugin for distributed deployments."""

from __future__ import annotations

import json
import time
from typing import Any, List, Optional

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None  # type: ignore


class RedisStoragePlugin:
    """
    Distributed storage using Redis.

    Install with: pip install 'istos[redis]'
    """

    def __init__(self, url: str = "redis://localhost:6379/0", prefix: str = "istos:"):
        if aioredis is None:
            raise ImportError(
                "redis is not installed. Install with: pip install 'istos[redis]'"
            )
        self._url = url
        self._prefix = prefix
        self._client: Any = None

    async def _get_client(self) -> Any:
        if self._client is None:
            self._client = aioredis.from_url(self._url, decode_responses=False)
        return self._client

    def _key(self, key: str) -> str:
        return f"{self._prefix}kv:{key}"

    def _log_key(self, key: str) -> str:
        return f"{self._prefix}log:{key}"

    def _idemp_key(self, key: str) -> str:
        return f"{self._prefix}idemp:{key}"

    async def put(self, key: str, value: Any) -> None:
        client = await self._get_client()
        payload = value if isinstance(value, bytes) else json.dumps(value).encode()
        await client.set(self._key(key), payload)

    async def get(self, key: str) -> Optional[Any]:
        client = await self._get_client()
        raw = await client.get(self._key(key))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    async def delete(self, key: str) -> None:
        client = await self._get_client()
        await client.delete(self._key(key))

    async def log(self, key: str, value: Any, idempotency_key: Optional[str] = None) -> None:
        if idempotency_key:
            existing = await self.check_processed(idempotency_key)
            if existing is not None:
                return
        client = await self._get_client()
        entry = json.dumps({
            "value": value.decode() if isinstance(value, bytes) else value,
            "timestamp": time.time(),
            "idempotency_key": idempotency_key,
        })
        await client.lpush(self._log_key(key), entry)

    async def get_log(self, key: str, limit: int = 100) -> List[Any]:
        client = await self._get_client()
        entries = await client.lrange(self._log_key(key), 0, limit - 1)
        return [json.loads(e) for e in entries]

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        client = await self._get_client()
        raw = await client.get(self._idemp_key(idempotency_key))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        client = await self._get_client()
        payload = json.dumps(result).encode()
        await client.set(self._idemp_key(idempotency_key), payload, nx=True)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
