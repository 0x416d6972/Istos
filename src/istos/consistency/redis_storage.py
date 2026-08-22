"""Redis-backed storage plugin for distributed deployments."""

from __future__ import annotations

import json
import time
from typing import Any, List, Optional, Tuple

from istos.consistency.storage import DEFAULT_CLAIM_LEASE_S, Claim, ClaimState

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None  # type: ignore


# Records are tagged so one GET tells claimed from finished: b"P" while a
# handler owns the key, b"D" + JSON result once it returns. Untagged values were
# written by an older Istos — bare results, i.e. already done.
_PENDING_TAG = b"P"
_DONE_TAG = b"D"

# Return what is already there, or take the key with a lease. One round trip, so
# the claim is atomic; SET NX alone cannot report *why* it failed.
_CLAIM_LUA = """
local cur = redis.call('GET', KEYS[1])
if cur then return cur end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return false
"""

# Complete a claim. Dropping the TTL matters: pending records expire so a dead
# owner cannot hold the key, done records never do.
_MARK_LUA = """
local cur = redis.call('GET', KEYS[1])
if cur and string.sub(cur, 1, 1) == 'D' then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
return 1
"""

# Release only our own unfinished claim — never a result.
_RELEASE_LUA = """
local cur = redis.call('GET', KEYS[1])
if cur and string.sub(cur, 1, 1) == 'P' then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
"""


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
        self._scripts: dict[str, Any] = {}

    async def _get_client(self) -> Any:
        if self._client is None:
            self._client = aioredis.from_url(self._url, decode_responses=False)
            # register_script caches by SHA — the body travels once, not per call.
            self._scripts = {
                "claim": self._client.register_script(_CLAIM_LUA),
                "mark": self._client.register_script(_MARK_LUA),
                "release": self._client.register_script(_RELEASE_LUA),
            }
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
            # Test the record, not the result, so a handler that returned None
            # still suppresses its duplicate.
            done, _ = self._decode(await self._read_record(idempotency_key))
            if done:
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

    # ---- Idempotency ----

    @staticmethod
    def _loads(raw: Any) -> Any:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            return raw

    @classmethod
    def _decode(cls, raw: Any) -> Tuple[bool, Any]:
        """Split a stored record into (finished?, result)."""
        if raw is None:
            return False, None
        if isinstance(raw, str):
            raw = raw.encode()
        tag, body = raw[:1], raw[1:]
        if tag == _DONE_TAG:
            return True, cls._loads(body)
        if tag == _PENDING_TAG:
            return False, None
        return True, cls._loads(raw)  # untagged: an older Istos wrote a bare result

    async def _read_record(self, idempotency_key: str) -> Any:
        client = await self._get_client()
        return await client.get(self._idemp_key(idempotency_key))

    async def claim_processed(
        self, idempotency_key: str, *, lease_s: float = DEFAULT_CLAIM_LEASE_S
    ) -> Claim:
        await self._get_client()
        existing = await self._scripts["claim"](
            keys=[self._idemp_key(idempotency_key)],
            args=[_PENDING_TAG, int(lease_s * 1000)],
        )
        if not existing:
            return Claim(ClaimState.CLAIMED)
        done, result = self._decode(existing)
        return Claim(ClaimState.DONE, result) if done else Claim(ClaimState.IN_FLIGHT)

    async def release_claim(self, idempotency_key: str) -> None:
        await self._get_client()
        await self._scripts["release"](keys=[self._idemp_key(idempotency_key)])

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        done, result = self._decode(await self._read_record(idempotency_key))
        return result if done else None

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        await self._get_client()
        await self._scripts["mark"](
            keys=[self._idemp_key(idempotency_key)],
            args=[_DONE_TAG + json.dumps(result).encode()],
        )

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
