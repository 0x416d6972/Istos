"""Durability ledger: the storage protocol every backend implements."""

from typing import Protocol, Any, Optional, List, runtime_checkable
from dataclasses import dataclass
from enum import Enum
import asyncio
import time


#: How long a claim stays valid before another delivery may take it over. Must
#: exceed the slowest exactly-once handler.
DEFAULT_CLAIM_LEASE_S = 300.0


class ClaimState(str, Enum):
    """Outcome of :meth:`StoragePlugin.claim_processed`.

    - CLAIMED:   this caller owns the key and must execute the work.
    - IN_FLIGHT: another caller owns it and has not finished — do **not** execute.
    - DONE:      the work already ran; :attr:`Claim.result` is what it returned.
    """
    CLAIMED = "claimed"
    IN_FLIGHT = "in_flight"
    DONE = "done"


@dataclass(frozen=True)
class Claim:
    """A claim attempt's outcome. ``result`` is meaningful only when DONE."""
    state: ClaimState
    result: Any = None


class Durability(str, Enum):
    """
    Delivery semantics for handler execution.

    - AT_MOST_ONCE:  Fire-and-forget. No logging, no dedup. Fastest.
    - AT_LEAST_ONCE: Logs every call to event_log. Retries may cause duplicates.
    - EXACTLY_ONCE:  Logs + idempotency. The key is claimed *before* the handler
      runs, so a concurrent duplicate cannot execute alongside it; once the work
      finishes, later duplicates return the cached result.
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

    async def claim_processed(
        self, idempotency_key: str, *, lease_s: float = DEFAULT_CLAIM_LEASE_S
    ) -> Claim:
        """Atomically claim ``idempotency_key`` for execution, or report its state.

        Decides in one indivisible step whether this caller is the one that
        executes. :meth:`check_processed` before the handler plus
        :meth:`mark_processed` after it is not equivalent: every concurrent
        duplicate passes the check and executes.

        The claim carries a lease, so a process that dies mid-handler does not
        block the key forever. DONE is terminal — a lease never expires a
        finished result.
        """
        ...

    async def release_claim(self, idempotency_key: str) -> None:
        """Give up a claim that did not finish, so the work can be retried.

        A no-op if the key is already DONE — a completed result is never undone.
        """
        ...

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        """Cached result if the work *finished*, else None.

        A key that is only claimed reads as None. This cannot tell "never ran"
        from "ran and returned None" — use :meth:`claim_processed` when that
        difference matters.
        """
        ...

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        """Complete a claim: mark the key DONE and cache ``result``.

        First result wins — a key that is already DONE is left untouched.
        """
        ...


@dataclass
class _Record:
    """One key's state: claimed with a lease, or done with a result."""
    done: bool
    result: Any = None
    lease_until: float = 0.0


class InMemoryStoragePlugin:
    """
    Thread-safe in-memory storage with full durability support.
    Data is lost on restart — use for testing and development.
    """
    def __init__(self):
        self._store: dict[str, Any] = {}
        self._event_log: dict[str, List[dict]] = {}
        self._processed: dict[str, _Record] = {}
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
            # Skip only a finished key — a claim is still in flight, and its
            # event belongs in the log.
            rec = self._processed.get(idempotency_key) if idempotency_key else None
            if rec is not None and rec.done:
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

    async def claim_processed(
        self, idempotency_key: str, *, lease_s: float = DEFAULT_CLAIM_LEASE_S
    ) -> Claim:
        async with self._lock:
            rec = self._processed.get(idempotency_key)
            if rec is not None:
                if rec.done:
                    return Claim(ClaimState.DONE, rec.result)
                if rec.lease_until > time.monotonic():
                    return Claim(ClaimState.IN_FLIGHT)
                # Lease lapsed — whoever held it died mid-handler. Take over.
            self._processed[idempotency_key] = _Record(
                done=False, lease_until=time.monotonic() + lease_s
            )
            return Claim(ClaimState.CLAIMED)

    async def release_claim(self, idempotency_key: str) -> None:
        async with self._lock:
            rec = self._processed.get(idempotency_key)
            if rec is not None and not rec.done:
                del self._processed[idempotency_key]

    async def check_processed(self, idempotency_key: str) -> Optional[Any]:
        async with self._lock:
            rec = self._processed.get(idempotency_key)
            return rec.result if rec is not None and rec.done else None

    async def mark_processed(self, idempotency_key: str, result: Any) -> None:
        async with self._lock:
            rec = self._processed.get(idempotency_key)
            if rec is not None and rec.done:
                return  # first result wins
            self._processed[idempotency_key] = _Record(done=True, result=result)

