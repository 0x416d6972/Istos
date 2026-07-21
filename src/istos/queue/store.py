"""Work-queue state: the job record and the heap-backed, write-through store."""

from __future__ import annotations

import asyncio
import base64
import heapq
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from istos.logging import get_logger

_logger = get_logger("queue")


class JobState(str, Enum):
    READY = "ready"      # waiting to be claimed
    LEASED = "leased"    # handed to a worker, ack pending
    DEAD = "dead"        # exhausted its attempts, parked for inspection
    DONE = "done"        # completed; result retained for the result backend


@dataclass
class JobRecord:
    id: str
    data: bytes                       # the job body, exactly as it went on the wire
    state: JobState = JobState.READY
    attempts: int = 0
    max_attempts: int = 5
    enqueued_at: float = field(default_factory=time.time)
    lease_until: float = 0.0
    not_before: float = 0.0           # earliest time the job may be claimed (delay/backoff)
    priority: int = 0                 # higher is claimed first
    seq: int = 0                      # monotonic tiebreak → FIFO within a priority
    last_error: Optional[str] = None
    result: Optional[str] = None      # base64 of the handler's return, when kept
    completed_at: float = 0.0
    wf: Optional[str] = None          # JSON workflow continuation (chain/chord), if any
    correlation_id: Optional[str] = None  # from enqueue attachment, for tracing
    traceparent: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "data": base64.b64encode(self.data).decode("ascii"),
            "state": self.state.value,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "enqueued_at": self.enqueued_at,
            "lease_until": self.lease_until,
            "not_before": self.not_before,
            "priority": self.priority,
            "seq": self.seq,
            "last_error": self.last_error,
            "result": self.result,
            "completed_at": self.completed_at,
            "wf": self.wf,
            "correlation_id": self.correlation_id,
            "traceparent": self.traceparent,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "JobRecord":
        return cls(
            id=d["id"],
            data=base64.b64decode(d["data"]),
            state=JobState(d["state"]),
            attempts=d["attempts"],
            max_attempts=d["max_attempts"],
            enqueued_at=d["enqueued_at"],
            lease_until=d["lease_until"],
            not_before=d.get("not_before", 0.0),
            priority=d.get("priority", 0),
            seq=d.get("seq", 0),
            last_error=d.get("last_error"),
            result=d.get("result"),
            completed_at=d.get("completed_at", 0.0),
            wf=d.get("wf"),
            correlation_id=d.get("correlation_id"),
            traceparent=d.get("traceparent"),
        )


@dataclass(frozen=True)
class JobContext:
    """What the queue knows about *this* delivery of a job.

    A worker that names a ``ctx`` parameter is handed one, the same way a handler
    that names ``db`` is handed the app's storage::

        @app.worker("jobs/email")
        async def send(job, ctx: JobContext):
            if ctx.is_last_attempt:
                log.warning("final try for %s: %s", ctx.job_id, ctx.last_error)

    The job body stays exactly what was enqueued — delivery metadata lives here
    rather than being mixed into the caller's data.
    """

    job_id: str
    queue: str
    attempt: int                      # 1 on first delivery, 2 on first redelivery
    max_attempts: int
    last_error: Optional[str] = None  # why the previous attempt nacked, if it did
    correlation_id: Optional[str] = None
    traceparent: Optional[str] = None

    @property
    def is_retry(self) -> bool:
        """True when a previous attempt at this job failed."""
        return self.attempt > 1

    @property
    def is_last_attempt(self) -> bool:
        """True when raising will dead-letter the job instead of redelivering it."""
        return self.attempt >= self.max_attempts


class QueueStore:
    """Authoritative job state for one queue, held in the owner's memory and
    (optionally) written through to a ``StoragePlugin`` so it survives a restart.

    A single owner serializes every mutation through ``_lock``, which is what
    makes claim/ack atomic without a database transaction — there is only ever
    one writer.
    """

    def __init__(
        self,
        name: str,
        storage: Any = None,
        *,
        keep_results: bool = False,
        result_ttl_s: float = 3600.0,
    ) -> None:
        self._name = name
        self._storage = storage
        self.keep_results = keep_results
        self.result_ttl_s = result_ttl_s
        self._jobs: Dict[str, JobRecord] = {}
        self._ids: set = set()                              # membership, mirrored to storage
        self._ready: List[Tuple[int, int, str]] = []        # (-priority, seq, id)
        self._delayed: List[Tuple[float, str]] = []         # (not_before, id)
        self._leases: List[Tuple[float, str]] = []          # (lease_until, id)
        self._done: List[Tuple[float, str]] = []            # (expiry, id)
        self._chords: Dict[str, dict] = {}                  # chord_id → barrier state
        self._seq = 0
        self._lock = asyncio.Lock()

    # --- heap bookkeeping (all O(log n); entries are validated lazily on pop) ---

    def _offer_ready(self, rec: JobRecord) -> None:
        if rec.not_before > time.time():
            heapq.heappush(self._delayed, (rec.not_before, rec.id))
        else:
            heapq.heappush(self._ready, (-rec.priority, rec.seq, rec.id))

    def _mature_delayed(self, now: float) -> None:
        while self._delayed and self._delayed[0][0] <= now:
            _, job_id = heapq.heappop(self._delayed)
            rec = self._jobs.get(job_id)
            if rec is not None and rec.state == JobState.READY and rec.not_before <= now:
                heapq.heappush(self._ready, (-rec.priority, rec.seq, rec.id))

    def _reclaim_leases(self, now: float) -> List[JobRecord]:
        """Move expired leases back to ready (or dead). Returns the changed records
        so the caller can persist them."""
        changed: List[JobRecord] = []
        while self._leases and self._leases[0][0] <= now:
            _, job_id = heapq.heappop(self._leases)
            rec = self._jobs.get(job_id)
            if rec is None or rec.state != JobState.LEASED or rec.lease_until > now:
                continue  # stale entry: job was acked, nacked, or re-leased
            if rec.attempts >= rec.max_attempts:
                rec.state = JobState.DEAD
            else:
                rec.state = JobState.READY
                rec.lease_until = 0.0
                rec.not_before = 0.0  # a lost lease is redelivered promptly
                heapq.heappush(self._ready, (-rec.priority, rec.seq, rec.id))
            changed.append(rec)
        return changed

    # --- persistence (per-job writes are O(1); the index only moves on membership) ---

    def _index_key(self) -> str:
        return f"queue:{self._name}:index"

    def _job_key(self, job_id: str) -> str:
        return f"queue:{self._name}:job:{job_id}"

    async def load(self) -> None:
        """Recover state from storage on bind. No-op for the in-memory default."""
        if self._storage is None:
            return
        try:
            index = await self._storage.get(self._index_key()) or []
            for job_id in index:
                raw = await self._storage.get(self._job_key(job_id))
                if raw is None:
                    continue
                rec = JobRecord.from_dict(raw if isinstance(raw, dict) else json.loads(raw))
                self._jobs[rec.id] = rec
                self._ids.add(rec.id)
                self._seq = max(self._seq, rec.seq)
                if rec.state == JobState.READY:
                    self._offer_ready(rec)
                elif rec.state == JobState.LEASED:
                    heapq.heappush(self._leases, (rec.lease_until, rec.id))
                elif rec.state == JobState.DONE:
                    heapq.heappush(self._done, (rec.completed_at + self.result_ttl_s, rec.id))
        except Exception:  # recovery is best-effort — never block startup
            _logger.exception("Queue %s failed to recover from storage", self._name)

    async def _write_job(self, rec: JobRecord) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.put(self._job_key(rec.id), rec.to_dict())
        except Exception:
            _logger.exception("Queue %s failed to persist job %s", self._name, rec.id)

    async def _write_index(self) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.put(self._index_key(), list(self._ids))
        except Exception:
            _logger.exception("Queue %s failed to persist index", self._name)

    async def _forget(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)
        self._ids.discard(job_id)
        if self._storage is not None:
            try:
                await self._storage.delete(self._job_key(job_id))
            except Exception:
                _logger.exception("Queue %s failed to erase job %s", self._name, job_id)
        await self._write_index()

    # --- operations ---

    async def add(
        self, data: bytes, *, max_attempts: int, priority: int = 0, delay_s: float = 0.0,
        wf: Optional[str] = None,
        correlation_id: Optional[str] = None,
        traceparent: Optional[str] = None,
    ) -> str:
        async with self._lock:
            self._seq += 1
            rec = JobRecord(
                id=uuid.uuid4().hex, data=data, max_attempts=max_attempts,
                priority=priority, seq=self._seq, wf=wf,
                not_before=time.time() + delay_s if delay_s > 0 else 0.0,
                correlation_id=correlation_id,
                traceparent=traceparent,
            )
            self._jobs[rec.id] = rec
            self._ids.add(rec.id)
            self._offer_ready(rec)
            await self._write_job(rec)
            await self._write_index()
        return rec.id

    async def claim(self, *, lease_s: float) -> Optional[JobRecord]:
        """Lease the highest-priority eligible job (FIFO within a priority) in
        O(log n): pop the ready heap, skipping stale entries. Matures delayed jobs
        and reclaims expired leases first. Returns a copy so callers can't mutate
        state off-lock."""
        now = time.time()
        async with self._lock:
            self._mature_delayed(now)
            for reclaimed in self._reclaim_leases(now):
                await self._write_job(reclaimed)
            while self._ready:
                _, _, job_id = heapq.heappop(self._ready)
                rec = self._jobs.get(job_id)
                if rec is None or rec.state != JobState.READY or rec.not_before > now:
                    continue  # tombstone: job changed state since it was queued
                rec.state = JobState.LEASED
                rec.attempts += 1
                rec.lease_until = now + lease_s
                heapq.heappush(self._leases, (rec.lease_until, rec.id))
                await self._write_job(rec)
                return JobRecord.from_dict(rec.to_dict())
        return None

    async def ack(self, job_id: str, *, result: Optional[bytes] = None) -> bool:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return False
            if self.keep_results:
                rec.state = JobState.DONE
                rec.result = base64.b64encode(result).decode("ascii") if result is not None else None
                rec.completed_at = time.time()
                rec.lease_until = 0.0
                heapq.heappush(self._done, (rec.completed_at + self.result_ttl_s, rec.id))
                await self._write_job(rec)
            else:
                await self._forget(job_id)
        return True

    async def nack(
        self, job_id: str, *, error: Optional[str] = None, retry_delay_s: float = 0.0,
    ) -> Optional[str]:
        """Fail a leased job. Redelivers (after ``retry_delay_s``) if attempts
        remain, else dead-letters. Returns "requeued" or "dead" (or None if
        the job is unknown)."""
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return None
            rec.last_error = error
            if rec.attempts >= rec.max_attempts:
                rec.state = JobState.DEAD
                disposition = "dead"
            else:
                rec.state = JobState.READY
                rec.lease_until = 0.0
                rec.not_before = time.time() + retry_delay_s if retry_delay_s > 0 else 0.0
                self._offer_ready(rec)
                disposition = "requeued"
            await self._write_job(rec)
        return disposition

    async def sweep(self) -> int:
        """Reclaim jobs whose lease expired (worker crashed without ack) and purge
        results past their TTL. Returns how many leases were reclaimed."""
        now = time.time()
        async with self._lock:
            changed = self._reclaim_leases(now)
            for reclaimed in changed:
                await self._write_job(reclaimed)
            while self._done and self._done[0][0] <= now:
                _, job_id = heapq.heappop(self._done)
                rec = self._jobs.get(job_id)
                if (
                    rec is not None and rec.state == JobState.DONE
                    and rec.completed_at + self.result_ttl_s <= now
                ):
                    await self._forget(job_id)
        return len(changed)

    async def result(self, job_id: str) -> Tuple[str, Optional[bytes]]:
        """Return ``(state, result_bytes)`` for a job. State is "unknown" once the
        job (and its retained result) is gone."""
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return "unknown", None
            data = base64.b64decode(rec.result) if rec.result is not None else None
            return rec.state.value, data

    async def dead_letters(self) -> List[JobRecord]:
        async with self._lock:
            return [
                JobRecord.from_dict(r.to_dict())
                for r in self._jobs.values() if r.state == JobState.DEAD
            ]

    async def stats(self) -> Dict[str, int]:
        async with self._lock:
            counts = {s.value: 0 for s in JobState}
            for rec in self._jobs.values():
                counts[rec.state.value] += 1
            return counts

    async def chord_report(
        self, chord_id: str, index: int, size: int, result: Optional[str],
    ) -> Optional[List[Optional[str]]]:
        """Record one chord member's completion. Returns the collected results (in
        member order) exactly once — to the caller that completes the barrier — and
        None for every earlier member. Results are base64 strings (or None)."""
        async with self._lock:
            chord = self._chords.get(chord_id)
            if chord is None:
                chord = {
                    "size": size, "results": [None] * size,
                    "reported": set(), "fired": False,
                }
                self._chords[chord_id] = chord
            if chord["fired"] or index in chord["reported"]:
                return None  # already fired, or this member was already counted (redelivery)
            chord["reported"].add(index)
            chord["results"][index] = result
            if len(chord["reported"]) >= chord["size"]:
                chord["fired"] = True
                results = list(chord["results"])
                del self._chords[chord_id]
                return results
            return None


def _encode_wf(obj: dict) -> str:
    """Pack a workflow continuation into a selector-safe string."""
    return base64.b64encode(json.dumps(obj).encode("utf-8")).decode("ascii")


def _decode_wf(s: str) -> dict:
    result: dict = json.loads(base64.b64decode(s))
    return result
