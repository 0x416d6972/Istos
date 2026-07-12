"""Work queues — jobs, not events.

Pub/sub fans a message out to every subscriber; a work queue hands each job to
*one* worker and doesn't consider it done until that worker acknowledges it. If
the worker crashes mid-job the lease expires and the job is redelivered; after
enough failed attempts it lands in a dead-letter list instead of looping forever.

Istos has no broker to hold that state, so one node plays the **queue owner**: a
:class:`QueueRole` bound on the shared session that answers enqueue / claim / ack
/ nack over Zenoh queryables and sweeps expired leases in the background. Workers
elsewhere on the mesh claim from it with :meth:`Istos.worker`; because every claim
goes through the single owner, competing workers never get the same job twice.

    # owner (holds the queue)
    app.queue("jobs/email", lease_s=30, max_attempts=5)

    # worker (anywhere on the mesh) — return to ack, raise to nack/redeliver
    @app.worker("jobs/email", concurrency=4)
    async def send(job):
        await smtp.send(job["to"])

    # producer
    await app.enqueue("jobs/email", {"to": "a@b.com"})

The owner keeps the authoritative state in memory but writes every transition
through to the app's ``StoragePlugin``; with the in-memory default the queue is
fast-but-volatile, and with Redis/SQLAlchemy configured it survives an owner
restart (recovered on bind).
"""

from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from istos.logging import get_logger

_logger = get_logger("queue")


class JobState(str, Enum):
    READY = "ready"      # waiting to be claimed
    LEASED = "leased"    # handed to a worker, ack pending
    DEAD = "dead"        # exhausted its attempts, parked for inspection


@dataclass
class JobRecord:
    id: str
    data: bytes                       # the job body, exactly as it went on the wire
    state: JobState = JobState.READY
    attempts: int = 0
    max_attempts: int = 5
    enqueued_at: float = field(default_factory=time.time)
    lease_until: float = 0.0
    last_error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "data": base64.b64encode(self.data).decode("ascii"),
            "state": self.state.value,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "enqueued_at": self.enqueued_at,
            "lease_until": self.lease_until,
            "last_error": self.last_error,
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
            last_error=d.get("last_error"),
        )


class QueueStore:
    """Authoritative job state for one queue, held in the owner's memory and
    (optionally) written through to a ``StoragePlugin`` so it survives a restart.

    A single owner serializes every mutation through ``_lock``, which is what
    makes claim/ack atomic without a database transaction — there is only ever
    one writer.
    """

    def __init__(self, name: str, storage: Any = None) -> None:
        self._name = name
        self._storage = storage
        self._jobs: Dict[str, JobRecord] = {}
        self._order: List[str] = []          # insertion order → FIFO claim
        self._lock = asyncio.Lock()

    # --- persistence keys ---

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
                if raw is not None:
                    rec = JobRecord.from_dict(raw if isinstance(raw, dict) else json.loads(raw))
                    self._jobs[rec.id] = rec
                    self._order.append(rec.id)
        except Exception:  # recovery is best-effort — never block startup
            _logger.exception("Queue %s failed to recover from storage", self._name)

    async def _write(self, rec: JobRecord) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.put(self._job_key(rec.id), rec.to_dict())
            await self._storage.put(self._index_key(), list(self._order))
        except Exception:
            _logger.exception("Queue %s failed to persist job %s", self._name, rec.id)

    async def _erase(self, job_id: str) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.delete(self._job_key(job_id))
            await self._storage.put(self._index_key(), list(self._order))
        except Exception:
            _logger.exception("Queue %s failed to erase job %s", self._name, job_id)

    # --- operations ---

    async def add(self, data: bytes, *, max_attempts: int) -> str:
        rec = JobRecord(id=uuid.uuid4().hex, data=data, max_attempts=max_attempts)
        async with self._lock:
            self._jobs[rec.id] = rec
            self._order.append(rec.id)
            await self._write(rec)
        return rec.id

    async def claim(self, *, lease_s: float) -> Optional[JobRecord]:
        """Lease the oldest job that is ready (or whose lease has expired). Returns
        a copy so callers can't mutate authoritative state off-lock."""
        now = time.time()
        async with self._lock:
            for job_id in self._order:
                rec = self._jobs[job_id]
                claimable = rec.state == JobState.READY or (
                    rec.state == JobState.LEASED and rec.lease_until <= now
                )
                if not claimable:
                    continue
                rec.state = JobState.LEASED
                rec.attempts += 1
                rec.lease_until = now + lease_s
                await self._write(rec)
                return JobRecord.from_dict(rec.to_dict())
        return None

    async def ack(self, job_id: str) -> bool:
        async with self._lock:
            if job_id not in self._jobs:
                return False
            del self._jobs[job_id]
            self._order.remove(job_id)
            await self._erase(job_id)
        return True

    async def nack(self, job_id: str, *, error: Optional[str] = None) -> Optional[str]:
        """Fail a leased job. Redelivers if attempts remain, else dead-letters.
        Returns "requeued" or "dead" (or None if the job is unknown)."""
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
                disposition = "requeued"
            await self._write(rec)
        return disposition

    async def sweep(self) -> int:
        """Reclaim jobs whose lease expired (worker crashed without ack). Redeliver
        those with attempts left, dead-letter the rest. Returns how many moved."""
        now = time.time()
        moved = 0
        async with self._lock:
            for rec in self._jobs.values():
                if rec.state == JobState.LEASED and rec.lease_until <= now:
                    if rec.attempts >= rec.max_attempts:
                        rec.state = JobState.DEAD
                    else:
                        rec.state = JobState.READY
                        rec.lease_until = 0.0
                    await self._write(rec)
                    moved += 1
        return moved

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


import zenoh  # noqa: E402  (kept next to the role that uses it)

from istos.core.authz import AuthContext, Authorizer, check_authorized  # noqa: E402
from istos.core.errors import UnauthorizedError  # noqa: E402
from istos.gateway import decode_params  # noqa: E402


def _reply(query: "zenoh.Query", obj: dict) -> None:
    query.reply(str(query.key_expr), json.dumps(obj).encode("utf-8"))


def _query_params(query: "zenoh.Query") -> dict:
    params = getattr(query.selector, "parameters", None)
    if not params:
        return {}
    return decode_params(dict(params))


def _query_payload(query: "zenoh.Query") -> bytes:
    payload = getattr(query, "payload", None)
    return bytes(payload) if payload is not None else b""


class QueueRole:
    """The queue owner. Binds enqueue / claim / ack / nack / dead queryables on the
    shared session and reclaims expired leases on a timer. One owner per queue."""

    def __init__(
        self,
        prefix: str,
        store: QueueStore,
        *,
        lease_s: float = 30.0,
        max_attempts: int = 5,
        sweep_interval_s: float = 5.0,
        authorizer: Optional[Authorizer] = None,
        logger: Optional[Any] = None,
    ) -> None:
        self.prefix = prefix.rstrip("/")
        self.store = store
        self.lease_s = lease_s
        self.max_attempts = max_attempts
        self.sweep_interval_s = sweep_interval_s
        self._authorizer = authorizer
        self._logger = logger or _logger
        self._queryables: List[Any] = []
        self._sweeper: Optional[asyncio.Task] = None

    async def bind(self, session: "zenoh.Session", loop: asyncio.AbstractEventLoop) -> None:
        routes = {
            "enqueue": self._on_enqueue,
            "claim": self._on_claim,
            "ack": self._on_ack,
            "nack": self._on_nack,
            "dead": self._on_dead,
        }

        def _make(handler: Callable) -> Callable:
            def _cb(query: "zenoh.Query") -> None:
                if loop.is_closed():
                    return
                asyncio.run_coroutine_threadsafe(handler(query), loop)
            return _cb

        await self.store.load()
        for verb, handler in routes.items():
            q = session.declare_queryable(f"{self.prefix}/{verb}", _make(handler))
            self._queryables.append(q)
        self._sweeper = asyncio.ensure_future(self._sweep_loop())
        self._logger.info("Bound queue owner for %s", self.prefix, extra={"prefix": self.prefix})

    async def _authorize(self, query: "zenoh.Query", params: dict) -> bool:
        if self._authorizer is None:
            return True
        raw = getattr(query, "attachment", None)
        attachment = bytes(raw) if raw is not None else None
        try:
            await check_authorized(
                self._authorizer,
                AuthContext(
                    prefix=self.prefix, key_expr=str(query.key_expr), params=params,
                    attachment=attachment, operation="queue",
                ),
            )
            return True
        except UnauthorizedError as e:
            _reply(query, {"error": e.message, "code": "unauthorized"})
            return False

    async def _on_enqueue(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        job_id = await self.store.add(_query_payload(query), max_attempts=self.max_attempts)
        _reply(query, {"job_id": job_id})

    async def _on_claim(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        rec = await self.store.claim(lease_s=self.lease_s)
        if rec is None:
            _reply(query, {"empty": True})
            return
        _reply(query, {
            "job_id": rec.id,
            "attempt": rec.attempts,
            "max_attempts": rec.max_attempts,
            "data": base64.b64encode(rec.data).decode("ascii"),
        })

    async def _on_ack(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        ok = await self.store.ack(params.get("job_id", ""))
        _reply(query, {"ok": ok})

    async def _on_nack(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        error = _query_payload(query).decode("utf-8") or None
        disposition = await self.store.nack(params.get("job_id", ""), error=error)
        _reply(query, {"ok": disposition is not None, "disposition": disposition})

    async def _on_dead(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        dead = await self.store.dead_letters()
        _reply(query, {
            "jobs": [
                {
                    "job_id": r.id,
                    "attempts": r.attempts,
                    "last_error": r.last_error,
                    "data": base64.b64encode(r.data).decode("ascii"),
                }
                for r in dead
            ]
        })

    async def _sweep_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.sweep_interval_s)
                moved = await self.store.sweep()
                if moved:
                    self._logger.info(
                        "Queue %s reclaimed %d expired lease(s)", self.prefix, moved,
                        extra={"prefix": self.prefix},
                    )
        except asyncio.CancelledError:
            pass

    def unbind(self) -> None:
        if self._sweeper is not None:
            self._sweeper.cancel()
            self._sweeper = None
        for q in self._queryables:
            try:
                q.undeclare()
            except Exception:  # pragma: no cover - best-effort teardown
                pass
        self._queryables.clear()

    async def aclose(self) -> None:
        self.unbind()


import inspect  # noqa: E402

from istos.di.depends import (  # noqa: E402
    has_dependencies,
    invoke_with_dependencies,
    positional_param_names,
)
from istos.messages.serialization import Serialize  # noqa: E402


class worker_wrapper:
    """A competing consumer. Runs ``concurrency`` claim→run→ack loops against a
    queue owner elsewhere on the mesh. The handler returning acks the job; the
    handler raising nacks it (redeliver until attempts run out, then dead-letter)."""

    def __init__(
        self,
        func: Callable,
        prefix: str,
        *,
        concurrency: int = 1,
        poll_interval_s: float = 1.0,
        serializer: Serialize,
        token: Optional[Any] = None,
        dependency_overrides: Optional[dict] = None,
    ) -> None:
        self.func = func
        self.prefix = prefix.rstrip("/")
        self.concurrency = max(1, concurrency)
        self.poll_interval_s = poll_interval_s
        self.serializer = serializer
        self._token = token
        self._has_depends = has_dependencies(func)
        self._skip_names = tuple(positional_param_names(func)[:1])  # the job param
        self._overrides = dependency_overrides or {}
        self._app: Any = None
        self._tasks: List[asyncio.Task] = []
        self._running = False

    def start(self, app: Any) -> None:
        self._app = app
        self._running = True
        for _ in range(self.concurrency):
            self._tasks.append(asyncio.ensure_future(self._loop()))

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()

    async def _run_body(self, job: Any) -> Any:
        if self._has_depends:
            return await invoke_with_dependencies(
                self.func, args=(job,), skip_names=self._skip_names,
                overrides=self._overrides,
            )
        if inspect.iscoroutinefunction(self.func):
            return await self.func(job)
        return await asyncio.to_thread(self.func, job)

    async def _loop(self) -> None:
        while self._running:
            try:
                reply = await self._app._queue_claim(self.prefix, token=self._token)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(self.poll_interval_s)
                continue

            if not reply or reply.get("empty"):
                await asyncio.sleep(self.poll_interval_s)
                continue

            job_id = reply["job_id"]
            try:
                job = self.serializer.deserialize(base64.b64decode(reply["data"]))
                await self._run_body(job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _logger.exception(
                    "Worker for %s failed job %s (attempt %s)",
                    self.prefix, job_id, reply.get("attempt"),
                    extra={"prefix": self.prefix},
                )
                try:
                    await self._app._queue_nack(self.prefix, job_id, error=str(exc), token=self._token)
                except Exception:
                    pass
                continue

            try:
                await self._app._queue_ack(self.prefix, job_id, token=self._token)
            except Exception:
                # The lease will expire and the job redeliver — at-least-once.
                _logger.exception(
                    "Worker for %s could not ack job %s", self.prefix, job_id,
                    extra={"prefix": self.prefix},
                )
