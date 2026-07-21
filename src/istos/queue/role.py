"""The queue owner: enqueue/claim/ack/nack/result queryables, lease sweeper,
and (in HA mode) liveliness leader election."""

import asyncio
import base64
import json
import random
import uuid
from typing import Any, Callable, List, Optional

import zenoh

from istos.context import RequestEnvelope
from istos.logging import get_logger
from istos.security.authz import AuthContext, Authorizer, check_authorized
from istos.errors import ErrorResponse, UnauthorizedError
from istos.http.gateway import decode_params
from istos.queue.store import QueueStore

_logger = get_logger("queue")


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

def _as_float(params: dict, key: str, default: float) -> float:
    try:
        return float(params.get(key, default))
    except (TypeError, ValueError):
        return default


def _as_int(params: dict, key: str, default: int) -> int:
    try:
        return int(params.get(key, default))
    except (TypeError, ValueError):
        return default


class QueueRole:
    """The queue owner. Binds enqueue / claim / ack / nack / result queryables on
    the shared session, publishes a nudge when a job arrives, and reclaims expired
    leases on a timer. One owner per queue."""

    def __init__(
        self,
        prefix: str,
        store: QueueStore,
        *,
        lease_s: float = 30.0,
        max_attempts: int = 5,
        sweep_interval_s: float = 5.0,
        retry_backoff_s: float = 0.0,
        retry_backoff_max_s: float = 600.0,
        retry_jitter: float = 0.1,
        ha: bool = False,
        authorizer: Optional[Authorizer] = None,
        logger: Optional[Any] = None,
    ) -> None:
        self.prefix = prefix.rstrip("/")
        self.store = store
        self.lease_s = lease_s
        self.max_attempts = max_attempts
        self.sweep_interval_s = sweep_interval_s
        self.retry_backoff_s = retry_backoff_s
        self.retry_backoff_max_s = retry_backoff_max_s
        self.retry_jitter = retry_jitter
        self.ha = ha
        self._authorizer = authorizer
        self._logger = logger or _logger
        self._session: Optional[Any] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queryables: List[Any] = []
        self._sweeper: Optional[asyncio.Task] = None
        # Leader election (HA mode).
        self._id = uuid.uuid4().hex
        self._members: set = set()
        self._active = False
        self._live_token: Optional[Any] = None
        self._liveliness_sub: Optional[Any] = None
        self._election_lock = asyncio.Lock()

    @property
    def is_active(self) -> bool:
        """Whether this node is the queue's live owner right now — always, without
        ``ha``; only the elected leader with it. Used to run a co-located
        :meth:`schedule` beat on exactly one node."""
        return self._active

    def _backoff(self, attempts: int) -> float:
        """Exponential backoff with jitter for the retry after ``attempts`` tries."""
        if self.retry_backoff_s <= 0:
            return 0.0
        delay: float = min(self.retry_backoff_s * (2 ** max(0, attempts - 1)), self.retry_backoff_max_s)
        if self.retry_jitter:
            delay *= 1.0 + random.uniform(-self.retry_jitter, self.retry_jitter)
        return max(0.0, delay)

    def _notify(self) -> None:
        """Nudge idle workers that a job is ready, so they claim without waiting
        for their next poll. Best-effort — a missed nudge is caught by the poll."""
        if self._session is not None:
            try:
                self._session.put(f"{self.prefix}/notify", b"1")
            except Exception:  # pragma: no cover - nudge is optional
                pass

    async def bind(self, session: "zenoh.Session", loop: asyncio.AbstractEventLoop) -> None:
        self._session = session
        self._loop = loop
        if not self.ha:
            await self._activate()
            return
        # HA: elect a single leader among the owner replicas via Zenoh liveliness.
        # Each replica declares a token; the lowest id among the live set leads and
        # binds the queryables. When it dies, its token drops and the next takes over.
        me = f"{self.prefix}/_owners/{self._id}"
        self._live_token = session.liveliness().declare_token(me)

        def _on_live(sample: "zenoh.Sample") -> None:
            if not loop.is_closed():
                asyncio.run_coroutine_threadsafe(self._on_membership(sample), loop)

        # history=True replays the currently-alive tokens so we see peers already up.
        self._liveliness_sub = session.liveliness().declare_subscriber(
            f"{self.prefix}/_owners/*", _on_live, history=True,
        )
        async with self._election_lock:
            self._members.add(self._id)
            await self._reconcile()

    async def _on_membership(self, sample: "zenoh.Sample") -> None:
        owner_id = str(sample.key_expr).rsplit("/", 1)[-1]
        async with self._election_lock:
            if sample.kind == zenoh.SampleKind.PUT:
                self._members.add(owner_id)
            else:
                self._members.discard(owner_id)
            await self._reconcile()

    async def _reconcile(self) -> None:
        leader = min(self._members) if self._members else None
        if leader == self._id and not self._active:
            self._logger.info("Queue %s: became owner (leader)", self.prefix, extra={"prefix": self.prefix})
            await self._activate()
        elif leader != self._id and self._active:
            self._logger.info("Queue %s: stepped down as owner", self.prefix, extra={"prefix": self.prefix})
            await self._deactivate()

    async def _activate(self) -> None:
        loop = self._loop
        session = self._session
        assert loop is not None and session is not None
        routes = {
            "enqueue": self._on_enqueue,
            "claim": self._on_claim,
            "ack": self._on_ack,
            "nack": self._on_nack,
            "result": self._on_result,
            "dead": self._on_dead,
            "chord": self._on_chord,
        }

        def _make(handler: Callable) -> Callable:
            def _cb(query: "zenoh.Query") -> None:
                if not loop.is_closed():
                    asyncio.run_coroutine_threadsafe(handler(query), loop)
            return _cb

        await self.store.load()
        for verb, handler in routes.items():
            q = session.declare_queryable(f"{self.prefix}/{verb}", _make(handler))
            self._queryables.append(q)
        self._sweeper = asyncio.ensure_future(self._sweep_loop())
        self._active = True
        self._logger.info("Bound queue owner for %s", self.prefix, extra={"prefix": self.prefix})

    async def _deactivate(self) -> None:
        if self._sweeper is not None:
            self._sweeper.cancel()
            self._sweeper = None
        for q in self._queryables:
            try:
                q.undeclare()
            except Exception:  # pragma: no cover
                pass
        self._queryables.clear()
        self._active = False

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
            _reply(query, ErrorResponse(
                error=e.code, code=e.code, message=e.message,
            ).to_dict())
            return False

    async def _on_enqueue(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        wf = params.get("wf")  # base64 JSON continuation, opaque to the owner
        raw = getattr(query, "attachment", None)
        env = RequestEnvelope.from_attachment(bytes(raw) if raw is not None else None)
        job_id = await self.store.add(
            _query_payload(query),
            max_attempts=self.max_attempts,
            priority=_as_int(params, "priority", 0),
            delay_s=_as_float(params, "delay", 0.0),
            wf=wf,
            correlation_id=env.correlation_id,
            traceparent=env.traceparent,
        )
        if _as_float(params, "delay", 0.0) <= 0:
            self._notify()
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
            "last_error": rec.last_error,   # why the previous attempt nacked, if any
            "data": base64.b64encode(rec.data).decode("ascii"),
            "wf": rec.wf,
            "correlation_id": rec.correlation_id,
            "traceparent": rec.traceparent,
        })

    async def _on_ack(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        result = _query_payload(query) or None
        ok = await self.store.ack(params.get("job_id", ""), result=result)
        _reply(query, {"ok": ok})

    async def _on_nack(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        error = _query_payload(query).decode("utf-8") or None
        attempt = _as_int(params, "attempt", 1)
        disposition = await self.store.nack(
            params.get("job_id", ""), error=error, retry_delay_s=self._backoff(attempt),
        )
        _reply(query, {"ok": disposition is not None, "disposition": disposition})

    async def _on_result(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        state, data = await self.store.result(params.get("job_id", ""))
        _reply(query, {
            "state": state,
            "result": base64.b64encode(data).decode("ascii") if data is not None else None,
        })

    async def _on_chord(self, query: "zenoh.Query") -> None:
        params = _query_params(query)
        if not await self._authorize(query, params):
            return
        result_b64 = _query_payload(query).decode("ascii") or None
        results = await self.store.chord_report(
            params.get("chord_id", ""), _as_int(params, "index", 0),
            _as_int(params, "size", 1), result_b64,
        )
        # `complete` is True (with results) for exactly one member; None otherwise.
        _reply(query, {"complete": results is not None, "results": results})

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
                    self._notify()  # reclaimed jobs are ready again
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
        for handle in (self._liveliness_sub, self._live_token):
            if handle is not None:
                try:
                    handle.undeclare()
                except Exception:  # pragma: no cover
                    pass
        self._liveliness_sub = None
        self._live_token = None
        self._active = False
        self._session = None

    async def aclose(self) -> None:
        self.unbind()


