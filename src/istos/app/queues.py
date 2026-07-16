"""Work queues: @queue owner, @worker, enqueue/result/dead_letters, chain/group/chord and scheduling."""

import asyncio
import base64
import json
import uuid
import zenoh
from typing import Any, Callable, List, Optional, Tuple, Union

from istos.messages.serialization import Serialize, JsonSerializer
from istos.queue import QueueRole, QueueStore, worker_wrapper, _encode_wf
from istos.queue.cron import CronSchedule
from istos.errors import (
    IstosError,
    error_from_payload,
    is_error_payload,
)
from istos.security.authz import Authorizer, combine_authorizers
from istos.context import RequestEnvelope, peek_request_context
from istos.http.gateway import build_selector

from istos.app._base import IstosBase


class _QueueMixin(IstosBase):
    """Work queues: @queue owner, @worker, enqueue/result/dead_letters, chain/group/chord and scheduling."""

    def queue(
        self,
        prefix: str,
        *,
        lease_s: float = 30.0,
        max_attempts: int = 5,
        sweep_interval_s: float = 5.0,
        retry_backoff_s: float = 1.0,
        retry_backoff_max_s: float = 600.0,
        retry_jitter: float = 0.1,
        keep_results: bool = False,
        result_ttl_s: float = 3600.0,
        ha: bool = False,
        authorizer: Optional[Authorizer] = None,
        store: Optional[QueueStore] = None,
    ) -> QueueRole:
        """Own a work queue at ``prefix`` — a job goes to exactly one worker and
        isn't done until that worker acks it.

            app.queue("jobs/email", lease_s=30, max_attempts=5)

        This node holds the authoritative state and answers enqueue / claim / ack /
        nack / result over Zenoh, reclaims leases whose worker went away, and nudges
        idle workers when a job arrives. Run it on a node of its own for a dedicated
        queue, or alongside the producer. Workers (see :meth:`worker`) may live
        anywhere on the mesh.

        State is kept in memory and written through to the app's storage, so with
        the in-memory default the queue is volatile and with Redis/SQLAlchemy it
        survives an owner restart. ``lease_s`` is how long a claimed job may run
        before it is considered lost; a failed job is retried with exponential
        backoff (``retry_backoff_s`` base, doubling, capped at ``retry_backoff_max_s``,
        ``retry_jitter`` spread) up to ``max_attempts`` times, then dead-lettered.
        With ``keep_results=True`` the handler's return value is retained for
        ``result_ttl_s`` seconds and readable via :meth:`result`.

        ``ha=True`` runs this owner as one of several replicas that elect a single
        leader over Zenoh liveliness; if the leader dies a standby takes over. HA
        needs a shared ``StoragePlugin`` (Redis/SQLAlchemy) so the new leader
        recovers the jobs — with the in-memory default each replica is isolated.
        """
        role = QueueRole(
            prefix,
            store or QueueStore(
                prefix.rstrip("/"), self._storage,
                keep_results=keep_results, result_ttl_s=result_ttl_s,
            ),
            lease_s=lease_s, max_attempts=max_attempts,
            sweep_interval_s=sweep_interval_s,
            retry_backoff_s=retry_backoff_s,
            retry_backoff_max_s=retry_backoff_max_s,
            retry_jitter=retry_jitter,
            ha=ha,
            authorizer=combine_authorizers(self._authorizer, authorizer),
            logger=self._logger,
        )
        self._queue_roles.append(role)
        return role

    def worker(
        self,
        prefix: str,
        *,
        concurrency: int = 1,
        poll_interval_s: float = 1.0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
    ) -> Callable:
        """Consume a work queue. The body takes the decoded job; returning acks it,
        raising nacks it (redelivered until ``max_attempts``, then dead-lettered)::

            @app.worker("jobs/email", concurrency=4)
            async def send(job):
                await smtp.send(job["to"])   # return → ack, raise → retry

        Run ``concurrency`` claim loops per process; run the decorated app on more
        processes to add competing consumers. The queue owner (see :meth:`queue`)
        hands each job to one claimer, so a job is never processed twice at once.
        ``Depends(...)`` parameters are injected like any other handler.
        """
        def decorator(func: Callable) -> worker_wrapper:
            wrapper = worker_wrapper(
                func, prefix,
                concurrency=concurrency, poll_interval_s=poll_interval_s,
                serializer=serializer or JsonSerializer(),
                token=token, dependency_overrides=self.dependency_overrides,
            )
            self._workers.append(wrapper)
            return wrapper
        return decorator

    async def enqueue(
        self,
        prefix: str,
        data: Any,
        *,
        delay_s: float = 0.0,
        priority: int = 0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
        timeout_s: float = 5.0,
    ) -> str:
        """Put a job on the queue and return its id. Reaches the queue owner over
        the mesh, so the producer needn't be the owner. ``delay_s`` holds the job
        back until that many seconds have passed; ``priority`` (higher first) jumps
        it ahead of lower-priority work."""
        _serializer = serializer or JsonSerializer()
        body = _serializer.serialize(data)
        payload = body.encode("utf-8") if isinstance(body, str) else body
        return await self._queue_enqueue(
            prefix, payload, delay_s=delay_s, priority=priority, token=token, timeout_s=timeout_s,
        )

    async def _queue_enqueue(
        self, prefix: str, payload: bytes, *, delay_s: float = 0.0, priority: int = 0,
        wf: Optional[str] = None, token: Any = None, timeout_s: float = 5.0,
    ) -> str:
        params: dict = {}
        if delay_s > 0:
            params["delay"] = delay_s
        if priority:
            params["priority"] = priority
        if wf is not None:
            params["wf"] = wf
        selector = build_selector(f"{prefix.rstrip('/')}/enqueue", params)
        reply = await self._queue_get(selector, payload=payload, token=token, timeout_s=timeout_s)
        if reply is None:
            raise IstosError(f"No queue owner answered for {prefix!r}.", code="not_found", status=504)
        return str(reply["job_id"])

    async def _queue_chord_report(
        self, prefix: str, chord_id: str, index: int, size: int,
        result_b64: str, *, token: Any = None,
    ) -> Optional[dict]:
        return await self._queue_get(
            build_selector(
                f"{prefix.rstrip('/')}/chord",
                {"chord_id": chord_id, "index": index, "size": size},
            ),
            payload=result_b64.encode("ascii"), token=token,
        )

    async def chain(
        self,
        prefixes: List[str],
        data: Any,
        *,
        priority: int = 0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
    ) -> str:
        """Run queues in sequence, piping each step's return into the next — a
        pipeline. ``chain(["jobs/fetch", "jobs/parse", "jobs/store"], url)`` runs
        fetch(url), then parse(<fetch result>), then store(<parse result>). Returns
        the first job's id; enable ``keep_results`` on the last queue to read the
        end result."""
        if not prefixes:
            raise ValueError("chain() needs at least one queue")
        _serializer = serializer or JsonSerializer()
        body = _serializer.serialize(data)
        payload = body.encode("utf-8") if isinstance(body, str) else body
        rest = [{"prefix": p} for p in prefixes[1:]]
        wf = _encode_wf({"chain": rest}) if rest else None
        return await self._queue_enqueue(prefixes[0], payload, priority=priority, wf=wf, token=token)

    async def group(
        self,
        prefix: str,
        items: List[Any],
        *,
        priority: int = 0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
    ) -> List[str]:
        """Fan a batch of jobs onto one queue in parallel and return their ids. With
        ``keep_results`` you can poll each with :meth:`result`; to run something once
        they've all finished, use :meth:`chord`."""
        _serializer = serializer or JsonSerializer()
        ids = []
        for item in items:
            body = _serializer.serialize(item)
            payload = body.encode("utf-8") if isinstance(body, str) else body
            ids.append(await self._queue_enqueue(prefix, payload, priority=priority, token=token))
        return ids

    async def chord(
        self,
        prefix: str,
        items: List[Any],
        callback: Tuple[str, Any],
        *,
        priority: int = 0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
    ) -> List[str]:
        """Run a group of jobs, then fire a callback once **all** of them succeed.
        ``chord("jobs/shard", shards, callback=("jobs/reduce", meta))`` runs every
        shard, then enqueues ``jobs/reduce`` with ``{"results": [...], "input": meta}``.
        Returns the member job ids. The group's queue owner is the barrier, so all
        members share ``prefix``; a member that dead-letters stalls the chord.
        """
        _serializer = serializer or JsonSerializer()
        chord_id = uuid.uuid4().hex
        size = len(items)
        cb_prefix, cb_data = callback
        cb_body = _serializer.serialize(cb_data)
        cb_payload = cb_body.encode("utf-8") if isinstance(cb_body, str) else cb_body
        callback_meta = {"prefix": cb_prefix, "data": base64.b64encode(cb_payload).decode("ascii")}

        ids = []
        for index, item in enumerate(items):
            body = _serializer.serialize(item)
            payload = body.encode("utf-8") if isinstance(body, str) else body
            wf = _encode_wf({"chord": {
                "id": chord_id, "index": index, "size": size, "callback": callback_meta,
            }})
            ids.append(await self._queue_enqueue(prefix, payload, priority=priority, wf=wf, token=token))
        return ids

    def schedule(
        self,
        prefix: str,
        data: Any,
        *,
        every_s: Optional[float] = None,
        cron: Optional[str] = None,
        initial_delay_s: Optional[float] = None,
        priority: int = 0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
    ) -> None:
        """Enqueue ``data`` onto a queue on a schedule — the periodic ("beat") side
        of the task system. Give it a fixed interval or a cron expression::

            app.queue("jobs/report")
            app.schedule("jobs/report", {"kind": "hourly"}, every_s=3600)
            app.schedule("jobs/report", {"kind": "daily"}, cron="0 0 * * *")

        For ``every_s`` the first run fires after ``initial_delay_s`` (defaults to
        one interval); ``cron`` fires at each matching minute. Runs on this node
        for as long as the app is serving — run a given schedule on one node to
        avoid duplicate ticks.
        """
        if (every_s is None) == (cron is None):
            raise ValueError("schedule() needs exactly one of every_s= or cron=")
        cron_sched = CronSchedule(cron) if cron is not None else None
        self._schedules.append({
            "prefix": prefix, "data": data, "every_s": every_s, "cron": cron_sched,
            "initial_delay_s": every_s if initial_delay_s is None else initial_delay_s,
            "priority": priority, "serializer": serializer, "token": token,
        })

    async def result(
        self,
        prefix: str,
        job_id: str,
        *,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
        timeout_s: float = 5.0,
    ) -> dict:
        """Look up a job's outcome (needs the queue's ``keep_results=True``).
        Returns ``{"state": ..., "result": <decoded return or None>}`` — state is
        ``done`` when finished, ``ready``/``leased`` while in flight, ``dead`` if it
        was dead-lettered, or ``unknown`` once the record has aged out."""
        _serializer = serializer or JsonSerializer()
        reply = await self._queue_get(
            build_selector(f"{prefix.rstrip('/')}/result", {"job_id": job_id}),
            token=token, timeout_s=timeout_s,
        )
        if reply is None:
            return {"state": "unknown", "result": None}
        import base64 as _b64
        result = None
        if reply.get("result") is not None:
            result = _serializer.deserialize(_b64.b64decode(reply["result"]))
        return {"state": reply.get("state", "unknown"), "result": result}

    async def dead_letters(
        self,
        prefix: str,
        *,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
        timeout_s: float = 5.0,
    ) -> List[dict]:
        """List the queue's dead-lettered jobs (decoded ``data`` plus ``job_id``,
        ``attempts`` and ``last_error``) for inspection or manual replay."""
        _serializer = serializer or JsonSerializer()
        reply = await self._queue_get(f"{prefix.rstrip('/')}/dead", token=token, timeout_s=timeout_s)
        if reply is None or "jobs" not in reply:
            return []
        import base64 as _b64
        out = []
        for job in reply["jobs"]:
            out.append({
                "job_id": job["job_id"],
                "attempts": job["attempts"],
                "last_error": job["last_error"],
                "data": _serializer.deserialize(_b64.b64decode(job["data"])),
            })
        return out

    async def _queue_get(
        self, selector: str, *, payload: Optional[bytes] = None,
        token: Optional[Union[bytes, str]] = None, timeout_s: float = 5.0,
    ) -> Optional[dict]:
        session = self._session_manager.session
        if session is None:
            raise RuntimeError("No active Zenoh session. Call run()/serving() first.")
        tok = None
        if token is not None:
            tok = token.decode("utf-8") if isinstance(token, bytes) else str(token)
        ctx = peek_request_context()
        att = RequestEnvelope(
            token=tok,
            correlation_id=ctx.correlation_id if ctx else None,
            traceparent=ctx.traceparent if ctx else None,
        ).to_attachment()

        def _do() -> Optional[bytes]:
            kwargs: dict = {"timeout": timeout_s}
            if payload is not None:
                kwargs["payload"] = payload
            if att is not None:
                kwargs["attachment"] = att
            for reply in session.get(selector, **kwargs):
                if reply.ok is not None:
                    return bytes(reply.ok.payload)
            return None

        raw = await asyncio.to_thread(_do)
        if raw is None:
            return None
        reply: dict = json.loads(raw)
        # Every queue call comes through here, so the envelope is checked once.
        if is_error_payload(reply):
            raise error_from_payload(reply)
        return reply

    async def _queue_claim(self, prefix: str, *, token: Any = None) -> Optional[dict]:
        return await self._queue_get(f"{prefix}/claim", token=token)

    async def _queue_ack(
        self, prefix: str, job_id: str, *, result: Optional[bytes] = None, token: Any = None,
    ) -> None:
        await self._queue_get(
            build_selector(f"{prefix}/ack", {"job_id": job_id}), payload=result, token=token,
        )

    async def _queue_nack(
        self, prefix: str, job_id: str, *, error: str = "", attempt: int = 1, token: Any = None,
    ) -> None:
        await self._queue_get(
            build_selector(f"{prefix}/nack", {"job_id": job_id, "attempt": attempt}),
            payload=error.encode("utf-8"), token=token,
        )

    async def _bind_queues(self, session: zenoh.Session) -> None:
        """Bind queue owners (enqueue/claim/ack/nack queryables + lease sweeper),
        then start any workers. Owners come up first so a co-located worker has
        something to claim from."""
        loop = asyncio.get_running_loop()
        for role in self._queue_roles:
            await role.bind(session, loop)
        for wrapper in self._workers:
            wrapper.start(self)
        for spec in self._schedules:
            self._schedule_tasks.append(asyncio.ensure_future(self._run_schedule(spec)))

    async def _run_schedule(self, spec: dict) -> None:
        import datetime as _datetime

        async def _fire() -> None:
            try:
                await self.enqueue(
                    spec["prefix"], spec["data"],
                    priority=spec["priority"], serializer=spec["serializer"],
                    token=spec["token"],
                )
            except Exception:
                self._logger.exception(
                    "Scheduled enqueue for %s failed", spec["prefix"],
                    extra={"prefix": spec["prefix"]},
                )

        try:
            cron = spec["cron"]
            if cron is not None:
                while True:
                    now = _datetime.datetime.now()
                    wait = (cron.next_after(now) - now).total_seconds()
                    await asyncio.sleep(max(0.0, wait))
                    await _fire()
            else:
                await asyncio.sleep(spec["initial_delay_s"])
                while True:
                    await _fire()
                    await asyncio.sleep(spec["every_s"])
        except asyncio.CancelledError:
            pass

    async def _unbind_queues(self) -> None:
        for task in self._schedule_tasks:
            task.cancel()
        for task in self._schedule_tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._schedule_tasks.clear()
        for wrapper in self._workers:
            await wrapper.stop()
        for role in self._queue_roles:
            await role.aclose()

