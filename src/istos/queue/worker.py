"""The competing-consumer worker loop that claims from a queue owner."""

import asyncio
import base64
import inspect
from typing import Any, Callable, List, Optional

from istos.logging import get_logger
from istos.di.depends import (
    has_dependencies,
    invoke_with_dependencies,
    positional_param_names,
)
from istos.messages.serialization import Serialize
from istos.queue.store import JobContext, _decode_wf, _encode_wf

_logger = get_logger("queue")


def _wants_ctx(func: Callable) -> bool:
    """True if the worker asks for the delivery context by naming a ``ctx`` param.

    Keyed on the name, like a handler's ``db``. A ``Depends(...)`` on the same
    name still wins — the dependency resolver checks for one first — so this
    cannot hijack a parameter that already means something else.
    """
    try:
        params = inspect.signature(func).parameters
    except (TypeError, ValueError):  # pragma: no cover - builtins, C callables
        return False
    return "ctx" in params


class worker_wrapper:
    """A competing consumer. Runs ``concurrency`` claim→run→ack loops against a
    queue owner elsewhere on the mesh, waking on the owner's nudge instead of busy
    polling. The handler returning acks the job (and hands back its result); the
    handler raising nacks it (redeliver with backoff, then dead-letter)."""

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
        self._wants_ctx = _wants_ctx(func)
        self._overrides = dependency_overrides or {}
        self._app: Any = None
        self._tasks: List[asyncio.Task] = []
        self._subscriber: Optional[Any] = None
        self._wake: Optional[asyncio.Event] = None
        self._running = False

    def start(self, app: Any) -> None:
        self._app = app
        self._running = True
        self._wake = asyncio.Event()
        loop = asyncio.get_running_loop()
        session = app._session_manager.session
        if session is not None:
            def _on_notify(_sample: Any) -> None:
                if not loop.is_closed() and self._wake is not None:
                    loop.call_soon_threadsafe(self._wake.set)
            try:
                self._subscriber = session.declare_subscriber(f"{self.prefix}/notify", _on_notify)
            except Exception:  # pragma: no cover - fall back to polling only
                self._subscriber = None
        for _ in range(self.concurrency):
            self._tasks.append(asyncio.ensure_future(self._loop()))

    async def stop(self) -> None:
        self._running = False
        if self._subscriber is not None:
            try:
                self._subscriber.undeclare()
            except Exception:  # pragma: no cover
                pass
            self._subscriber = None
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()

    async def _run_body(self, job: Any, ctx: Optional[JobContext] = None) -> Any:
        extra = {"ctx": ctx} if self._wants_ctx else {}
        if self._has_depends:
            return await invoke_with_dependencies(
                self.func, args=(job,), context=extra, skip_names=self._skip_names,
                overrides=self._overrides,
            )
        if inspect.iscoroutinefunction(self.func):
            return await self.func(job, **extra)
        return await asyncio.to_thread(lambda: self.func(job, **extra))

    async def _idle(self) -> None:
        """Wait for a nudge, or fall through after poll_interval to re-check for
        redelivered / newly-eligible jobs the nudge might not cover."""
        wake = self._wake
        if wake is None:
            await asyncio.sleep(self.poll_interval_s)
            return
        try:
            await asyncio.wait_for(wake.wait(), timeout=self.poll_interval_s)
        except asyncio.TimeoutError:
            pass
        wake.clear()

    async def _loop(self) -> None:
        while self._running:
            try:
                reply = await self._app._queue_claim(self.prefix, token=self._token)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Without this a rejected token looks the same as an idle queue.
                _logger.warning(
                    "Worker for %s could not claim: %s", self.prefix, exc,
                    extra={"prefix": self.prefix},
                )
                await asyncio.sleep(self.poll_interval_s)
                continue

            if not reply or reply.get("empty"):
                await self._idle()
                continue

            job_id = reply["job_id"]
            attempt = reply.get("attempt", 1)
            wf = reply.get("wf")
            ctx = JobContext(
                job_id=job_id,
                queue=self.prefix,
                attempt=attempt,
                max_attempts=reply.get("max_attempts", 0),
                last_error=reply.get("last_error"),
            )
            try:
                job = self.serializer.deserialize(base64.b64decode(reply["data"]))
                result = await self._run_body(job, ctx)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _logger.exception(
                    "Worker for %s failed job %s (attempt %s)",
                    self.prefix, job_id, attempt, extra={"prefix": self.prefix},
                )
                try:
                    await self._app._queue_nack(
                        self.prefix, job_id, error=str(exc), attempt=attempt, token=self._token,
                    )
                except Exception:
                    pass
                continue

            try:
                payload = self.serializer.serialize(result)
                result_bytes = payload.encode("utf-8") if isinstance(payload, str) else payload
                # Advance any workflow (chain step / chord report) before acking, so
                # a crash redelivers the whole step rather than losing the follow-on.
                await self._advance_workflow(wf, result_bytes)
                await self._app._queue_ack(
                    self.prefix, job_id, result=result_bytes, token=self._token,
                )
            except Exception:
                # The lease will expire and the job redeliver — at-least-once.
                _logger.exception(
                    "Worker for %s could not finish job %s", self.prefix, job_id,
                    extra={"prefix": self.prefix},
                )

    async def _advance_workflow(self, wf_b64: Optional[str], result_bytes: bytes) -> None:
        if not wf_b64:
            return
        wf = _decode_wf(wf_b64)
        chain = wf.get("chain")
        if chain:
            nxt, rest = chain[0], chain[1:]
            next_wf = _encode_wf({"chain": rest}) if rest else None
            await self._app._queue_enqueue(
                nxt["prefix"], result_bytes, wf=next_wf, token=self._token,
            )
        chord = wf.get("chord")
        if chord:
            result_b64 = base64.b64encode(result_bytes).decode("ascii")
            reply = await self._app._queue_chord_report(
                self.prefix, chord["id"], chord["index"], chord["size"],
                result_b64, token=self._token,
            )
            if reply and reply.get("complete"):
                member_results = [
                    self.serializer.deserialize(base64.b64decode(r)) if r is not None else None
                    for r in (reply.get("results") or [])
                ]
                cb = chord["callback"]
                cb_input = self.serializer.deserialize(base64.b64decode(cb["data"]))
                body = self.serializer.serialize({"results": member_results, "input": cb_input})
                cb_bytes = body.encode("utf-8") if isinstance(body, str) else body
                await self._app._queue_enqueue(cb["prefix"], cb_bytes, token=self._token)
