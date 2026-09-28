"""Startup, shutdown and process entry points (serving/run/run_async) that orchestrate the domain binds."""

import asyncio
import contextlib
import signal
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, cast

from istos.logging import ensure_configured

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from istos.app import Istos

from istos.app._base import IstosBase


class _LifecycleMixin(IstosBase):
    """Startup, shutdown and process entry points (serving/run/run_async) that orchestrate the domain binds."""

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        # A new event every run. A previous SIGINT leaves the old event set,
        # and wait() on it would return before this run did any work.
        self._shutdown_event = asyncio.Event()
        self._installed_signals: list = []

        def _request_shutdown() -> None:
            self._logger.info("Shutdown signal received")
            if self._shutdown_event is not None:
                self._shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except (NotImplementedError, RuntimeError):
                continue
            self._installed_signals.append(sig)

    def _remove_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in getattr(self, "_installed_signals", ()):
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass
        self._installed_signals = []

    async def _startup(self, *, serve_http: bool) -> Any:
        """Open the session and bind every registry. Shared by run_async and the
        serving() context manager. No signal handlers, no blocking."""
        if self._configure_logging is not False:
            ensure_configured(self._log_level, self._json_logs)
        self._register_builtin_handlers()

        stack = AsyncExitStack()
        self._lifecycle_stack = stack
        try:
            if self.lifespan:
                await stack.enter_async_context(self.lifespan(cast("Istos", self)))

            # close() is optional (Redis/SQLAlchemy); InMemory has none.
            storage_close = getattr(self._storage, "close", None)
            if callable(storage_close):
                stack.push_async_callback(storage_close)
            stack.push_async_callback(self._databases.dispose_all)

            # session="sync" managers use __enter__, not __aenter__.
            if hasattr(self._session_manager, "__aenter__"):
                session = await stack.enter_async_context(self._session_manager)  # type: ignore
            else:
                session = stack.enter_context(self._session_manager)  # type: ignore

            await self._bind_handlers(session)
            await self._bind_streams(session)
            await self._bind_channels(session)
            await self._bind_persist(session)
            await self._bind_queues(session)
            await self._bind_publishers(session)
            await self._bind_subscribers(session)
            await self._bind_liveliness(session)
        except BaseException:
            # serving() only shuts down after _startup returns. A bind that
            # raises would otherwise leave the session, storage, and pools open.
            await self._shutdown()
            raise

        self._health.ready = True
        prefixes = [a.prefix for a in self._handlers]
        subs = [s.prefix for s in self._subscribers]
        self._logger.info(
            "Service started with %d handler(s) and %d subscriber(s)",
            len(prefixes), len(subs),
            extra={"handlers": prefixes, "subscribers": subs},
        )

        self._web_runner = None
        if serve_http and self._http_server_port() is not None:
            self._web_runner = await self._start_http_server()
        return session

    async def _shutdown(self) -> None:
        """Reverse of _startup: unbind, then close session/storage/databases.

        Each step is isolated. One undeclare that raises must not skip the
        session and pool close that follow it.
        """
        self._health.ready = False
        self._logger.info("Service stopping")

        async def _stop_http() -> None:
            if self._web_runner is not None:
                await self._web_runner.cleanup()
                self._web_runner = None

        async def _close_stack() -> None:
            if self._lifecycle_stack is not None:
                stack = self._lifecycle_stack
                self._lifecycle_stack = None
                await stack.aclose()

        for name, step in (
            ("http", _stop_http),
            ("replay", self._cancel_replay_tasks),
            ("liveliness", self._unbind_liveliness),
            ("subscribers", self._unbind_subscribers),
            ("publishers", self._unbind_publishers),
            ("queues", self._unbind_queues),
            ("persist", self._unbind_persist),
            ("channels", self._unbind_channels),
            ("handlers", self._unbind_handlers),
            ("session", _close_stack),
        ):
            try:
                await step()
            except Exception:
                self._logger.exception("shutdown step %s failed", name)
        self._logger.info("Service stopped")

    @contextlib.asynccontextmanager
    async def serving(self, *, serve_http: bool = False) -> "AsyncIterator[Istos]":
        """Run the mesh for as long as the block is entered, without owning the
        process (no signal handlers, no blocking loop). This is the co-host hook:
        an ASGI host or a test drives the lifecycle.

            async with app.serving():
                reply = await app.query_once("robot/move", distance=5)

        Pass ``serve_http=True`` to also start the embedded aiohttp surface;
        under FastAPI/Starlette leave it off and let the ASGI host serve HTTP."""
        await self._startup(serve_http=serve_http)
        try:
            yield cast("Istos", self)
        finally:
            await self._shutdown()

    async def run_async(self) -> None:
        """
        Async entry-point.
        Opens a Zenoh session, binds registries, and keeps the loop alive.
        """
        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)
        try:
            async with self.serving(serve_http=True):
                try:
                    if self._shutdown_event is not None:
                        await self._shutdown_event.wait()
                    else:
                        while True:
                            await asyncio.sleep(1)
                except asyncio.CancelledError:
                    pass
        finally:
            self._remove_signal_handlers(loop)

    def run(self) -> None:
        """
        Sync entry-point.
        Detects whether an event loop is already running and adapts.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.run_async())
            return
        # The loop only keeps a weak reference to the task. Hold it on the app
        # so a running service is not collected, and so a startup failure stays
        # attached to an object the caller still has.
        self._background_run = loop.create_task(self.run_async())
