"""Streaming RPC via multi-reply queryables.

``@handle`` answers once. ``@stream`` is an async generator: each ``yield`` is
one reply chunk on a single Zenoh query.

    @app.stream("llm/generate")
    async def generate(prompt: str):
        async for token in model.stream(prompt):
            yield token

    async for token in app.stream_query("llm/generate", prompt="hi"):
        print(token, end="")

Same pipeline as ``@handle`` (auth, validation, DI, envelope). Dep scope stays
open for the whole stream.
"""

import asyncio
import inspect
from contextlib import AsyncExitStack, suppress
from typing import Any, Callable, Iterable, Optional, Tuple, cast

import zenoh

from istos.context import RequestEnvelope, get_request_context
from istos.security.authz import AuthContext, Authorizer, check_authorized
from istos.errors import (
    ExceptionHandlerRegistry,
    UnauthorizedError,
    get_default_registry,
)
from istos.validation import SchemaValidationError, validate_params
from istos.di.depends import extract_depends, resolve_dependencies
from istos.http.gateway import decode_params
from istos.logging import get_logger
from istos.messages.serialization import Serialize
from istos.middleware.base import MiddlewareStack, RequestScope


class stream_wrapper:
    """Queryable backed by an async generator (one reply per yield)."""

    def __init__(
        self,
        func: Callable,
        prefix: str,
        serializer: Serialize,
        *,
        authorizer: Optional[Authorizer] = None,
        exception_registry: Optional[ExceptionHandlerRegistry] = None,
        dependency_overrides: Optional[dict] = None,
        middleware: Optional[MiddlewareStack] = None,
    ) -> None:
        if not inspect.isasyncgenfunction(func):
            raise TypeError(
                f"@stream requires an async generator function (use 'yield'); "
                f"{getattr(func, '__name__', func)!r} is not one."
            )
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self._authorizer = authorizer
        self._middleware = middleware
        self._exception_registry = exception_registry or get_default_registry()
        self._logger = get_logger("stream")

        _params = inspect.signature(func).parameters
        self._depends_params = {
            n for n, p in _params.items() if extract_depends(p) is not None
        }
        self._has_depends = bool(self._depends_params)
        self._injected_params = set(self._depends_params)
        self._dependency_overrides = (
            dependency_overrides if dependency_overrides is not None else {}
        )

    @staticmethod
    def _extract_attachment(query: zenoh.Query) -> Optional[bytes]:
        raw = getattr(query, "attachment", None)
        if raw is None:
            return None
        try:
            return bytes(raw)
        except (TypeError, ValueError):
            return None

    def _reply_error(self, query: zenoh.Query, key: str, exc: Exception) -> None:
        error = self._exception_registry.resolve(exc)
        error.correlation_id = get_request_context().correlation_id
        try:
            query.reply(key, self.serializer.serialize(error.to_dict()))
        except Exception:  # pragma: no cover - reply channel gone
            pass

    async def on_query(self, query: zenoh.Query) -> None:
        key = str(query.selector.key_expr)
        try:
            params: dict = {}
            if hasattr(query.selector, "parameters") and query.selector.parameters:
                params = decode_params(
                    dict(cast(Iterable[Tuple[str, str]], query.selector.parameters))
                )

            attachment = self._extract_attachment(query)
            try:
                principal = await check_authorized(
                    self._authorizer,
                    AuthContext(
                        prefix=self.prefix, key_expr=key, params=params,
                        attachment=attachment, operation="stream",
                    ),
                )
            except UnauthorizedError as e:
                self._reply_error(query, key, e)
                return

            req_ctx = get_request_context()
            req_ctx.prefix = self.prefix
            req_ctx.operation = "stream"
            req_ctx.principal = principal
            req_ctx.attachment = attachment
            env = RequestEnvelope.from_attachment(attachment)
            if env.correlation_id:
                req_ctx.correlation_id = env.correlation_id
            req_ctx.traceparent = env.traceparent

            try:
                validated = validate_params(
                    self.func, params, skip_params=self._injected_params
                )
                validated.pop("db", None)
            except SchemaValidationError as e:
                self._reply_error(query, key, e)
                return

            async with AsyncExitStack() as di_stack:
                call_kwargs = dict(validated)
                if self._has_depends:
                    call_kwargs = await resolve_dependencies(
                        self.func, call_kwargs, di_stack, cache={},
                        overrides=self._dependency_overrides,
                    )

                async def _drive(_scope: Any = None) -> None:
                    agen = self.func(**call_kwargs)
                    try:
                        async for chunk in agen:
                            payload = self.serializer.serialize(chunk)
                            # Zenoh reply is sync; offload so the loop keeps pumping.
                            await asyncio.to_thread(query.reply, key, payload)
                    finally:
                        if hasattr(agen, "aclose"):
                            await agen.aclose()

                # Middleware wraps the whole stream — it runs once at open and
                # once when the last chunk has gone out, not per chunk.
                if self._middleware is not None:
                    scope = RequestScope(
                        prefix=self.prefix, operation="stream", params=params,
                    )
                    scope.context.principal = req_ctx.principal
                    scope.context.attachment = req_ctx.attachment
                    scope.context.correlation_id = req_ctx.correlation_id
                    scope.context.traceparent = req_ctx.traceparent
                    await self._middleware.invoke(scope, _drive)
                else:
                    await _drive()
        except Exception as e:
            self._logger.error(
                "Stream error on %s: %s", self.prefix, e,
                exc_info=True, extra={"prefix": self.prefix},
            )
            self._reply_error(query, key, e)
        finally:
            # End the query the moment the generator is done.
            #
            # Zenoh finishes a query when its Query is dropped, and the consumer's
            # get() only returns once every matching queryable has finished. Left
            # to refcounting that drop is not prompt: this coroutine's frame, the
            # _drive closure over `query`, and the traceback on the error path all
            # keep it alive until a cycle-GC pass. Meanwhile the consumer sits
            # there — and since it cannot tell "still thinking" from "finished",
            # it waits out the full timeout. Every SSE client would hang for
            # `http_timeout_s` after its last chunk.
            with suppress(Exception):
                query.drop()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """In-process invocation returns the underlying async generator (used by
        the TestClient); network delivery goes through :meth:`on_query`."""
        return self.func(*args, **kwargs)
