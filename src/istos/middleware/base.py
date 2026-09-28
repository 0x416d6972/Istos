"""Middleware pipeline for cross-cutting concerns."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, List, Optional, Protocol, runtime_checkable

from istos.context import (
    RequestContext,
    get_request_context,
    pop_request_context,
    push_request_context,
)


@dataclass
class RequestScope:
    """Metadata passed through the middleware chain."""

    prefix: str
    operation: str  # "handle", "subscribe", "publish", "query"
    params: dict[str, Any] = field(default_factory=dict)
    context: RequestContext = field(default_factory=RequestContext)


HandlerCallable = Callable[[RequestScope], Awaitable[Any]]


@runtime_checkable
class Middleware(Protocol):
    """Middleware processes requests before/after handler execution."""

    async def __call__(
        self,
        scope: RequestScope,
        call_next: HandlerCallable,
    ) -> Any:
        ...


class MiddlewareStack:
    """Composes middleware into a single callable."""

    def __init__(self, middlewares: Optional[List[Middleware]] = None) -> None:
        self._middlewares = list(middlewares or [])

    def add(self, middleware: Middleware) -> None:
        self._middlewares.append(middleware)

    async def invoke(
        self,
        scope: RequestScope,
        handler: HandlerCallable,
    ) -> Any:
        scope.context.prefix = scope.prefix
        scope.context.operation = scope.operation
        # The token restores whatever this task was doing before the request,
        # including "nothing". A reused task must not keep this request's
        # principal, token, or correlation id.
        token = push_request_context(scope.context)

        async def dispatch(index: int, current: RequestScope) -> Any:
            current.context.prefix = current.prefix
            current.context.operation = current.operation
            if index >= len(self._middlewares):
                return await handler(current)
            middleware = self._middlewares[index]

            async def call_next(next_scope: RequestScope) -> Any:
                # A middleware may hand a different scope downstream. Install
                # that scope's context for the rest of the chain, then put the
                # previous one back so the caller still sees its own.
                inner = push_request_context(next_scope.context)
                try:
                    return await dispatch(index + 1, next_scope)
                finally:
                    pop_request_context(inner)

            return await middleware(current, call_next)

        try:
            return await dispatch(0, scope)
        finally:
            pop_request_context(token)


class LoggingMiddleware:
    """Logs request start/end with duration and correlation ID."""

    def __init__(self, logger: Any) -> None:
        self._logger = logger

    async def __call__(
        self,
        scope: RequestScope,
        call_next: HandlerCallable,
    ) -> Any:
        ctx = get_request_context()
        start = time.perf_counter()
        self._logger.info(
            "%s %s started",
            scope.operation, scope.prefix,
            extra={
                "correlation_id": ctx.correlation_id,
                "prefix": scope.prefix,
                "operation": scope.operation,
            },
        )
        try:
            result = await call_next(scope)
            duration_ms = (time.perf_counter() - start) * 1000
            self._logger.info(
                "%s %s completed in %.2fms",
                scope.operation, scope.prefix, duration_ms,
                extra={
                    "correlation_id": ctx.correlation_id,
                    "prefix": scope.prefix,
                    "operation": scope.operation,
                    "duration_ms": round(duration_ms, 2),
                },
            )
            return result
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            self._logger.error(
                "%s %s failed after %.2fms: %s",
                scope.operation, scope.prefix, duration_ms, exc,
                extra={
                    "correlation_id": ctx.correlation_id,
                    "prefix": scope.prefix,
                    "operation": scope.operation,
                    "duration_ms": round(duration_ms, 2),
                    "error": str(exc),
                },
            )
            raise


class CorrelationIdMiddleware:
    """Ensures every request has a correlation ID."""

    async def __call__(
        self,
        scope: RequestScope,
        call_next: HandlerCallable,
    ) -> Any:
        if not scope.context.correlation_id:
            # Fill the id on the existing context. Replacing the whole context
            # would drop principal and attachment that an outer layer already set.
            scope.context.correlation_id = str(uuid.uuid4())
        return await call_next(scope)
