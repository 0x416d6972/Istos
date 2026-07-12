"""In-process test client for handlers, streams and subscribers — no network."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, List, Optional

from istos.app import Istos
from istos.context import RequestContext, RequestEnvelope, set_request_context
from istos.core.authz import AuthContext, check_authorized
from istos.core.validation import validate_params
from istos.di.depends import resolve_dependencies


class IstosTestClient:
    """
    Invoke handlers, streams and subscribers in-process. The authorizer gate,
    validation, DI and durability ledger all still run.

        istos = Istos()

        @istos.handle("robot/move")
        async def move(distance: int):
            return {"moved": distance}

        client = IstosTestClient(istos)
        assert await client.query("robot/move", distance=10) == {"moved": 10}

    Pass ``token=`` to drive an authorizer; a denied request raises
    ``UnauthorizedError``.
    """

    def __init__(self, app: Istos) -> None:
        self.app = app

    def _find_handler(self, prefix: str) -> Any:
        for handler in self.app._handlers:
            if handler.prefix == prefix:
                return handler
        raise KeyError(f"No handler registered for prefix: {prefix!r}")

    def _find_stream(self, prefix: str) -> Any:
        for wrapper in self.app._streams:
            if wrapper.prefix == prefix:
                return wrapper
        raise KeyError(f"No stream registered for prefix: {prefix!r}")

    def _find_subscribers(self, prefix: str) -> List[Any]:
        return [s for s in self.app._subscribers if s.prefix == prefix]

    async def _gate(self, wrapper: Any, prefix: str, params: dict, token: Optional[str]) -> None:
        """Run the authorizer and set a request context so the body can inject
        the resolved principal/token. A denial raises ``UnauthorizedError``."""
        attachment = RequestEnvelope(token=token).to_attachment() if token is not None else None
        principal = await check_authorized(
            getattr(wrapper, "_authorizer", None),
            AuthContext(prefix=prefix, key_expr=prefix, params=params, attachment=attachment),
        )
        set_request_context(RequestContext(
            prefix=prefix, principal=principal, attachment=attachment,
        ))

    async def query(self, prefix: str, token: Optional[str] = None, **kwargs: Any) -> Any:
        """Invoke a handler in-process."""
        handler = self._find_handler(prefix)
        # db / Depends(...) are injected by the handler, not validated here.
        skip = getattr(handler, "_injected_params", None)
        validated = validate_params(handler.func, kwargs, skip_params=skip)
        validated.pop("db", None)
        validated.pop("session", None)
        await self._gate(handler, prefix, validated, token)
        return await handler(**validated)

    async def stream(
        self, prefix: str, token: Optional[str] = None, **kwargs: Any
    ) -> AsyncIterator[Any]:
        """Consume a ``@stream`` handler in-process, yielding each chunk.

            async for chunk in client.stream("llm/generate", prompt="hi"):
                ...
        """
        wrapper = self._find_stream(prefix)
        skip = getattr(wrapper, "_injected_params", None)
        validated = validate_params(wrapper.func, kwargs, skip_params=skip)
        validated.pop("db", None)
        await self._gate(wrapper, prefix, validated, token)

        async with AsyncExitStack() as di_stack:
            call_kwargs = dict(validated)
            if getattr(wrapper, "_has_depends", False):
                call_kwargs = await resolve_dependencies(
                    wrapper.func, call_kwargs, di_stack, cache={},
                    overrides=getattr(wrapper, "_dependency_overrides", {}),
                )
            agen = wrapper.func(**call_kwargs)
            try:
                async for chunk in agen:
                    yield chunk
            finally:
                if hasattr(agen, "aclose"):
                    await agen.aclose()

    async def publish(self, prefix: str, data: Any) -> None:
        """Deliver data to all matching subscribers in-process."""
        subscribers = self._find_subscribers(prefix)
        if not subscribers:
            raise KeyError(f"No subscribers registered for prefix: {prefix!r}")
        for sub in subscribers:
            await sub(data)

    def run_query(self, prefix: str, **kwargs: Any) -> Any:
        """Synchronous wrapper around query()."""
        return asyncio.run(self.query(prefix, **kwargs))

    def run_publish(self, prefix: str, data: Any) -> None:
        """Synchronous wrapper around publish()."""
        return asyncio.run(self.publish(prefix, data))
