"""In-process test client — test handlers without a Zenoh network."""

from __future__ import annotations

import asyncio
from typing import Any, List

from istos.app import Istos
from istos.core.validation import validate_params


class IstosTestClient:
    """
    In-process test client for Istos.

    Calls handlers and subscribers in-process without Zenoh networking.

        istos = Istos()

        @istos.handle("robot/move")
        async def move(distance: int):
            return {"moved": distance}

        client = IstosTestClient(istos)
        result = await client.query("robot/move", distance=10)
        assert result == {"moved": 10}
    """

    def __init__(self, app: Istos) -> None:
        self.app = app

    def _find_handler(self, prefix: str) -> Any:
        for handler in self.app._handlers:
            if handler.prefix == prefix:
                return handler
        raise KeyError(f"No handler registered for prefix: {prefix!r}")

    def _find_subscribers(self, prefix: str) -> List[Any]:
        return [s for s in self.app._subscribers if s.prefix == prefix]

    async def query(self, prefix: str, **kwargs: Any) -> Any:
        """Invoke a handler directly, bypassing Zenoh."""
        handler = self._find_handler(prefix)
        # Exclude framework-injected params (db, Depends(...)) from validation;
        # the handler resolves those itself on invocation.
        skip = getattr(handler, "_injected_params", None)
        validated = validate_params(handler.func, kwargs, skip_params=skip)
        validated.pop("db", None)
        validated.pop("session", None)
        return await handler(**validated)

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
