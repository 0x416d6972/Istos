"""Client-side decorators for reaching @stream and @channel — the declarative
counterparts to @query (which reaches @handle) and @subscribe (which reaches
@publish). A service is a mix of senders and receivers; these let the receiving
side be attached the same way, on the app or a router.

@stream_client hands the body the live chunk iterator; @channel_client hands it
an open ChannelClient and closes it when the body returns. Call kwargs become the
selector params, and a per-call ``token=`` (or a decorator-level ``token=``)
carries the auth token — just like @query.
"""

import asyncio
import functools
import inspect
from typing import Any, Optional, Union

from istos.di.depends import has_dependencies, invoke_with_dependencies, positional_param_names
from istos.messages.serialization import Serialize


class _bound_client:
    """Bound-method proxy that injects `self` for class-based services."""

    def __init__(self, desc: Any, subj: Any) -> None:
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class _client_base:
    def __init__(
        self,
        func: Any,
        app: Any,
        prefix: str,
        *,
        serializer: Optional[Serialize],
        timeout_s: float,
        token: Optional[Union[bytes, str]] = None,
        dependency_overrides: Optional[dict] = None,
    ) -> None:
        self.func = func
        self._app = app
        self.prefix = prefix
        self.serializer = serializer
        self.timeout_s = timeout_s
        self._token = token
        self._has_depends = has_dependencies(func)
        # The live object (iterator / session) fills the first positional slot;
        # Depends fill the rest.
        self._skip_names = tuple(positional_param_names(func)[:1])
        self._overrides = dependency_overrides or {}

    async def _drive(self, args: tuple, live: Any) -> Any:
        """Run the body with the live object as its first positional argument."""
        if self._has_depends:
            return await invoke_with_dependencies(
                self.func, args=(*args, live), skip_names=self._skip_names,
                overrides=self._overrides,
            )
        if inspect.iscoroutinefunction(self.func):
            return await self.func(*args, live)
        return await asyncio.to_thread(functools.partial(self.func, *args, live))

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return _bound_client(self, instance)


class stream_client_wrapper(_client_base):
    """Reaches a @stream: opens the stream and passes its chunk iterator to the
    body. ``async for chunk in it`` inside the body."""

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        token = kwargs.pop("token", None)
        stream = self._app.stream_query(
            self.prefix, timeout_s=self.timeout_s, serializer=self.serializer,
            token=token if token is not None else self._token, **kwargs,
        )
        return await self._drive(args, stream)


class channel_client_wrapper(_client_base):
    """Reaches a @channel: opens a session, passes the ChannelClient to the body,
    and closes it when the body returns."""

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        token = kwargs.pop("token", None)
        chan = await self._app.open_channel(
            self.prefix, token=token if token is not None else self._token,
            timeout_s=self.timeout_s, serializer=self.serializer, **kwargs,
        )
        try:
            return await self._drive(args, chan)
        finally:
            await chan.close()
