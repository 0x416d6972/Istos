import asyncio
import inspect
import zenoh
from typing import Any, Callable, Optional, Union

from istos.messages.serialization import Serialize
from istos.core.retry import RetryPolicy, execute_with_retry
from istos.core.validation import build_payload_validator
from istos.core.authz import Authorizer, AuthContext, check_authorized
from istos.core.errors import UnauthorizedError
from istos.di.depends import has_dependencies, invoke_with_dependencies, positional_param_names
from istos.middleware.base import MiddlewareStack, RequestScope
from istos.context import get_request_context
from istos.logging import get_logger

class bound_subscribe_wrapper:
    """Bound-method proxy that injects `self` (the instance) into calls."""
    def __init__(self, desc: "subscribe_wrapper", subj: Any):
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class subscribe_wrapper:
    """
    Descriptor that wraps a function to become a subscriber callback.
    It takes the payload from Zenoh, deserializes it, and passes it to the function.
    The callback may declare Depends(...) dependencies, resolved per message.
    """
    def __init__(
        self,
        func: Callable,
        prefix: str,
        serializer: Serialize,
        retry: Optional[Union[int, RetryPolicy]] = None,
        dependency_overrides: Optional[dict] = None,
        durable: bool = False,
        replay: int = 1000,
        recover: bool = True,
        on_miss: Optional[Callable[[str, int], Any]] = None,
        middleware: Optional[MiddlewareStack] = None,
        authorizer: Optional[Authorizer] = None,
    ):
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self.calls = 0
        # Cross-cutting parity with @handle: an inbound sample is untrusted
        # network input, so it passes the authorizer gate and the middleware
        # chain before the callback body runs.
        self._middleware = middleware
        self._authorizer = authorizer
        # Brokerless durability: subscribe via an AdvancedSubscriber that replays
        # history on join and recovers missed samples (see Istos._bind_subscribers).
        self.durable = durable
        self.replay = replay
        self.recover = recover
        # Fired when a gap could NOT be recovered — the honest at-least-once signal.
        self.on_miss = on_miss

        # Normalize retry parameter
        if retry is None:
            self.retry_policy = RetryPolicy(max_retries=0)
        elif isinstance(retry, int):
            self.retry_policy = RetryPolicy.from_int(retry)
        else:
            self.retry_policy = retry
        self._logger = get_logger("subscribe")

        # Dependency injection: the payload fills the first positional slot.
        self._has_depends = has_dependencies(func)
        _positional = positional_param_names(func)
        self._skip_names = tuple(_positional[:1])
        self._dependency_overrides = dependency_overrides if dependency_overrides is not None else {}

        # Boundary validation: the incoming message is untrusted network input, so
        # coerce/validate it against the payload param's type hint — mirroring how
        # @handle validates its params. None when the payload param is untyped.
        self._validate_payload = build_payload_validator(
            func, _positional[0] if _positional else None
        )

    async def _dispatch(self, data: Any, instance: Optional[Any] = None) -> Any:
        args = (instance, data) if instance is not None else (data,)
        if self._has_depends:
            return await invoke_with_dependencies(
                self.func, args=args, skip_names=self._skip_names,
                overrides=self._dependency_overrides,
            )
        if inspect.iscoroutinefunction(self.func):
            return await self.func(*args)
        return await asyncio.to_thread(self.func, *args)

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # Direct/in-process path (e.g. TestClient): first positional is the payload.
        instance = args[0] if len(args) > 1 else None
        data = args[-1] if args else kwargs.get("data")
        if self._validate_payload is not None:
            data = self._validate_payload(data)
        return await self._dispatch(data, instance)

    async def handle_miss(self, source: str, nb: int) -> None:
        """Report an unrecoverable gap: always logged, then forwarded to ``on_miss``.

        Runs on the event loop (bridged from Zenoh's miss listener thread), so an
        async ``on_miss`` callback is awaited.
        """
        self._logger.warning(
            "Durable subscriber on %s missed %d sample(s) from %s",
            self.prefix, nb, source,
            extra={"prefix": self.prefix, "missed": nb, "source": source},
        )
        if self.on_miss is not None:
            try:
                result = self.on_miss(source, nb)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                self._logger.error(
                    "on_miss callback failed on %s: %s", self.prefix, e,
                    exc_info=True, extra={"prefix": self.prefix},
                )

    @staticmethod
    def _extract_attachment(sample: zenoh.Sample) -> Optional[bytes]:
        """Best-effort read of a sample's attachment as raw bytes (for auth tokens)."""
        raw = getattr(sample, "attachment", None)
        if raw is None:
            return None
        try:
            return bytes(raw)
        except (TypeError, ValueError):
            return None

    async def on_sample(self, sample: zenoh.Sample, instance: Optional[Any] = None) -> None:
        """Called by Zenoh when a new sample arrives."""
        self.calls += 1
        try:
            key = str(getattr(sample, "key_expr", self.prefix))
            attachment = self._extract_attachment(sample)

            # --- Authorization: enforced at the network boundary, mirroring
            # @handle. A denied sample is logged and dropped — pub/sub has no
            # reply channel. In-process delivery (TestClient) goes through
            # __call__ and is not subject to this gate. ---
            try:
                principal = await check_authorized(
                    self._authorizer,
                    AuthContext(
                        prefix=self.prefix,
                        key_expr=key,
                        attachment=attachment,
                        operation="subscribe",
                    ),
                )
            except UnauthorizedError as e:
                self._logger.warning(
                    "Unauthorized sample on %s dropped: %s", self.prefix, e,
                    extra={"prefix": self.prefix, "error": str(e)},
                )
                return

            raw_payload = bytes(sample.payload)
            data = self.serializer.deserialize(raw_payload)
            # Validate once, before the retry loop — a schema failure won't pass on
            # retry, and an invalid event is logged and dropped (can't reply to pub/sub).
            if self._validate_payload is not None:
                data = self._validate_payload(data)

            # Expose the resolved identity to the callback body (Depends(...)).
            req_ctx = get_request_context()
            req_ctx.prefix = self.prefix
            req_ctx.operation = "subscribe"
            req_ctx.principal = principal
            req_ctx.attachment = attachment

            async def _process():
                if self._middleware is not None:
                    scope = RequestScope(prefix=self.prefix, operation="subscribe")
                    scope.context.principal = principal
                    scope.context.attachment = attachment
                    return await self._middleware.invoke(
                        scope, lambda _s: self._dispatch(data, instance)
                    )
                return await self._dispatch(data, instance)

            await execute_with_retry(_process, self.retry_policy)
        except Exception as e:
            self._logger.error(
                "Subscriber error on %s: %s", self.prefix, e,
                exc_info=True,
                extra={"prefix": self.prefix},
            )

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_subscribe_wrapper(self, instance)

