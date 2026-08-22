import inspect
import hashlib
import asyncio
import json as _json
import time
import warnings
import zenoh
from contextlib import AsyncExitStack
from typing import Any, Callable, Iterable, Mapping, Optional, Tuple, Union, cast, get_type_hints
from istos.consistency.storage import (
    DEFAULT_CLAIM_LEASE_S,
    Claim,
    ClaimState,
    Durability,
    StoragePlugin,
)
from istos.messages.serialization import Serialize
from istos.validation import validate_params, SchemaValidationError
from istos.retry import RetryPolicy, execute_with_retry
from istos.errors import (
    ConflictError,
    ExceptionHandlerRegistry,
    get_default_registry,
    UnauthorizedError,
)
from istos.security.authz import Authorizer, AuthContext, check_authorized
from istos.http.gateway import decode_params
from istos.di.depends import resolve_dependencies, extract_depends
from istos.context import RequestEnvelope, get_request_context
from istos.middleware.base import MiddlewareStack, RequestScope
from istos.logging import get_logger

# One warning per storage class — a degraded backend would otherwise warn on
# every request.
_WARNED_NO_CLAIM: set[str] = set()

try:
    from pydantic import BaseModel, TypeAdapter, ValidationError
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False


class bound_handler_wrapper:
    """Bound-method proxy that injects `self` (the instance) into calls."""
    def __init__(self, desc: "handler_wrapper", subj: Any):
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class handler_wrapper:
    """
    Descriptor that replaces the original function.
    Tracks invocations and writes serialized params/result to storage.
    Supports retry, return-type validation, and storage injection.
    """
    def __init__(
        self,
        func: Callable,
        prefix: str,
        storage: StoragePlugin,
        serializer: Serialize,
        retry: Optional[Union[int, RetryPolicy]] = None,
        durability: Union[str, Durability] = Durability.AT_MOST_ONCE,
        middleware: Optional[MiddlewareStack] = None,
        exception_registry: Optional[ExceptionHandlerRegistry] = None,
        authorizer: Optional[Authorizer] = None,
        dependency_overrides: Optional[Mapping[Callable, Callable]] = None,
        approval: Union[bool, str] = False,
        idempotency_lease_s: Optional[float] = None,
    ):
        self.func = func
        self.prefix = prefix
        # Advisory, for agents: "a human should clear this call before it runs".
        # Carried in the capability manifest; enforcement is the caller's gate
        # (see istos.agent.approval), not this wrapper — the authorizer is what
        # stops a peer that ignores the flag.
        self.approval = approval
        self.storage = storage
        self.serializer = serializer
        self.calls = 0
        self._middleware = middleware
        self._authorizer = authorizer
        self._exception_registry = exception_registry or get_default_registry()
        self._logger = get_logger("handler")
        self.idempotency_lease_s = (
            DEFAULT_CLAIM_LEASE_S if idempotency_lease_s is None else idempotency_lease_s
        )

        if retry is None:
            self.retry_policy = RetryPolicy(max_retries=0)
        elif isinstance(retry, int):
            self.retry_policy = RetryPolicy.from_int(retry)
        else:
            self.retry_policy = retry

        _params = inspect.signature(func).parameters
        self._inject_db = "db" in _params
        self._depends_params = {n for n, p in _params.items() if extract_depends(p) is not None}
        self._has_depends = bool(self._depends_params)
        # db / Depends(...) are framework-injected — not network params.
        self._injected_params = set(self._depends_params)
        if self._inject_db:
            self._injected_params.add("db")
        self._dependency_overrides = dependency_overrides if dependency_overrides is not None else {}

        if isinstance(durability, str):
            self.durability = Durability(durability)
        else:
            self.durability = durability

        hints = get_type_hints(func)
        self._return_type = hints.get("return", None)

    @staticmethod
    def _make_idempotency_key(prefix: str, params: dict) -> str:
        """
        Deterministic key from prefix + sorted params.
        Same input always produces the same key → enables exactly-once.
        """
        raw = f"{prefix}:{_json.dumps(params, sort_keys=True, default=str)}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _validate_return(self, result: Any) -> Any:
        """Validate the return value against the function's return type hint."""
        if self._return_type is None or not HAS_PYDANTIC:
            return result
        if self._return_type is type(None):
            return result
        try:
            adapter = TypeAdapter(self._return_type)
            return adapter.validate_python(result)
        except ValidationError as e:
            raise SchemaValidationError(
                e.errors(),
                message=f"Return type validation failed for '{self.func.__name__}'"
            ) from e

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1

        # Idempotency key from network params only (not db / Depends).
        idemp_params = {k: v for k, v in kwargs.items() if k not in self._injected_params}
        idemp_key = self._make_idempotency_key(self.prefix, idemp_params)

        claimed = False
        if self.durability == Durability.EXACTLY_ONCE:
            # Claim before executing: a check first and a mark afterwards leaves
            # the whole handler body between the two, so every concurrent
            # redelivery passes the check and runs the side effects.
            claim = await self._claim(idemp_key)
            if claim.state is ClaimState.DONE:
                return claim.result
            if claim.state is ClaimState.IN_FLIGHT:
                raise ConflictError(
                    f"'{self.prefix}' is already executing this exact request; "
                    "retry to collect its result",
                    details={"idempotency_key": idemp_key},
                )
            claimed = True

        try:
            return await self._invoke(args, kwargs, idemp_params, idemp_key)
        except BaseException:
            # The work did not complete, so the key must not stay claimed or a
            # failed call is never retryable. Cancellation counts: a cancelled
            # handler has no result either.
            if claimed:
                await self._release(idemp_key)
            raise

    async def _claim(self, idemp_key: str) -> Claim:
        """Claim the idempotency key, atomically where the backend can."""
        claim_processed = getattr(self.storage, "claim_processed", None)
        if claim_processed is not None:
            return cast(Claim, await claim_processed(idemp_key, lease_s=self.idempotency_lease_s))
        # A plugin written against the pre-claim protocol. Check-then-act is all
        # that is left, and it does not hold under concurrency — say so rather
        # than let it pass as exactly-once.
        self._warn_no_claim_support()
        cached = await self.storage.check_processed(idemp_key)
        if cached is not None:
            return Claim(ClaimState.DONE, cached)
        return Claim(ClaimState.CLAIMED)

    async def _release(self, idemp_key: str) -> None:
        release_claim = getattr(self.storage, "release_claim", None)
        if release_claim is None:
            return
        try:
            await release_claim(idemp_key)
        except Exception:
            # The lease expires anyway, so this costs a delay, not correctness.
            # Never mask the error that got us here.
            self._logger.warning(
                "failed to release idempotency claim; it expires in %ss",
                self.idempotency_lease_s,
                exc_info=True,
            )

    def _warn_no_claim_support(self) -> None:
        backend = type(self.storage).__name__
        if backend in _WARNED_NO_CLAIM:
            return
        _WARNED_NO_CLAIM.add(backend)
        warnings.warn(
            f"{backend} does not implement claim_processed(), so "
            "durability='exactly_once' degrades to at-least-once with a result "
            "cache: concurrent duplicates of one request can all execute. "
            "Implement claim_processed()/release_claim() (see StoragePlugin) or "
            "use a built-in storage backend.",
            RuntimeWarning,
            stacklevel=2,
        )

    async def _invoke(
        self,
        args: Tuple[Any, ...],
        kwargs: dict,
        idemp_params: dict,
        idemp_key: str,
    ) -> Any:
        """Resolve dependencies, run the handler, and record the outcome."""
        # Exit stack tears down yield-deps after the handler finishes.
        async with AsyncExitStack() as di_stack:
            call_kwargs = dict(kwargs)

            if self._inject_db and "db" not in call_kwargs:
                call_kwargs["db"] = self.storage

            if self._has_depends:
                call_kwargs = await resolve_dependencies(
                    self.func,
                    call_kwargs,
                    di_stack,
                    cache={},
                    overrides=self._dependency_overrides,
                )

            async def _execute():
                async def _handler(scope: RequestScope) -> Any:
                    if inspect.iscoroutinefunction(self.func):
                        return await self.func(*args, **call_kwargs)
                    return await asyncio.to_thread(self.func, *args, **call_kwargs)

                if self._middleware is not None:
                    scope = RequestScope(
                        prefix=self.prefix,
                        operation="handle",
                        params=idemp_params,
                    )
                    # middleware.invoke() installs a fresh context — copy
                    # principal / attachment / cid / traceparent into it.
                    outer = get_request_context()
                    scope.context.principal = outer.principal
                    scope.context.attachment = outer.attachment
                    scope.context.correlation_id = outer.correlation_id
                    scope.context.traceparent = outer.traceparent
                    return await self._middleware.invoke(scope, _handler)
                return await _handler(RequestScope(prefix=self.prefix, operation="handle"))

            result = await execute_with_retry(_execute, self.retry_policy)
            result = self._validate_return(result)

            metadata = {
                "func_name": self.func.__name__,
                "total_calls": self.calls,
                "timestamp": time.time(),
                "status": "ok",
            }
            try:
                serialized = self.serializer.serialize(metadata)
                await self.storage.put(self.prefix, serialized)

                if self.durability in (Durability.AT_LEAST_ONCE, Durability.EXACTLY_ONCE):
                    await self.storage.log(self.prefix, serialized, idempotency_key=idemp_key)
            except Exception:
                pass  # storage write must never crash the handler

            # Complete the claim last: log() skips keys already marked done, so
            # the event log must land while the key is still only claimed. Not
            # best-effort like the writes above — swallowing this leaves the key
            # claimed until the lease lapses and the work runs twice.
            if self.durability == Durability.EXACTLY_ONCE:
                await self.storage.mark_processed(idemp_key, result)

            return result

    @staticmethod
    def _extract_attachment(query: zenoh.Query) -> Optional[bytes]:
        """Best-effort read of a query's attachment as raw bytes (for auth tokens)."""
        raw = getattr(query, "attachment", None)
        if raw is None:
            return None
        try:
            return bytes(raw)
        except (TypeError, ValueError):
            return None

    async def on_query(self, query: zenoh.Query) -> None:
        try:
            key = str(query.selector.key_expr)

            # zenoh.Parameters has no .items(); iterates as (key, value).
            # stub omits __iter__, but it is iterable at runtime.
            params: dict = {}
            if hasattr(query.selector, "parameters") and query.selector.parameters:
                params = decode_params(
                    dict(cast(Iterable[Tuple[str, str]], query.selector.parameters))
                )

            # Network gate only — TestClient / in-process __call__ skips this.
            attachment = self._extract_attachment(query)
            try:
                principal = await check_authorized(
                    self._authorizer,
                    AuthContext(
                        prefix=self.prefix,
                        key_expr=key,
                        params=params,
                        attachment=attachment,
                    ),
                )
            except UnauthorizedError as e:
                error = self._exception_registry.resolve(e)
                error.correlation_id = get_request_context().correlation_id
                self._logger.warning(
                    "Unauthorized request on %s: %s", self.prefix, e,
                    extra={"prefix": self.prefix, "error": str(e)},
                )
                try:
                    query.reply(key, self.serializer.serialize(error.to_dict()))
                except Exception:
                    pass
                return

            req_ctx = get_request_context()
            req_ctx.prefix = self.prefix
            req_ctx.operation = "handle"
            req_ctx.principal = principal
            req_ctx.attachment = attachment
            # Keep the caller's correlation_id / traceparent across hops.
            _env = RequestEnvelope.from_attachment(attachment)
            if _env.correlation_id:
                req_ctx.correlation_id = _env.correlation_id
            req_ctx.traceparent = _env.traceparent

            try:
                validated_params = validate_params(
                    self.func, params, skip_params=self._injected_params
                )
                validated_params.pop("db", None)
            except SchemaValidationError as e:
                error = self._exception_registry.resolve(e)
                error.correlation_id = get_request_context().correlation_id
                self._logger.warning(
                    "Validation error on %s: %s", self.prefix, e,
                    extra={"prefix": self.prefix, "error": str(e)},
                )
                query.reply(key, self.serializer.serialize(error.to_dict()))
                return

            result = await self(**validated_params)

            if result is not None:
                payload = self.serializer.serialize(result)
                query.reply(key, payload)
        except Exception as e:
            error = self._exception_registry.resolve(e)
            error.correlation_id = get_request_context().correlation_id
            self._logger.error(
                "Handler error on %s: %s", self.prefix, e,
                exc_info=True,
                extra={"prefix": self.prefix},
            )
            try:
                query.reply(
                    str(query.selector.key_expr),
                    self.serializer.serialize(error.to_dict()),
                )
            except Exception:
                pass
            
    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_handler_wrapper(self, instance)

