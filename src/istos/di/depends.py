import asyncio
import inspect
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from typing import Any, Callable, Dict, Optional, Mapping, Tuple


class Depends:
    """
    Dependency injection marker.

    Use it either as a default value or inside ``Annotated`` (recommended)::

        def handler(svc: Service = Depends(get_service)): ...
        def handler(svc: Annotated[Service, Depends(get_service)]): ...

    :param dependency: The callable to resolve (plain, async, or a sync/async
        generator for setup/teardown via ``yield``).
    :param use_cache: If True (default), the result is cached per-request so the
        same dependency resolves once even if declared by several parameters.
    """
    def __init__(self, dependency: Callable[..., Any], use_cache: bool = True):
        self.dependency = dependency
        self.use_cache = use_cache

    def __repr__(self) -> str:
        name = getattr(self.dependency, "__name__", repr(self.dependency))
        return f"Depends({name})"


class DependencyCycleError(RuntimeError):
    """Raised when dependencies form a cycle (A needs B needs A)."""


def extract_depends(param: inspect.Parameter) -> Optional[Depends]:
    """Return the Depends marker for a parameter, from either its default value
    or an ``Annotated[..., Depends(...)]`` annotation."""
    if isinstance(param.default, Depends):
        return param.default
    for meta in getattr(param.annotation, "__metadata__", ()):  # Annotated metadata
        if isinstance(meta, Depends):
            return meta
    return None


def has_dependencies(func: Callable[..., Any]) -> bool:
    """True if any parameter of ``func`` declares a Depends (default or Annotated)."""
    try:
        params = inspect.signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(extract_depends(p) is not None for p in params)


@asynccontextmanager
async def _sync_cm_in_thread(cm: Any):
    """Wrap a sync context manager so its enter/exit run off the event loop."""
    value = await asyncio.to_thread(cm.__enter__)
    try:
        yield value
    except BaseException as exc:  # propagate into the CM's __exit__, off-loop
        if not await asyncio.to_thread(cm.__exit__, type(exc), exc, exc.__traceback__):
            raise
    else:
        await asyncio.to_thread(cm.__exit__, None, None, None)


async def resolve_dependencies(
    func: Callable[..., Any],
    existing_kwargs: Mapping[str, Any],
    exit_stack: AsyncExitStack,
    cache: Optional[Dict[Callable[..., Any], Any]] = None,
    overrides: Optional[Mapping[Callable[..., Any], Callable[..., Any]]] = None,
    _chain: Tuple[Callable[..., Any], ...] = (),
    skip: Optional[set] = None,
) -> Dict[str, Any]:
    """
    Recursively resolve a callable's dependencies into a kwargs dict.

    Supports ``Depends`` via default value or ``Annotated``, sub-dependencies,
    per-request caching, testing overrides, and ``yield`` dependencies whose
    teardown is registered on ``exit_stack``. Sync dependencies (plain or
    generator) are offloaded to a worker thread so they never block the loop.

    ``skip`` names parameters the caller supplies positionally (e.g. a message
    payload); they are neither resolved nor validated here.
    """
    if cache is None:
        cache = {}
    if overrides is None:
        overrides = {}
    skip = skip or set()

    sig = inspect.signature(func)
    resolved_kwargs: Dict[str, Any] = {}

    for param_name, param in sig.parameters.items():
        if param_name == "self" or param_name in skip:
            continue

        depends = extract_depends(param)

        # Context injection: a plain parameter whose value is supplied by the caller.
        if depends is None and param_name in existing_kwargs:
            resolved_kwargs[param_name] = existing_kwargs[param_name]
            continue

        # **kwargs: splat any remaining context values.
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            for k, v in existing_kwargs.items():
                if k not in resolved_kwargs:
                    resolved_kwargs[k] = v
            continue

        if depends is not None:
            original = depends.dependency
            dependency_func = overrides.get(original, original)  # testing overrides

            # Cycle detection.
            if original in _chain or dependency_func in _chain:
                names = " -> ".join(
                    getattr(f, "__name__", repr(f)) for f in (*_chain, dependency_func)
                )
                raise DependencyCycleError(f"Circular dependency detected: {names}")

            # Per-request cache.
            if depends.use_cache and dependency_func in cache:
                resolved_kwargs[param_name] = cache[dependency_func]
                continue

            # Resolve sub-dependencies first.
            dep_kwargs = await resolve_dependencies(
                dependency_func,
                existing_kwargs,
                exit_stack,
                cache=cache,
                overrides=overrides,
                _chain=(*_chain, dependency_func),
            )

            # Execute, managing yield-dependency lifecycles on the exit stack.
            if inspect.isasyncgenfunction(dependency_func):
                acm = asynccontextmanager(dependency_func)(**dep_kwargs)
                result = await exit_stack.enter_async_context(acm)
            elif inspect.isgeneratorfunction(dependency_func):
                scm = contextmanager(dependency_func)(**dep_kwargs)
                result = await exit_stack.enter_async_context(_sync_cm_in_thread(scm))
            elif inspect.iscoroutinefunction(dependency_func):
                result = await dependency_func(**dep_kwargs)
            else:
                # Sync dependency → offload so it can't block the event loop.
                result = await asyncio.to_thread(lambda: dependency_func(**dep_kwargs))

            if depends.use_cache:
                cache[dependency_func] = result
            resolved_kwargs[param_name] = result
            continue

        # Not context, not **kwargs, not a Depends. If it's required, fail loudly
        # rather than letting the eventual call raise a bare TypeError.
        if param.default is inspect.Parameter.empty and param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise TypeError(
                f"Cannot resolve required parameter {param_name!r} of "
                f"{getattr(func, '__name__', func)!r}: it was not supplied and is "
                f"not a Depends(...)."
            )
        # Otherwise the parameter has its own default; leave it to the callable.

    return resolved_kwargs


async def inject_and_run(
    func: Callable[..., Any],
    context: Optional[Mapping[str, Any]] = None,
    overrides: Optional[Mapping[Callable[..., Any], Callable[..., Any]]] = None,
) -> Any:
    """
    High-level entrypoint: resolve ``func``'s dependencies and call it.

    Manages the lifecycle of all ``yield`` dependencies via an AsyncExitStack,
    so they tear down cleanly even if the call raises.
    """
    context = context or {}
    overrides = overrides or {}

    async with AsyncExitStack() as stack:
        kwargs = await resolve_dependencies(func, context, stack, cache={}, overrides=overrides)
        if inspect.iscoroutinefunction(func):
            return await func(**kwargs)
        return await asyncio.to_thread(lambda: func(**kwargs))


def positional_param_names(func: Callable[..., Any]) -> list:
    """Ordered names of the parameters a framework supplies positionally.

    Excludes ``self``, ``Depends(...)`` parameters, and *args/**kwargs — i.e. the
    slots a wrapper fills with a payload (message data, query result, ...).
    """
    names: list[str] = []
    try:
        params = inspect.signature(func).parameters
    except (TypeError, ValueError):
        return names
    for name, param in params.items():
        if name == "self":
            continue
        if extract_depends(param) is not None:
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        names.append(name)
    return names


async def invoke_with_dependencies(
    func: Callable[..., Any],
    *,
    args: Tuple[Any, ...] = (),
    context: Optional[Mapping[str, Any]] = None,
    skip_names: Tuple[str, ...] = (),
    overrides: Optional[Mapping[Callable[..., Any], Callable[..., Any]]] = None,
) -> Any:
    """Resolve ``func``'s dependencies and call it, driving yield-dep teardown.

    ``args`` are positional values passed through unchanged (e.g. an instance and
    a message payload); ``skip_names`` are the parameter names those positional
    values fill, so they are not treated as dependencies. Sync callables run in a
    worker thread.
    """
    async with AsyncExitStack() as stack:
        resolved = await resolve_dependencies(
            func,
            context or {},
            stack,
            cache={},
            overrides=overrides,
            skip=set(skip_names),
        )
        if inspect.iscoroutinefunction(func):
            return await func(*args, **resolved)
        return await asyncio.to_thread(lambda: func(*args, **resolved))


# Backwards-compatible alias.
async_resolve_dependencies = resolve_dependencies
