import inspect
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from typing import Any, Callable, TypeVar, Dict, Optional, Mapping

T = TypeVar("T")

class Depends:
    """
    Dependency Injection marker.
    
    :param dependency: The callable to resolve.
    :param use_cache: If True, the result is cached per-request (default is True).
    """
    def __init__(self, dependency: Callable[..., Any], use_cache: bool = True):
        self.dependency = dependency
        self.use_cache = use_cache

async def resolve_dependencies(
    func: Callable[..., Any],
    existing_kwargs: Mapping[str, Any],
    exit_stack: AsyncExitStack,
    cache: Optional[Dict[Callable[..., Any], Any]] = None,
    overrides: Optional[Mapping[Callable[..., Any], Callable[..., Any]]] = None,
) -> Dict[str, Any]:
    """
    Recursively resolves dependencies, supports yield fixtures and context access.
    """
    if cache is None:
        cache = {}
    if overrides is None:
        overrides = {}

    sig = inspect.signature(func)
    resolved_kwargs: Dict[str, Any] = {}

    for param_name, param in sig.parameters.items():
        # Inject standard Context Kwargs if they match the function parameter
        if param_name in existing_kwargs and not isinstance(param.default, Depends):
            resolved_kwargs[param_name] = existing_kwargs[param_name]
            continue
            
        # Catch **kwargs injection
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            for k, v in existing_kwargs.items():
                if k not in resolved_kwargs:
                    resolved_kwargs[k] = v
            continue

        # Handle Dependency Injection
        if isinstance(param.default, Depends):
            depends: Depends = param.default
            original_dependency = depends.dependency
            
            # 1. Testing Overrides
            dependency_func = overrides.get(original_dependency, original_dependency)
            
            # 2. Request-Scope Caching
            if depends.use_cache and dependency_func in cache:
                resolved_kwargs[param_name] = cache[dependency_func]
                continue

            # 3. Recursively resolve sub-dependencies
            dep_kwargs = await resolve_dependencies(
                func=dependency_func,
                existing_kwargs=existing_kwargs,
                exit_stack=exit_stack,
                cache=cache,
                overrides=overrides,
            )

            # 4. Execute Dependency (with Generative Lifecycle Management via ExitStack)
            result: Any
            
            if inspect.isasyncgenfunction(dependency_func):
                cm_async = asynccontextmanager(dependency_func)(**dep_kwargs)
                result = await exit_stack.enter_async_context(cm_async)
            elif inspect.isgeneratorfunction(dependency_func):
                cm_sync = contextmanager(dependency_func)(**dep_kwargs)
                result = exit_stack.enter_context(cm_sync)
            elif inspect.iscoroutinefunction(dependency_func):
                result = await dependency_func(**dep_kwargs)
            else:
                result = dependency_func(**dep_kwargs)

            # Save to cache
            if depends.use_cache:
                cache[dependency_func] = result
                
            resolved_kwargs[param_name] = result

    return resolved_kwargs

async def inject_and_run(
    func: Callable[..., Any], 
    context: Optional[Mapping[str, Any]] = None,
    overrides: Optional[Mapping[Callable[..., Any], Callable[..., Any]]] = None
) -> Any:
    """
    High-level entrypoint to run any function with absolute DI resolution.
    This manages the lifecycle of all 'yield' dependencies safely.
    """
    context = context or {}
    overrides = overrides or {}
    
    # AsyncExitStack guarantees all 'yield' generators properly close even if errors happen!
    async with AsyncExitStack() as stack:
        kwargs = await resolve_dependencies(func, context, stack, cache={}, overrides=overrides)
        
        if inspect.iscoroutinefunction(func):
            return await func(**kwargs)
        else:
            return func(**kwargs)

# Alias for backward compatibility
async_resolve_dependencies = resolve_dependencies
