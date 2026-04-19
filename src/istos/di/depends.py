from typing import Any, Callable, TypeVar, Generic, Union, get_type_hints
import inspect

T = TypeVar("T")

class Depends:
    """
    Dependency Injection marker.
    """
    def __init__(self, dependency: Callable[..., Any]):
        self.dependency = dependency

async def resolve_dependencies(func: Callable[..., Any], existing_kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Resolve dependencies for a function call.
    Supports both sync and async functions/dependencies.
    """
    sig = inspect.signature(func)
    resolved_kwargs = existing_kwargs.copy()
    
    for param_name, param in sig.parameters.items():
        if isinstance(param.default, Depends):
            dependency_func = param.default.dependency
            
            # Recursively resolve dependencies for the dependency itself
            dep_kwargs = await resolve_dependencies(dependency_func, {})
            
            if inspect.iscoroutinefunction(dependency_func):
                resolved_kwargs[param_name] = await dependency_func(**dep_kwargs)
            else:
                resolved_kwargs[param_name] = dependency_func(**dep_kwargs)
                
    return resolved_kwargs

# Alias for compatibility with the user's snippet
async_resolve_dependencies = resolve_dependencies
