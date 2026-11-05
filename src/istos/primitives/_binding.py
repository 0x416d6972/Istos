"""Checks that run when an endpoint is bound to the network."""

import inspect
from typing import Any, Callable


def expects_instance(func: Callable[..., Any]) -> bool:
    """Whether ``func`` is a plain method whose first parameter is ``self``."""
    try:
        params = list(inspect.signature(func).parameters.values())
    except (TypeError, ValueError):
        return False
    return bool(params) and params[0].name == "self"


def ensure_network_bindable(func: Callable[..., Any], verb: str, prefix: str) -> None:
    """Refuse to serve a method over the network.

    The decorator wraps the function defined in the class body, so a network
    delivery has no instance to pass as ``self`` and every call would fail with
    an internal error. In-process calls through an instance still work; it is
    only the network path that cannot.
    """
    if expects_instance(func):
        name = getattr(func, "__qualname__", getattr(func, "__name__", repr(func)))
        raise TypeError(
            f"@{verb}({prefix!r}) decorates {name}, a method taking 'self'. "
            "A network delivery has no instance to bind it to. Decorate a "
            "module-level function (or a closure over the instance) instead."
        )
