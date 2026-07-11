"""Authorization primitives for Istos network handlers.

Istos handlers are invoked over a shared Zenoh fabric where, by default, any
peer can query any key expression. An :class:`Authorizer` is the hook that
decides whether an incoming request is allowed to reach a handler.

An authorizer receives an :class:`AuthContext` describing the request (the key
expression, parameters, and any attachment the caller sent) and returns:

* a **truthy** value to allow the request, or
* a falsy value — or raises :class:`~istos.core.errors.UnauthorizedError` —
  to deny it.

The truthy value may be a bare ``True`` (a stateless "allowed"), or a
**principal** — any object identifying *who* made the request (for example a
:class:`Principal`). When an authorizer returns a principal, Istos stashes it on
the request context so the handler body can inject it with
``Depends(current_principal)`` — the gate resolves identity once, and the body
reuses it. Both sync and async authorizers are supported.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable, Optional, Union

from istos.core.errors import UnauthorizedError


@dataclass
class AuthContext:
    """Information about an incoming request, handed to an :class:`Authorizer`."""

    prefix: str
    key_expr: str
    params: dict[str, Any] = field(default_factory=dict)
    attachment: Optional[bytes] = None
    operation: str = "handle"

    @property
    def token(self) -> Optional[str]:
        """Decode the request attachment as a UTF-8 token, if present."""
        if self.attachment is None:
            return None
        try:
            return bytes(self.attachment).decode("utf-8")
        except (UnicodeDecodeError, ValueError, TypeError):
            return None


@dataclass
class Principal:
    """The authenticated identity behind a request.

    A convenience shape for the value an authorizer may return to say "allowed,
    and here is *who*". Istos treats **any** non-``bool`` truthy return from an
    authorizer as a principal, so you are free to return your own type instead —
    this class is just a batteries-included default carrying an id, roles, and
    arbitrary claims.

    Inject it into a handler body with ``Depends(current_principal)``.
    """

    id: str
    roles: frozenset[str] = field(default_factory=frozenset)
    claims: dict[str, Any] = field(default_factory=dict)

    def has_role(self, role: str) -> bool:
        """True if this principal carries ``role``."""
        return role in self.roles


# An authorizer returns a truthy value to allow (either bare ``True`` or a
# principal object), or a falsy value / raises UnauthorizedError to deny.
AuthResult = Union[bool, Any]
Authorizer = Callable[[AuthContext], Union[AuthResult, Awaitable[AuthResult]]]


class TokenAuthorizer:
    """Shared-secret authorizer.

    Allows a request only when its attachment carries one of the accepted
    tokens. Callers supply the token via the ``attachment=`` argument of their
    Zenoh get/put (see :meth:`Istos.query_once`).

        istos = Istos(authorizer=TokenAuthorizer("s3cr3t"))
    """

    def __init__(self, tokens: Union[str, Iterable[str]]):
        if isinstance(tokens, str):
            tokens = {tokens}
        self._tokens = set(tokens)
        if not self._tokens:
            raise ValueError("TokenAuthorizer requires at least one token")

    def __call__(self, ctx: AuthContext) -> bool:
        return ctx.token is not None and ctx.token in self._tokens


async def check_authorized(authorizer: Optional[Authorizer], ctx: AuthContext) -> Any:
    """Run ``authorizer`` against ``ctx``, raising on denial.

    A ``None`` authorizer is a no-op (allow). Supports sync and async
    authorizers; a falsy return is treated as denial.

    Returns the **principal** the authorizer resolved, or ``None`` when the
    request was allowed without an identity (authorizer absent, or it returned a
    bare ``True``). Callers use this to expose the identity to the handler body.
    """
    if authorizer is None:
        return None
    result = authorizer(ctx)
    if inspect.isawaitable(result):
        result = await result
    if not result:
        raise UnauthorizedError(f"Not authorized for '{ctx.prefix}'")
    # A bare ``True`` is a stateless allow and carries no identity; any other
    # truthy value is the principal.
    return None if result is True else result


class _Public:
    """Sentinel authorizer marking a handler as intentionally public.

    Passing ``authorizer=Public`` opts a single handler out of the app-wide
    authorizer entirely — the request is allowed even when ``Istos(authorizer=...)``
    is set. Use it to open one endpoint under an otherwise-protected app.
    """

    def __repr__(self) -> str:
        return "Public"

    def __call__(self, ctx: AuthContext) -> bool:
        return True


#: Shared sentinel — pass ``authorizer=Public`` to bypass the app-wide gate.
Public = _Public()


def combine_authorizers(
    app_authorizer: Optional[Authorizer],
    handler_authorizer: Optional[Union[Authorizer, _Public]],
) -> Optional[Authorizer]:
    """Resolve a handler's effective authorizer using **layered** semantics.

    This mirrors mainstream frameworks (global middleware + route guard, Envoy
    ext_authz + RBAC): the app-wide gate is a baseline that always applies, and a
    per-handler authorizer adds an *additional* requirement on top of it.

    - ``handler_authorizer is None``   → inherit the app-wide authorizer only.
    - ``handler_authorizer is Public`` → explicitly public; the app-wide gate is
      bypassed for this one handler.
    - otherwise → **both** the app-wide and the per-handler authorizer must allow
      the request (defense in depth).
    """
    if handler_authorizer is Public:
        return None
    if handler_authorizer is None:
        return app_authorizer
    if app_authorizer is None:
        return handler_authorizer

    async def _layered(ctx: AuthContext) -> Any:
        # Both must pass; check_authorized raises UnauthorizedError on either denial.
        app_principal = await check_authorized(app_authorizer, ctx)
        handler_principal = await check_authorized(handler_authorizer, ctx)
        # Prefer the more specific (per-handler) identity, then the app-wide one.
        # Fall back to ``True`` so an all-``bool`` chain still reads as allowed.
        return handler_principal or app_principal or True

    return _layered
