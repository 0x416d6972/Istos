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

from istos.core.errors import ForbiddenError, UnauthorizedError


@dataclass
class AuthContext:
    """Information about an incoming request, handed to an :class:`Authorizer`."""

    prefix: str
    key_expr: str
    params: dict[str, Any] = field(default_factory=dict)
    attachment: Optional[bytes] = None
    operation: str = "handle"
    #: Identity resolved by an upstream authorizer in a layered chain. Set by
    #: ``combine_authorizers`` after the app-wide authorizer runs, so a per-handler
    #: guard like ``require_roles`` can inspect who was authenticated.
    principal: Any = None

    @property
    def token(self) -> Optional[str]:
        """The auth token from the request attachment (envelope-aware).

        Accepts both a bare-token attachment and a structured
        :class:`~istos.context.RequestEnvelope`.
        """
        from istos.context import RequestEnvelope

        return RequestEnvelope.from_attachment(self.attachment).token


@dataclass
class Principal:
    """The authenticated identity behind a request.

    A convenience shape for the value an authorizer may return to say "allowed,
    and here is *who*". Istos treats any non-``bool`` truthy return from an
    authorizer as a principal, so you can return your own type instead; this class
    is a default carrying an id, roles, and arbitrary claims.

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
    # True = allow with no identity; any other truthy value is the principal.
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
        app_principal = await check_authorized(app_authorizer, ctx)
        # Per-handler guards (require_roles) read who the app layer authenticated.
        ctx.principal = app_principal
        handler_principal = await check_authorized(handler_authorizer, ctx)
        # Prefer handler identity, then app-wide; True if both were bare bools.
        return handler_principal or app_principal or True

    return _layered


class JWTAuthorizer:
    """Authenticate a request from a JSON Web Token in its attachment.

    Verifies the token's signature and standard claims (``exp``, and — when
    configured — ``aud`` / ``iss``) with `PyJWT <https://pyjwt.readthedocs.io>`_
    (the ``istos[jwt]`` extra), then maps it to a :class:`Principal`: ``id`` from
    the ``id_claim`` (``sub`` by default), roles from ``roles_claim``, and the
    full decoded payload as ``claims``.

    Symmetric (HS*) verification uses ``secret``; asymmetric (RS*/ES*/PS*) uses
    ``public_key``. The ``none`` algorithm is always rejected.

        # HS256 shared secret:
        Istos(authorizer=JWTAuthorizer(secret=os.environ["JWT_SECRET"]))

        # RS256 with an identity provider's public key + audience:
        Istos(authorizer=JWTAuthorizer(
            public_key=PUBKEY_PEM, algorithms=["RS256"], audience="my-api"))

    An absent, malformed, or invalid token is a denial (falsy → ``UnauthorizedError``).
    """

    def __init__(
        self,
        secret: Optional[str] = None,
        *,
        public_key: Optional[str] = None,
        algorithms: Iterable[str] = ("HS256",),
        audience: Optional[str] = None,
        issuer: Optional[str] = None,
        roles_claim: str = "roles",
        id_claim: str = "sub",
        leeway: float = 0,
        require_exp: bool = True,
    ) -> None:
        try:
            import jwt  # noqa: F401
        except ImportError as e:  # pragma: no cover - exercised via the extra
            raise RuntimeError(
                "JWTAuthorizer requires the 'pyjwt' package. Install it with "
                "`pip install \"istos[jwt]\"`."
            ) from e

        self._algorithms = list(algorithms)
        if "none" in (a.lower() for a in self._algorithms):
            raise ValueError("The 'none' JWT algorithm is unsafe and not allowed.")
        self._key = public_key or secret
        if not self._key:
            raise ValueError("JWTAuthorizer requires a `secret` or `public_key`.")
        self._audience = audience
        self._issuer = issuer
        self._roles_claim = roles_claim
        self._id_claim = id_claim
        self._leeway = leeway
        self._require_exp = require_exp

    @staticmethod
    def _as_roles(value: Any) -> frozenset[str]:
        if value is None:
            return frozenset()
        if isinstance(value, str):
            # Accept a space- or comma-separated scope-style string.
            return frozenset(value.replace(",", " ").split())
        if isinstance(value, (list, tuple, set, frozenset)):
            return frozenset(str(v) for v in value)
        return frozenset()

    def __call__(self, ctx: AuthContext) -> Any:
        import jwt

        token = ctx.token
        if not token:
            return False
        options = {"require": ["exp"] if self._require_exp else [],
                   "verify_aud": self._audience is not None}
        try:
            payload = jwt.decode(
                token,
                self._key,
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
                leeway=self._leeway,
                options=options,
            )
        except jwt.PyJWTError:
            return False
        return Principal(
            id=str(payload.get(self._id_claim, "")),
            roles=self._as_roles(payload.get(self._roles_claim)),
            claims=payload,
        )


def _roles_of(principal: Any) -> frozenset[str]:
    """Best-effort role set from a principal (``Principal`` or any object with
    a ``roles`` attribute)."""
    roles = getattr(principal, "roles", None)
    if roles is None:
        return frozenset()
    return frozenset(roles)


def require_roles(
    *roles: str,
    mode: str = "all",
    authenticator: Optional[Authorizer] = None,
) -> Authorizer:
    """Authorize based on the authenticated principal's roles (RBAC).

    Designed to **layer** on top of an authenticating authorizer: set the
    authenticator app-wide (``Istos(authorizer=JWTAuthorizer(...))``) and guard
    individual handlers with the roles they need::

        @app.handle("admin/reset", authorizer=require_roles("admin"))
        async def reset(): ...

    ``mode="all"`` (default) requires every listed role; ``mode="any"`` requires
    at least one. When no authenticator has run (no identity), the request is
    **401**; when the identity lacks the roles, it is **403**.

    If there is no app-wide authenticator, pass ``authenticator=`` to run one
    first::

        require_roles("admin", authenticator=JWTAuthorizer(secret=...))
    """
    required = frozenset(roles)
    if mode not in ("all", "any"):
        raise ValueError("mode must be 'all' or 'any'")

    async def _authz(ctx: AuthContext) -> Any:
        principal = ctx.principal
        if authenticator is not None:
            principal = await check_authorized(authenticator, ctx)
            ctx.principal = principal
        if principal is None:
            raise UnauthorizedError(f"Authentication required for '{ctx.prefix}'")
        have = _roles_of(principal)
        ok = bool(required & have) if mode == "any" else required <= have
        if not ok:
            raise ForbiddenError(
                f"Requires role(s) {sorted(required)} ({mode}); "
                f"principal has {sorted(have)}"
            )
        return principal

    return _authz
