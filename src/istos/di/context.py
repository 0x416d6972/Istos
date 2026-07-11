"""Injectable accessors for request-scoped values.

These are ordinary dependency callables — use them with :class:`Depends` to pull
request-scoped state into a handler body::

    from istos import Depends
    from istos.di import current_principal

    @istos.handle("fleet/shutdown", authorizer=admins_only)
    async def shutdown(user=Depends(current_principal)) -> dict:
        log.info("shutdown requested by %s", user.id)
        return {"stopping": True}

The values they return are populated by :meth:`Istos` at the network boundary,
*after* the authorizer has allowed the request. In an in-process call
(``TestClient`` / local ``@query``) there is no network gate, so ``current_principal``
returns ``None`` — override it in tests via ``dependency_overrides`` when a
handler needs an identity.
"""

from __future__ import annotations

from typing import Any, Optional

from istos.context import RequestContext, get_request_context


def current_request() -> RequestContext:
    """Return the active :class:`RequestContext` for this request."""
    return get_request_context()


def current_principal() -> Any:
    """Return the identity the authorizer resolved for this request.

    ``None`` when the request was allowed without an identity (no authorizer, a
    ``TokenAuthorizer`` / bool authorizer, ``Public``, or an in-process call).
    """
    return get_request_context().principal


def current_token() -> Optional[str]:
    """Return the caller's raw attachment decoded as a UTF-8 token, if any."""
    return get_request_context().token
