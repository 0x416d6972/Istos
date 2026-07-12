"""JWT authentication + role-based authorization (RBAC) batteries.

These layer on the existing authorizer gate: JWTAuthorizer authenticates a token
into a Principal; require_roles authorizes against that principal's roles.
"""

import time

import pytest

from istos import (
    AuthContext,
    ForbiddenError,
    JWTAuthorizer,
    Principal,
    UnauthorizedError,
    require_roles,
)
from istos.core.authz import check_authorized, combine_authorizers

jwt = pytest.importorskip("jwt")  # PyJWT (istos[jwt])

SECRET = "test-secret-that-is-long-enough-for-hs256-hmac"


def _token(payload: dict, secret: str = SECRET, algorithm: str = "HS256") -> str:
    return jwt.encode(payload, secret, algorithm=algorithm)


def _ctx(token: str | None = None, prefix: str = "admin/op") -> AuthContext:
    attachment = token.encode() if token is not None else None
    return AuthContext(prefix=prefix, key_expr=prefix, attachment=attachment)


# ---------------------------------------------------------------------------
# 1. JWTAuthorizer
# ---------------------------------------------------------------------------
def test_jwt_valid_token_returns_principal():
    authz = JWTAuthorizer(secret=SECRET)
    tok = _token({"sub": "user-1", "roles": ["admin", "pilot"], "exp": time.time() + 60})
    result = authz(_ctx(tok))
    assert isinstance(result, Principal)
    assert result.id == "user-1"
    assert result.roles == frozenset({"admin", "pilot"})
    assert result.claims["sub"] == "user-1"


def test_jwt_missing_token_denies():
    assert JWTAuthorizer(secret=SECRET)(_ctx(None)) is False


def test_jwt_bad_signature_denies():
    tok = _token({"sub": "u", "exp": time.time() + 60},
                 secret="a-different-secret-also-long-enough-for-hmac")
    assert JWTAuthorizer(secret=SECRET)(_ctx(tok)) is False


def test_jwt_expired_denies():
    tok = _token({"sub": "u", "exp": time.time() - 10})
    assert JWTAuthorizer(secret=SECRET)(_ctx(tok)) is False


def test_jwt_requires_exp_by_default():
    tok = _token({"sub": "u"})  # no exp
    assert JWTAuthorizer(secret=SECRET)(_ctx(tok)) is False
    # ...but can be relaxed:
    assert isinstance(JWTAuthorizer(secret=SECRET, require_exp=False)(_ctx(tok)), Principal)


def test_jwt_audience_and_issuer_enforced():
    authz = JWTAuthorizer(secret=SECRET, audience="my-api", issuer="my-idp")
    good = _token({"sub": "u", "aud": "my-api", "iss": "my-idp", "exp": time.time() + 60})
    assert isinstance(authz(_ctx(good)), Principal)
    wrong_aud = _token({"sub": "u", "aud": "other", "iss": "my-idp", "exp": time.time() + 60})
    assert authz(_ctx(wrong_aud)) is False


def test_jwt_roles_from_scope_string():
    authz = JWTAuthorizer(secret=SECRET, roles_claim="scope")
    tok = _token({"sub": "u", "scope": "read write admin", "exp": time.time() + 60})
    assert authz(_ctx(tok)).roles == frozenset({"read", "write", "admin"})


def test_jwt_custom_id_claim():
    authz = JWTAuthorizer(secret=SECRET, id_claim="email")
    tok = _token({"email": "a@b.co", "exp": time.time() + 60})
    assert authz(_ctx(tok)).id == "a@b.co"


def test_jwt_rejects_none_algorithm():
    with pytest.raises(ValueError, match="none"):
        JWTAuthorizer(secret=SECRET, algorithms=["none"])


def test_jwt_requires_key():
    with pytest.raises(ValueError, match="secret.*public_key|public_key"):
        JWTAuthorizer()


# ---------------------------------------------------------------------------
# 2. require_roles (layered on a resolved principal)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_require_roles_all_pass():
    ctx = _ctx()
    ctx.principal = Principal(id="u", roles=frozenset({"admin", "pilot"}))
    assert await require_roles("admin")(ctx) is ctx.principal


@pytest.mark.asyncio
async def test_require_roles_missing_role_forbidden():
    ctx = _ctx()
    ctx.principal = Principal(id="u", roles=frozenset({"pilot"}))
    with pytest.raises(ForbiddenError):
        await require_roles("admin")(ctx)


@pytest.mark.asyncio
async def test_require_roles_no_identity_unauthorized():
    with pytest.raises(UnauthorizedError):
        await require_roles("admin")(_ctx())  # ctx.principal is None


@pytest.mark.asyncio
async def test_require_roles_any_mode():
    ctx = _ctx()
    ctx.principal = Principal(id="u", roles=frozenset({"viewer"}))
    assert await require_roles("admin", "viewer", mode="any")(ctx)
    ctx.principal = Principal(id="u", roles=frozenset({"nobody"}))
    with pytest.raises(ForbiddenError):
        await require_roles("admin", "viewer", mode="any")(ctx)


def test_require_roles_rejects_bad_mode():
    with pytest.raises(ValueError):
        require_roles("admin", mode="most")


@pytest.mark.asyncio
async def test_require_roles_with_inline_authenticator():
    authz = require_roles("admin", authenticator=JWTAuthorizer(secret=SECRET))
    tok = _token({"sub": "u", "roles": ["admin"], "exp": time.time() + 60})
    result = await authz(_ctx(tok))
    assert isinstance(result, Principal) and result.id == "u"
    # Wrong role → forbidden even with a valid token.
    bad = _token({"sub": "u", "roles": ["viewer"], "exp": time.time() + 60})
    with pytest.raises(ForbiddenError):
        await authz(_ctx(bad))


# ---------------------------------------------------------------------------
# 3. Layered: app-wide JWT authenticator + per-handler require_roles
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_layered_jwt_plus_require_roles():
    combined = combine_authorizers(JWTAuthorizer(secret=SECRET), require_roles("admin"))
    admin_tok = _token({"sub": "boss", "roles": ["admin"], "exp": time.time() + 60})
    principal = await check_authorized(combined, _ctx(admin_tok))
    assert isinstance(principal, Principal) and principal.id == "boss"

    # Authenticated but not admin → 403.
    user_tok = _token({"sub": "peon", "roles": ["viewer"], "exp": time.time() + 60})
    with pytest.raises(ForbiddenError):
        await check_authorized(combined, _ctx(user_tok))

    # No token → 401.
    with pytest.raises(UnauthorizedError):
        await check_authorized(combined, _ctx(None))
