"""Security tests: authorization enforcement, pickle removal, insecure-default warnings."""

import pytest

from istos import (
    Istos,
    TokenAuthorizer,
    AuthContext,
    UnauthorizedError,
    Principal,
    Depends,
    current_principal,
    current_token,
)
from istos.security.authz import check_authorized
from istos.messages.serialization import JsonSerializer


# ---------------------------------------------------------------------------
# 1. Pickle serializer must not exist
# ---------------------------------------------------------------------------

def test_pickle_serializer_removed():
    import istos.messages.serialization as ser
    assert not hasattr(ser, "PickleSerializer")
    from istos import messages
    assert "PickleSerializer" not in messages.__all__


# ---------------------------------------------------------------------------
# 2. AuthContext / TokenAuthorizer / check_authorized
# ---------------------------------------------------------------------------

def test_auth_context_token_decoding():
    assert AuthContext(prefix="p", key_expr="p", attachment=b"tok").token == "tok"
    assert AuthContext(prefix="p", key_expr="p", attachment=None).token is None
    # non-utf8 bytes decode to None rather than raising
    assert AuthContext(prefix="p", key_expr="p", attachment=b"\xff\xfe").token is None


def test_token_authorizer_allows_and_denies():
    authz = TokenAuthorizer("secret")
    assert authz(AuthContext(prefix="p", key_expr="p", attachment=b"secret")) is True
    assert authz(AuthContext(prefix="p", key_expr="p", attachment=b"wrong")) is False
    assert authz(AuthContext(prefix="p", key_expr="p", attachment=None)) is False


def test_token_authorizer_multiple_tokens():
    authz = TokenAuthorizer({"a", "b"})
    assert authz(AuthContext(prefix="p", key_expr="p", attachment=b"a"))
    assert authz(AuthContext(prefix="p", key_expr="p", attachment=b"b"))
    assert not authz(AuthContext(prefix="p", key_expr="p", attachment=b"c"))


def test_token_authorizer_requires_tokens():
    with pytest.raises(ValueError):
        TokenAuthorizer(set())


@pytest.mark.asyncio
async def test_check_authorized_none_is_allow():
    # None authorizer never raises
    await check_authorized(None, AuthContext(prefix="p", key_expr="p"))


@pytest.mark.asyncio
async def test_check_authorized_denies():
    ctx = AuthContext(prefix="p", key_expr="p", attachment=b"nope")
    with pytest.raises(UnauthorizedError):
        await check_authorized(TokenAuthorizer("secret"), ctx)


@pytest.mark.asyncio
async def test_check_authorized_supports_async():
    async def deny(ctx):
        return False

    with pytest.raises(UnauthorizedError):
        await check_authorized(deny, AuthContext(prefix="p", key_expr="p"))


# ---------------------------------------------------------------------------
# 3. on_query enforces authorization at the network boundary
# ---------------------------------------------------------------------------

class _FakeSelector:
    def __init__(self, key, params=None):
        self.key_expr = key
        self.parameters = params or {}


class _FakeQuery:
    """Minimal stand-in for zenoh.Query used by handler_wrapper.on_query."""
    def __init__(self, key, params=None, attachment=None):
        self.selector = _FakeSelector(key, params)
        self.attachment = attachment
        self.replies = []

    def reply(self, key, payload):
        self.replies.append((key, payload))


def _handler_for(app, prefix):
    return next(h for h in app._handlers if h.prefix == prefix)


@pytest.mark.asyncio
async def test_on_query_denies_without_token():
    app = Istos(authorizer=TokenAuthorizer("secret"))

    @app.handle("admin/op")
    async def admin() -> dict:
        return {"ok": True}

    handler = _handler_for(app, "admin/op")
    q = _FakeQuery("admin/op", attachment=None)
    await handler.on_query(q)

    assert len(q.replies) == 1
    payload = JsonSerializer().deserialize(q.replies[0][1])
    assert payload["code"] == "unauthorized"
    # the handler body never ran
    assert handler.calls == 0


@pytest.mark.asyncio
async def test_on_query_allows_with_token():
    app = Istos(authorizer=TokenAuthorizer("secret"))

    @app.handle("admin/op")
    async def admin() -> dict:
        return {"ok": True}

    handler = _handler_for(app, "admin/op")
    q = _FakeQuery("admin/op", attachment=b"secret")
    await handler.on_query(q)

    assert len(q.replies) == 1
    payload = JsonSerializer().deserialize(q.replies[0][1])
    assert payload == {"ok": True}
    assert handler.calls == 1


@pytest.mark.asyncio
async def test_per_handler_authorizer_without_global():
    app = Istos()  # no global authorizer

    @app.handle("open/op")
    async def open_op() -> dict:
        return {"ok": True}

    @app.handle("locked/op", authorizer=TokenAuthorizer("secret"))
    async def locked_op() -> dict:
        return {"ok": True}

    open_h = _handler_for(app, "open/op")
    locked_h = _handler_for(app, "locked/op")

    q_open = _FakeQuery("open/op")
    await open_h.on_query(q_open)
    assert JsonSerializer().deserialize(q_open.replies[0][1]) == {"ok": True}

    q_locked = _FakeQuery("locked/op", attachment=None)
    await locked_h.on_query(q_locked)
    assert JsonSerializer().deserialize(q_locked.replies[0][1])["code"] == "unauthorized"


@pytest.mark.asyncio
async def test_no_authorizer_allows_all():
    app = Istos()

    @app.handle("free/op")
    async def free_op() -> dict:
        return {"ok": True}

    handler = _handler_for(app, "free/op")
    q = _FakeQuery("free/op")
    await handler.on_query(q)
    assert JsonSerializer().deserialize(q.replies[0][1]) == {"ok": True}


# ---------------------------------------------------------------------------
# 4. Layered authorization: app-wide AND per-handler both apply
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_layered_authorizer_requires_both():
    from istos import AuthContext as _AC

    app = Istos(authorizer=TokenAuthorizer("app-token"))

    def admins_only(ctx: _AC) -> bool:
        return ctx.params.get("role") == "admin"

    @app.handle("fleet/shutdown", authorizer=admins_only)
    async def shutdown(role: str = "user") -> dict:
        return {"ok": True}

    handler = _handler_for(app, "fleet/shutdown")

    # Valid app token AND admin role -> allowed.
    q_ok = _FakeQuery("fleet/shutdown", params={"role": "admin"}, attachment=b"app-token")
    await handler.on_query(q_ok)
    assert JsonSerializer().deserialize(q_ok.replies[0][1]) == {"ok": True}

    # Right role but missing app token -> denied by the app-wide layer.
    q_bad_token = _FakeQuery("fleet/shutdown", params={"role": "admin"}, attachment=None)
    await handler.on_query(q_bad_token)
    assert JsonSerializer().deserialize(q_bad_token.replies[0][1])["code"] == "unauthorized"

    # Valid app token but wrong role -> denied by the per-handler layer.
    q_bad_role = _FakeQuery("fleet/shutdown", params={"role": "user"}, attachment=b"app-token")
    await handler.on_query(q_bad_role)
    assert JsonSerializer().deserialize(q_bad_role.replies[0][1])["code"] == "unauthorized"


@pytest.mark.asyncio
async def test_public_opts_out_of_app_gate():
    from istos import Public

    app = Istos(authorizer=TokenAuthorizer("app-token"))

    @app.handle("health/ping", authorizer=Public)
    async def ping() -> dict:
        return {"pong": True}

    handler = _handler_for(app, "health/ping")

    # No token at all, yet reachable because the handler is explicitly Public.
    q = _FakeQuery("health/ping", attachment=None)
    await handler.on_query(q)
    assert JsonSerializer().deserialize(q.replies[0][1]) == {"pong": True}


@pytest.mark.asyncio
async def test_handler_inherits_app_authorizer_when_unset():
    app = Istos(authorizer=TokenAuthorizer("app-token"))

    @app.handle("fleet/status")  # no per-handler authorizer -> inherit app-wide
    async def status() -> dict:
        return {"ok": True}

    handler = _handler_for(app, "fleet/status")

    q_denied = _FakeQuery("fleet/status", attachment=None)
    await handler.on_query(q_denied)
    assert JsonSerializer().deserialize(q_denied.replies[0][1])["code"] == "unauthorized"

    q_ok = _FakeQuery("fleet/status", attachment=b"app-token")
    await handler.on_query(q_ok)
    assert JsonSerializer().deserialize(q_ok.replies[0][1]) == {"ok": True}


# ---------------------------------------------------------------------------
# 5. Principal: an authorizer may resolve an identity, injected via DI
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_authorized_returns_principal():
    who = Principal(id="alice", roles=frozenset({"admin"}))

    def authorize(ctx):
        return who

    resolved = await check_authorized(authorize, AuthContext(prefix="p", key_expr="p"))
    assert resolved is who


@pytest.mark.asyncio
async def test_check_authorized_bare_true_has_no_principal():
    # A plain bool allow carries no identity.
    assert await check_authorized(lambda ctx: True, AuthContext(prefix="p", key_expr="p")) is None
    assert await check_authorized(None, AuthContext(prefix="p", key_expr="p")) is None


@pytest.mark.asyncio
async def test_principal_injected_into_handler_body():
    def authorize(ctx: AuthContext) -> Principal:
        assert ctx.token == "app-token"  # gate still runs on the token
        return Principal(id="alice", roles=frozenset({"admin"}))

    app = Istos(authorizer=authorize)

    seen = {}

    @app.handle("fleet/status")
    async def status(user: Principal = Depends(current_principal),
                     token: str = Depends(current_token)) -> dict:
        seen["user"] = user
        seen["token"] = token
        return {"id": user.id, "admin": user.has_role("admin")}

    handler = _handler_for(app, "fleet/status")
    q = _FakeQuery("fleet/status", attachment=b"app-token")
    await handler.on_query(q)

    assert JsonSerializer().deserialize(q.replies[0][1]) == {"id": "alice", "admin": True}
    assert seen["user"].id == "alice"
    assert seen["token"] == "app-token"


@pytest.mark.asyncio
async def test_no_principal_when_bool_authorizer():
    app = Istos(authorizer=TokenAuthorizer("app-token"))

    @app.handle("fleet/status")
    async def status(user=Depends(current_principal)) -> dict:
        return {"user_is_none": user is None}

    handler = _handler_for(app, "fleet/status")
    q = _FakeQuery("fleet/status", attachment=b"app-token")
    await handler.on_query(q)
    assert JsonSerializer().deserialize(q.replies[0][1]) == {"user_is_none": True}


@pytest.mark.asyncio
async def test_layered_prefers_handler_principal():
    def app_authorize(ctx):
        return Principal(id="app-identity")

    def handler_authorize(ctx):
        return Principal(id="handler-identity")

    app = Istos(authorizer=app_authorize)

    @app.handle("fleet/op", authorizer=handler_authorize)
    async def op(user=Depends(current_principal)) -> dict:
        return {"id": user.id}

    handler = _handler_for(app, "fleet/op")
    q = _FakeQuery("fleet/op")
    await handler.on_query(q)
    # The more specific (per-handler) identity wins.
    assert JsonSerializer().deserialize(q.replies[0][1]) == {"id": "handler-identity"}


@pytest.mark.asyncio
async def test_layered_falls_back_to_app_principal():
    def app_authorize(ctx):
        return Principal(id="app-identity")

    def handler_authorize(ctx):
        return True  # allow, but resolves no identity

    app = Istos(authorizer=app_authorize)

    @app.handle("fleet/op", authorizer=handler_authorize)
    async def op(user=Depends(current_principal)) -> dict:
        return {"id": user.id}

    handler = _handler_for(app, "fleet/op")
    q = _FakeQuery("fleet/op")
    await handler.on_query(q)
    assert JsonSerializer().deserialize(q.replies[0][1]) == {"id": "app-identity"}


# ---------------------------------------------------------------------------
# 6. Authorization on @subscribe (pub/sub inbound edge)
# ---------------------------------------------------------------------------

class _FakeSample:
    """Minimal stand-in for zenoh.Sample used by subscribe_wrapper.on_sample."""
    def __init__(self, payload, attachment=None, key="events/in"):
        self.payload = payload
        self.attachment = attachment
        self.key_expr = key


@pytest.mark.asyncio
async def test_subscribe_denies_without_token():
    app = Istos(authorizer=TokenAuthorizer("app-token"))
    got = []

    @app.subscribe("events/in")
    async def on_evt(data) -> None:
        got.append(data)

    w = app._subscribers[0]
    payload = JsonSerializer().serialize({"x": 1})

    # No token -> the sample is dropped, the callback never runs.
    await w.on_sample(_FakeSample(payload, attachment=None))
    assert got == []

    # Correct token -> delivered.
    await w.on_sample(_FakeSample(payload, attachment=b"app-token"))
    assert got == [{"x": 1}]


@pytest.mark.asyncio
async def test_subscribe_injects_principal():
    def authn(ctx: AuthContext):
        return Principal(id="pub-1") if ctx.token == "app-token" else None

    app = Istos(authorizer=authn)
    seen = {}

    @app.subscribe("events/in")
    async def on_evt(data, user: Principal = Depends(current_principal)) -> None:
        seen["user"] = user
        seen["data"] = data

    w = app._subscribers[0]
    payload = JsonSerializer().serialize({"x": 2})
    await w.on_sample(_FakeSample(payload, attachment=b"app-token"))

    assert seen["data"] == {"x": 2}
    assert seen["user"].id == "pub-1"


@pytest.mark.asyncio
async def test_subscribe_public_opts_out():
    from istos import Public

    app = Istos(authorizer=TokenAuthorizer("app-token"))
    got = []

    @app.subscribe("events/pub", authorizer=Public)
    async def on_evt(data) -> None:
        got.append(data)

    w = app._subscribers[0]
    payload = JsonSerializer().serialize({"y": 1})
    await w.on_sample(_FakeSample(payload, attachment=None, key="events/pub"))
    assert got == [{"y": 1}]


# ---------------------------------------------------------------------------
# 7. IstosRouter propagates authorizer to the app
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_router_handle_authorizer():
    from istos import IstosRouter

    app = Istos()
    router = IstosRouter(prefix="v1")

    @router.handle("op", authorizer=TokenAuthorizer("r-token"))
    async def op() -> dict:
        return {"ok": True}

    app.include_router(router)
    handler = _handler_for(app, "v1/op")

    q_denied = _FakeQuery("v1/op", attachment=None)
    await handler.on_query(q_denied)
    assert JsonSerializer().deserialize(q_denied.replies[0][1])["code"] == "unauthorized"

    q_ok = _FakeQuery("v1/op", attachment=b"r-token")
    await handler.on_query(q_ok)
    assert JsonSerializer().deserialize(q_ok.replies[0][1]) == {"ok": True}
