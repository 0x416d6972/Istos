"""HTTP → Zenoh ingress gateway + K8s probes + /metrics.

Unit tests cover the pure gateway logic (spec parsing, param/selector encoding,
status mapping); the integration test boots a real Istos app with an HTTP surface
and drives it over HTTP end to end, proving auth forwarding and the probes.
"""

import asyncio

import pytest

from istos import Istos
from istos.http.gateway import (
    HttpRoute,
    build_selector,
    decode_params,
    encode_params,
    extract_bearer,
    is_error_payload,
    parse_http_spec,
    status_for_reply,
)


# ---------------------------------------------------------------------------
# 1. Spec parsing
# ---------------------------------------------------------------------------
def test_parse_http_spec_true_defaults_to_post_prefix():
    r = parse_http_spec(True, "robot/move")
    assert (r.method, r.path, r.key_expr) == ("POST", "/robot/move", "robot/move")


def test_parse_http_spec_method_and_path():
    r = parse_http_spec("GET /things", "catalog/list")
    assert (r.method, r.path, r.key_expr) == ("GET", "/things", "catalog/list")


def test_parse_http_spec_bare_path_defaults_post():
    r = parse_http_spec("/custom", "k")
    assert (r.method, r.path) == ("POST", "/custom")


def test_parse_http_spec_adds_leading_slash():
    assert parse_http_spec("GET things", "k").path == "/things"


def test_parse_http_spec_rejects_garbage():
    with pytest.raises(ValueError):
        parse_http_spec("GET /a /b", "k")
    with pytest.raises(ValueError):
        parse_http_spec(123, "k")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 2. Params / selector / auth
# ---------------------------------------------------------------------------
def test_extract_bearer_variants():
    assert extract_bearer("Bearer abc123") == "abc123"
    assert extract_bearer("bearer xyz") == "xyz"
    assert extract_bearer("rawtoken") == "rawtoken"
    assert extract_bearer(None) is None
    assert extract_bearer("") is None


def test_encode_params_scalars_and_nested():
    out = encode_params({"n": 5, "f": 1.5, "b": True, "s": "hi", "skip": None,
                         "obj": {"a": 1}})
    assert out == {"n": "5", "f": "1.5", "b": "true", "s": "hi",
                   "obj": '{"a": 1}'}  # None dropped, nested JSON-encoded


def test_build_selector():
    assert build_selector("robot/move", {}) == "robot/move"
    sel = build_selector("robot/move", {"distance": 5})
    assert sel == "robot/move?distance=5"


def test_param_encode_decode_roundtrip_with_special_chars():
    # Zenoh does not percent-decode selector params, so the server must. Values
    # with spaces / reserved chars must survive encode (client) → decode (server).
    import zenoh
    params = {"prompt": "hello brave new world", "q": "a=b;c"}
    sel = build_selector("llm/gen", params)
    raw = dict(zenoh.Selector(sel).parameters)   # still percent-encoded
    assert decode_params(raw) == {"prompt": "hello brave new world", "q": "a=b;c"}


def test_build_selector_multi_param_uses_semicolon():
    # Zenoh splits selector params on ';', NOT '&' (which it reads as part of the
    # value). Multi-param requests must use ';'.
    sel = build_selector("robot/move", {"distance": 5, "speed": "fast"})
    assert sel == "robot/move?distance=5;speed=fast"
    import zenoh
    assert dict(zenoh.Selector(sel).parameters) == {"distance": "5", "speed": "fast"}


# ---------------------------------------------------------------------------
# 3. Reply → HTTP status mapping
# ---------------------------------------------------------------------------
def test_status_for_reply_success():
    assert status_for_reply({"status": "ok"}) == 200
    assert status_for_reply([1, 2, 3]) == 200


def test_status_for_reply_maps_error_codes():
    assert status_for_reply(
        {"error": "unauthorized", "code": "unauthorized", "message": "no"}) == 401
    assert status_for_reply(
        {"error": "validation_error", "code": "validation_error", "message": "x"}) == 400
    assert status_for_reply(
        {"error": "not_found", "code": "not_found", "message": "x"}) == 404
    # Unknown error code -> 500
    assert status_for_reply(
        {"error": "boom", "code": "weird", "message": "x"}) == 500


def test_is_error_payload_requires_all_fields():
    assert is_error_payload({"error": "e", "code": "c", "message": "m"})
    assert not is_error_payload({"error": "e"})           # partial → treated as data
    assert not is_error_payload({"code": "ok", "value": 1})


# ---------------------------------------------------------------------------
# 4. Route registration on the app
# ---------------------------------------------------------------------------
def test_handle_http_registers_route(istos: Istos):
    @istos.handle("robot/move", http=True)
    async def move(distance: int):
        return {"moved": distance}

    assert istos._http_routes == [HttpRoute("POST", "/robot/move", "robot/move")]


def test_handle_without_http_registers_no_route(istos: Istos):
    @istos.handle("robot/move")
    async def move(distance: int):
        return {"moved": distance}

    assert istos._http_routes == []


# ---------------------------------------------------------------------------
# 5. Integration: full HTTP surface over a running app
# ---------------------------------------------------------------------------
def _free_port() -> int:
    """A hardcoded port collides with whatever else is running on the machine."""
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_http_surface_end_to_end():
    """Boot an app with an HTTP surface and drive probes, /metrics, and the
    gateway (with auth forwarding) over real HTTP."""
    import aiohttp

    from istos import AuthContext, Principal

    def authorize(ctx: AuthContext) -> Principal | None:
        return Principal(id="svc-1") if ctx.token == "good-token" else None

    port = _free_port()
    app = Istos(http_port=port, authorizer=authorize,
                enable_health=False, enable_metrics=False)

    @app.handle("robot/move", http=True, authorizer=None)
    async def move(distance: int):
        return {"moved": distance}

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.5)  # let the session + HTTP server come up
        base = f"http://localhost:{port}"
        async with aiohttp.ClientSession() as http:
            # Probes
            async with http.get(f"{base}/livez") as r:
                assert r.status == 200
                assert (await r.json())["status"] == "alive"
            async with http.get(f"{base}/readyz") as r:
                assert r.status == 200
            async with http.get(f"{base}/metrics") as r:
                assert r.status == 200

            # Gateway WITHOUT a token → gate denies → 401
            async with http.post(f"{base}/robot/move", json={"distance": 5}) as r:
                assert r.status == 401

            # Gateway WITH a valid token → handler runs, auth forwarded
            async with http.post(
                f"{base}/robot/move", json={"distance": 5},
                headers={"Authorization": "Bearer good-token"},
            ) as r:
                assert r.status == 200
                assert await r.json() == {"moved": 5}
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
