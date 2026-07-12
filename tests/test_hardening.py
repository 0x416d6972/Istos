"""P0 hardening: attachment/auth symmetry on @query & publish_once, and the
fail-closed require_auth mode."""

import asyncio

import pytest

from istos import Istos, IstosSecurityError, TokenAuthorizer


# ---------------------------------------------------------------------------
# 1. Fail-closed auth
# ---------------------------------------------------------------------------
def test_require_auth_without_authorizer_raises():
    with pytest.raises(IstosSecurityError, match="require_auth"):
        Istos(require_auth=True)


def test_require_auth_with_authorizer_ok():
    app = Istos(
        require_auth=True, authorizer=TokenAuthorizer("k"),
        enable_health=False, enable_metrics=False, enable_discovery=False,
    )
    assert app._authorizer is not None


def test_require_auth_defaults_off():
    # Back-compat: unauthenticated construction still works (with a warning path).
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    assert app is not None


# ---------------------------------------------------------------------------
# 2. @query carries an attachment (symmetry with query_once)
# ---------------------------------------------------------------------------
def test_query_decorator_accepts_attachment(istos: Istos):
    @istos.query("admin/op", attachment="tok")
    def op(result):
        return result

    assert istos._queries[0]._attachment == b"tok"


# ---------------------------------------------------------------------------
# 3. Integration: publish_once token reaches a gated subscriber
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_once_token_reaches_gated_subscriber():
    received = []
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)

    @app.subscribe("secure/topic", authorizer=lambda ctx: ctx.token == "k")
    async def on_msg(data):
        received.append(data)

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.2)
        await app.publish_once("secure/topic", {"x": 1}, attachment="k")  # allowed
        await app.publish_once("secure/topic", {"x": 2})                  # no token → dropped
        await asyncio.sleep(0.5)
        assert received == [{"x": 1}]
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
