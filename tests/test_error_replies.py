"""A responder that raised must raise on the caller's side too.

A handler that raises replies with an ErrorResponse payload, an ordinary dict on
the wire. A caller that does not check it reads the failure as data:

    reply = await app.query_once("clients/list")
    reply.get("clients") or []      # [] for an error envelope, so an outage
                                    # reads as "there are no clients"
"""

import asyncio

import pytest

from istos import (
    ERROR_MARKER,
    ErrorResponse,
    ForbiddenError,
    Istos,
    IstosError,
    NotFoundError,
    RateLimitError,
    UnauthorizedError,
    error_from_payload,
    is_error_payload,
    is_retryable,
    reply_err,
)


def _bg(app: Istos):
    return asyncio.create_task(app.run_async())


async def _stop(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# 1. Recognising the envelope
# ---------------------------------------------------------------------------
def test_recognises_an_error_envelope():
    assert is_error_payload({"error": "not_found", "code": "not_found", "message": "gone"})


@pytest.mark.parametrize(
    "payload",
    [
        {"clients": []},                       # an ordinary reply
        {"error": "boom"},                     # no code, no message
        {"code": "not_found", "message": "x"},  # no error
        "just a string",
        None,
        [{"error": "x", "code": "y", "message": "z"}],  # a list of replies
    ],
)
def test_leaves_ordinary_replies_alone(payload):
    assert not is_error_payload(payload)


def test_a_reply_that_merely_has_an_error_key_is_not_an_envelope():
    """A handler may return a field named `error`; only all three fields together
    mean the responder failed."""
    assert not is_error_payload({"error": None, "rows": 3})


def test_the_error_response_stamps_the_discriminator():
    assert ErrorResponse(error="x", code="x", message="m").to_dict()[ERROR_MARKER] is True


def test_reply_err_builds_a_detectable_envelope():
    payload = reply_err("boom", code="internal_error")
    assert is_error_payload(payload)
    assert error_from_payload(payload).message == "boom"


def test_the_discriminator_is_authoritative_over_shape():
    """A success value that legitimately carries error/code/message reads as data
    once it stamps the discriminator false — the false-positive escape hatch."""
    success = {ERROR_MARKER: False, "error": "none", "code": "OK", "message": "all good"}
    assert not is_error_payload(success)


def test_the_legacy_shape_still_works_without_the_marker():
    """An older responder or another-language client sends no discriminator; the
    three-key rule still recognises its error."""
    assert is_error_payload({"error": "x", "code": "y", "message": "z"})


# ---------------------------------------------------------------------------
# 2. Rebuilding the exception
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "code, expected",
    [
        ("not_found", NotFoundError),
        ("unauthorized", UnauthorizedError),
        ("forbidden", ForbiddenError),
    ],
)
def test_the_code_round_trips_back_to_its_class(code, expected):
    exc = error_from_payload({"error": code, "code": code, "message": "no"})
    assert isinstance(exc, expected)
    assert exc.code == code


def test_an_unknown_code_keeps_its_code():
    exc = error_from_payload({"error": "quota", "code": "quota_exceeded", "message": "no"})
    assert type(exc) is IstosError
    assert exc.code == "quota_exceeded"


def test_the_correlation_id_survives_the_hop():
    exc = error_from_payload(
        {"error": "x", "code": "internal_error", "message": "no", "correlation_id": "abc-123"}
    )
    assert exc.correlation_id == "abc-123"


# ---------------------------------------------------------------------------
# 3. Integration: the same failure through every door
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_once_raises_what_the_handler_raised():
    app = Istos(enable_health=False, enable_metrics=False)

    @app.handle("istos/test/err/missing")
    async def missing():
        raise NotFoundError("no such client")

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)
        with pytest.raises(NotFoundError) as caught:
            await app.query_once("istos/test/err/missing", timeout_s=5)
        assert "no such client" in str(caught.value)
    finally:
        await _stop(task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_decorator_never_sees_the_envelope():
    """The decorated function must never be handed an error payload."""
    app = Istos(enable_health=False, enable_metrics=False)
    seen = []

    @app.handle("istos/test/err/down")
    async def down():
        raise IstosError("StarRocks is unreachable")

    @app.query("istos/test/err/down")
    async def ask(reply):
        seen.append(reply)
        return reply

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)
        with pytest.raises(IstosError):
            await ask()
        assert seen == []
    finally:
        await _stop(task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_an_ordinary_reply_still_arrives_as_data():
    app = Istos(enable_health=False, enable_metrics=False)

    @app.handle("istos/test/err/ok")
    async def ok():
        return {"clients": ["acme"]}

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)
        assert await app.query_once("istos/test/err/ok", timeout_s=5) == {"clients": ["acme"]}
    finally:
        await _stop(task)


# ---------------------------------------------------------------------------
# 4. Queue calls must not read a refusal as a fact either
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_refused_queue_call_raises_rather_than_reporting_nothing():
    """A refusal comes back as an envelope, so a caller reading the field it wants
    off it finds nothing: `result` reports "unknown", `dead_letters` reports none."""
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    app.queue("istos/test/refuse", authorizer=lambda ctx: ctx.token == "good")

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)

        with pytest.raises(UnauthorizedError):
            await app.enqueue("istos/test/refuse", {"x": 1}, token="bad")
        with pytest.raises(UnauthorizedError):
            await app.result("istos/test/refuse", "job-1", token="bad")
        with pytest.raises(UnauthorizedError):
            await app.dead_letters("istos/test/refuse", token="bad")

        job_id = await app.enqueue("istos/test/refuse", {"x": 1}, token="good")
        assert job_id
        assert await app.dead_letters("istos/test/refuse", token="good") == []
    finally:
        await _stop(task)


# ---------------------------------------------------------------------------
# 5. Retrying an answer that will not change
# ---------------------------------------------------------------------------
def test_a_refusal_is_not_retryable():
    """A refusal comes back the same however often it is asked."""
    assert not is_retryable(NotFoundError("gone"))
    assert not is_retryable(UnauthorizedError("bad token"))
    assert not is_retryable(ForbiddenError("not yours"))


def test_a_transient_failure_still_retries():
    assert is_retryable(IstosError("StarRocks is unreachable"))  # 500
    assert is_retryable(TimeoutError("no answer"))
    assert is_retryable(RateLimitError("slow down"))  # waiting is the remedy


@pytest.mark.asyncio
async def test_retry_stops_on_a_refusal_and_keeps_going_on_a_fault():
    from istos.retry import RetryPolicy, execute_with_retry

    calls = []

    async def refuses():
        calls.append("x")
        raise NotFoundError("no such client")

    with pytest.raises(NotFoundError):
        await execute_with_retry(refuses, RetryPolicy(max_retries=3, delay=0.01))
    assert len(calls) == 1, "a not_found must not be asked three more times"

    calls.clear()

    async def faults():
        calls.append("x")
        raise IstosError("StarRocks is unreachable")

    with pytest.raises(IstosError):
        await execute_with_retry(faults, RetryPolicy(max_retries=2, delay=0.01))
    assert len(calls) == 3, "a transient fault still gets its retries"


def test_a_rebuilt_error_knows_its_status():
    """The status is not on the wire but decides retryability, so a code without a
    dedicated class must still rebuild non-retryable."""
    exc = error_from_payload(
        {"error": "validation_error", "code": "validation_error", "message": "bad param"}
    )
    assert exc.status == 400
    assert not is_retryable(exc)


def test_correlation_id_is_a_real_field():
    assert IstosError("x").correlation_id is None
    assert NotFoundError("x", correlation_id="abc").correlation_id == "abc"
