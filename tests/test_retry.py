import pytest
from unittest.mock import MagicMock
from istos.core.retry import RetryPolicy, execute_with_retry


# ------------------------------------------------------------------
# RetryPolicy unit tests
# ------------------------------------------------------------------

class TestRetryPolicy:
    def test_from_int(self):
        policy = RetryPolicy.from_int(5)
        assert policy.max_retries == 5
        assert policy.delay == 0.5
        assert policy.backoff_factor == 2.0

    def test_default_no_retry(self):
        policy = RetryPolicy()
        assert policy.max_retries == 0


# ------------------------------------------------------------------
# execute_with_retry unit tests
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_succeeds_immediately():
    """If the function succeeds on first try, no retries happen."""
    call_count = 0

    async def always_works():
        nonlocal call_count
        call_count += 1
        return "ok"

    policy = RetryPolicy(max_retries=3, delay=0.01)
    result = await execute_with_retry(always_works, policy)
    
    assert result == "ok"
    assert call_count == 1


@pytest.mark.asyncio
async def test_retry_succeeds_after_failures():
    """Function fails twice, then succeeds on 3rd attempt."""
    call_count = 0

    async def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("network down")
        return "recovered"

    policy = RetryPolicy(max_retries=5, delay=0.01)
    result = await execute_with_retry(flaky, policy)

    assert result == "recovered"
    assert call_count == 3


@pytest.mark.asyncio
async def test_retry_exhausted_raises():
    """If all retries fail, the last exception is re-raised."""
    async def always_fails():
        raise TimeoutError("timed out")

    policy = RetryPolicy(max_retries=2, delay=0.01)
    
    with pytest.raises(TimeoutError, match="timed out"):
        await execute_with_retry(always_fails, policy)


@pytest.mark.asyncio
async def test_retry_on_failure_callback():
    """If on_failure is set, it is called instead of raising."""
    captured = []

    def failure_handler(exc):
        captured.append(exc)

    async def always_fails():
        raise ValueError("bad data")

    policy = RetryPolicy(max_retries=1, delay=0.01, on_failure=failure_handler)
    await execute_with_retry(always_fails, policy)

    assert len(captured) == 1
    assert isinstance(captured[0], ValueError)


# ------------------------------------------------------------------
# @istos.query with retry=
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_query_decorator_retry_parameter(istos, mocker):
    """Test that @istos.query(retry=3) stores the correct retry policy."""
    @istos.query("weather", retry=3)
    def get_weather(result):
        return result

    wrapper = istos._queries[0]
    assert wrapper.retry_policy.max_retries == 3


@pytest.mark.asyncio
async def test_query_decorator_retry_policy_object(istos, mocker):
    """Test that @istos.query(retry=RetryPolicy(...)) works."""
    custom = RetryPolicy(max_retries=10, delay=1.0, backoff_factor=3.0)

    @istos.query("weather", retry=custom)
    def get_weather(result):
        return result

    wrapper = istos._queries[0]
    assert wrapper.retry_policy.max_retries == 10
    assert wrapper.retry_policy.delay == 1.0
    assert wrapper.retry_policy.backoff_factor == 3.0


@pytest.mark.asyncio
async def test_query_no_retry_by_default(istos):
    """Test that @istos.query without retry has 0 retries."""
    @istos.query("weather")
    def get_weather(result):
        return result

    wrapper = istos._queries[0]
    assert wrapper.retry_policy.max_retries == 0


# ------------------------------------------------------------------
# @istos.subscribe with retry=
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_subscribe_decorator_retry_parameter(istos):
    """Test that @istos.subscribe(retry=3) stores the correct retry policy."""
    @istos.subscribe("sensor/data", retry=3)
    def on_data(data):
        pass

    wrapper = istos._subscribers[0]
    assert wrapper.retry_policy.max_retries == 3


@pytest.mark.asyncio
async def test_subscribe_on_sample_retries_on_failure(istos):
    """Test that a flaky subscriber function is retried."""
    call_count = 0
    received = []

    @istos.subscribe("sensor/data", retry=3)
    def on_data(data):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("transient failure")
        received.append(data)

    wrapper = istos._subscribers[0]

    fake_sample = MagicMock()
    fake_sample.payload = istos._serializer.serialize({"temp": 42})

    await wrapper.on_sample(fake_sample)

    assert call_count == 3
    assert received == [{"temp": 42}]
