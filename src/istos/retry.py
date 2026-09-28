import asyncio
import random
from typing import Any, Callable, Optional
from dataclasses import dataclass

from istos.errors import is_retryable
from istos.logging import get_logger

_logger = get_logger("retry")


@dataclass
class RetryPolicy:
    """
    Configures retry behavior for any Istos decorator.
    Uses exponential backoff: delay * (2 ** attempt).
    """
    max_retries: int = 0
    delay: float = 0.5
    backoff_factor: float = 2.0
    #: Cap on a single wait. Without it, delay * factor**attempt grows without
    #: bound (delay=0.5, factor=2, 40 retries is millions of days).
    max_delay: float = 60.0
    #: Fractional jitter applied to each wait so retriers don't wake together.
    #: 0.1 spreads the delay by ±10%. 0 disables it.
    jitter: float = 0.1
    on_failure: Optional[Callable[..., Any]] = None

    @classmethod
    def from_int(cls, value: int) -> "RetryPolicy":
        """Shorthand: retry=5 becomes RetryPolicy(max_retries=5)."""
        return cls(max_retries=value)


async def execute_with_retry(
    func: Callable[..., Any],
    policy: RetryPolicy,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Executes a callable with retry logic and exponential backoff.
    If all retries are exhausted and on_failure is set, it is called
    with the last exception. The exception is then re-raised either way, so a
    dead-letter hook cannot turn a failed call into a successful ``None``.

    Errors that asking again cannot fix (``not_found``, ``unauthorized``; see
    :func:`istos.errors.is_retryable`) fail on the first attempt.
    """
    last_exception: Optional[Exception] = None

    for attempt in range(policy.max_retries + 1):
        try:
            result = func(*args, **kwargs)
            # If it's a coroutine, await it
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as e:
            last_exception = e
            if not is_retryable(e):
                break
            if attempt < policy.max_retries:
                wait = policy.delay * (policy.backoff_factor ** attempt)
                if policy.max_delay > 0:
                    wait = min(wait, policy.max_delay)
                if policy.jitter:
                    wait *= 1.0 + random.uniform(-policy.jitter, policy.jitter)
                wait = max(0.0, wait)
                _logger.warning(
                    "Attempt %d/%d failed: %s. Retrying in %.2fs...",
                    attempt + 1, policy.max_retries, e, wait,
                    extra={"attempt": attempt + 1, "max_retries": policy.max_retries},
                )
                await asyncio.sleep(wait)

    # All retries exhausted. The callback is a hook (dead-letter, metric);
    # the failure still propagates so the caller — and an exactly-once ledger —
    # cannot record it as a successful None.
    if policy.on_failure is not None:
        policy.on_failure(last_exception)
    raise last_exception  # type: ignore
