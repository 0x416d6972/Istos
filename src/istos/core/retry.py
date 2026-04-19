import asyncio
from typing import Any, Callable, Optional
from dataclasses import dataclass


@dataclass
class RetryPolicy:
    """
    Configures retry behavior for any Istos decorator.
    Uses exponential backoff: delay * (2 ** attempt).
    """
    max_retries: int = 0
    delay: float = 0.5
    backoff_factor: float = 2.0
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
    with the last exception. Otherwise the exception is re-raised.
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
            if attempt < policy.max_retries:
                wait = policy.delay * (policy.backoff_factor ** attempt)
                print(
                    f"[Istos Retry] Attempt {attempt + 1}/{policy.max_retries} "
                    f"failed: {e}. Retrying in {wait:.2f}s..."
                )
                await asyncio.sleep(wait)

    # All retries exhausted
    if policy.on_failure is not None:
        policy.on_failure(last_exception)
    else:
        raise last_exception  # type: ignore
