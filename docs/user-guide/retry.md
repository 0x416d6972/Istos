# Retry Policies

Istos provides built-in retry mechanisms with exponential backoff for fault-tolerant distributed systems.

## Quick Usage

Add automatic retries with a simple integer:

```python
from istos import Istos

istos = Istos()

# Retry up to 5 times with default exponential backoff
@istos.query("weather/forecast", retry=5)
def get_forecast(result):
    return result

# Subscriber with retries — if processing fails, it retries
@istos.subscribe("sensor/readings", retry=3)
def on_reading(data):
    save_to_database(data)
```

## Advanced Configuration

For fine-grained control, use the `RetryPolicy` class:

```python
from istos.retry import RetryPolicy

@istos.query("weather/forecast", retry=RetryPolicy(
    max_retries=10,
    delay=1.0,           # Initial delay in seconds
    backoff_factor=3.0,  # Multiply delay by this factor each retry
    on_failure=lambda e: print(f"Dead letter: {e}")
))
def get_forecast(result):
    return result
```

### RetryPolicy Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_retries` | `int` | `3` | Maximum number of retry attempts |
| `delay` | `float` | `1.0` | Initial delay between retries (seconds) |
| `backoff_factor` | `float` | `2.0` | Multiplier applied to delay after each retry |
| `on_failure` | `Callable` | `None` | Callback invoked when all retries are exhausted |

### Backoff Timeline Example

With `delay=1.0` and `backoff_factor=2.0`:

```
Attempt 1: immediate
Attempt 2: wait 1.0s
Attempt 3: wait 2.0s
Attempt 4: wait 4.0s
Attempt 5: wait 8.0s
→ on_failure() called
```

## Dead Letter Handling

The `on_failure` callback lets you handle permanently failed operations:

```python
def handle_dead_letter(error):
    """Called when all retries are exhausted."""
    log.error(f"Operation permanently failed: {error}")
    alert_ops_team(error)

@istos.query("critical/service", retry=RetryPolicy(
    max_retries=5,
    on_failure=handle_dead_letter
))
def critical_query(result):
    return result
```

!!! tip "When to Use Retries"
    Retries are ideal for **transient failures** — network blips, temporary unavailability, or race conditions during startup. For permanent errors (bad data, missing endpoints), retries won't help.

## Next Steps

- [Handlers & Queries (RPC)](rpc.md)
- [Publish & Subscribe](pubsub.md)
- [API: Retry](../api/retry.md)
