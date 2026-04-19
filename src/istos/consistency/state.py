import asyncio
import zenoh
from typing import Protocol, Any, runtime_checkable


@runtime_checkable
class StateFetcher(Protocol):
    """
    Client component that uses `get` to pull the 'last known state' 
    of a resource when a new module starts up.
    """
    async def fetch_last_state(self, key_expression: str) -> dict[str, Any]:
        """
        Pulls the last known state of a resource (e.g., 'status/robot1').
        Usually returns a mapping of keys to their last known values.
        """
        ...
        
    async def fetch_historical_data(self, key_expression: str, start_time: Any, end_time: Any) -> list[Any]:
        """
        Optional extension for fetching historical data if the underlying
        storage (like InfluxDB) supports time-series queries.
        """
        ...


class ZenohStateFetcher:
    """
    Fetches the 'last known state' or historical data asynchronously using Zenoh `get`.
    Wraps Zenoh's blocking get operations to prevent stalling the asyncio loop.
    """
    def __init__(self, session: zenoh.Session):
        self._session = session

    async def fetch_last_state(self, key_expression: str) -> dict[str, Any]:
        """
        Pulls state from Zenoh, offloading the blocking call to a thread pool.
        """
        # zenoh.Session.get is blocking, we await it via to_thread
        responses = await asyncio.to_thread(self._session.get, key_expression)
        
        results = {}
        for reply in responses:
            if reply.ok:
                key = str(reply.ok.key_expr)
                payload = bytes(reply.ok.payload).decode('utf-8')
                results[key] = payload
            else:
                error_msg = "Unknown error"
                if reply.err is not None and reply.err.payload:
                    error_msg = bytes(reply.err.payload).decode('utf-8')
                print(f"[ZenohStateFetcher] Error on key_expression '{key_expression}': {error_msg}")
                
        return results

    async def fetch_historical_data(self, key_expression: str, start_time: Any, end_time: Any) -> list[Any]:
        """
        An example of appending query parameters to the key expression for advanced Storage queries
        e.g., fetching from InfluxDB through a Queryable provider that supports them.
        """
        # Zenoh supports passing arguments, often formatted as query strings appended to the expression
        expr_with_args = f"{key_expression}?start={start_time}&end={end_time}"
        
        responses = await asyncio.to_thread(self._session.get, expr_with_args)
        
        history = []
        for reply in responses:
            if reply.ok:
                history.append({
                    "src": str(reply.ok.key_expr),
                    "payload": bytes(reply.ok.payload).decode('utf-8')
                })
                
        return history
