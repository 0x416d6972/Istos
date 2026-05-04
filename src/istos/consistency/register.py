import asyncio
import zenoh
from abc import ABC, abstractmethod
from typing import Protocol, Any, Optional
from istos.consistency.storage import StoragePlugin

class AbstractRegistery(ABC):
    """
    Abstraction class for registering prefixes.
    """
    def __init__(self, prefix: str, storage: StoragePlugin):
        self._prefix = prefix
        self._storage = storage

    @abstractmethod
    async def register(self, client: Any) -> None:
        """
        Registers the queryable with the client so it can start listenining
        to questions on `self.prefix`.
        """
        ...

    @abstractmethod  
    async def unregister(self) -> None:
        """
        Unregister so stops listening for queries
        """
        ...
    
    @abstractmethod
    async def on_query(self, query: Any) -> None:
        """
        Callback triggered by Zenoh when someone queries `self.prefix`.
        Implementation should:
          1. Extract the specific key from the query
          2. Fetch the state using `await self._storage.get(key)`
          3. Send the reply back via the Zenoh query object
        """
        ...



class PrefixRegistery(AbstractRegistery):
    """
    Registers the Queryable with the Zenoh session. 
    Hooks the Zenoh callback into the running asyncio loop.
    """
    def __init__(self, prefix: str, storage: StoragePlugin):
        super().__init__(prefix, storage)
        self._queryable: Optional[zenoh.Queryable] = None
        self._session: Optional[zenoh.Session] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
    async def on_query(self, query: zenoh.Query) -> None:
        """
        Callback triggered by Zenoh when someone queries `self._prefix`.
        """
        try:
            key = str(query.selector.key_expr)
            
            # Extract parameters safely, converting zenoh mapping to standard dict
            params = {}
            if hasattr(query.selector, "parameters") and query.selector.parameters:
                params = dict(query.selector.parameters)

            # Look for the internal representation if applicable, 
            # currently just passing it to storage, but storage doesn't take params natively yet.
            # A true implementation will pass params to the decorated function.
            # Since PrefixRegistery just wraps StoragePlugin now, we can pass parameters
            # by looking for a specific dict structure or just ignoring them if storage doesn't support them.
            # But the real power is when @handle gets them!
            value = await self._storage.get(key)
            
            # (In the future, if value is a function, we could invoke it with **params)
            if value is not None:
                payload = str(value).encode('utf-8')
                query.reply(key, payload)
        except Exception as e:
            print(f"[PrefixRegistery] Error: {e}")

    async def register(self, client: zenoh.Session) -> None:
        """
        Registers the Queryable with the backend client. 
        Hooks the Zenoh callback into the running asyncio loop.
        """
        self._session = client
        self._loop = asyncio.get_running_loop()
        
        def _sync_callback(query: zenoh.Query):
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(self.on_query(query), self._loop)

        self._queryable = self._session.declare_queryable(
            self._prefix, 
            _sync_callback, 
            complete=True
        )

    async def unregister(self) -> None:
        if self._queryable is not None:
            self._queryable.undeclare()
            self._queryable = None