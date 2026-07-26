"""Human-in-the-loop approval for irreversible tools.

A mesh tool that refunds money or deletes a bucket should not run because a model
felt like it. Mark it ``approval=True`` and the loop stops before the call: the
request is written to durable storage, announced on the fabric, and the tool only
runs once a human decides.

Nothing polls. The waiting agent holds an ``asyncio.Event``; the decision arrives
as a query on this node's ``.istos/approvals/<service>-<node>/decide`` key and
wakes it. Two keys per gate, both carrying a node-unique chunk so a wildcard
reaches every waiting node rather than one of them::

    .istos/approvals/<service>-<node>          → pending requests
    .istos/approvals/<service>-<node>/decide   → approve or deny one

Operator side, from any node on the mesh::

    pending = await list_approvals(app)
    await decide_approval(app, pending[0]["id"], approved=True, by="amir")

Fail-closed throughout: a request that is denied, that expires, or that outlives
``timeout_s`` never runs the tool. A tool marked ``approval=True`` with no gate
configured is a startup error, not a silent pass.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from istos.discovery.naming import key_chunk
from istos.errors import IstosError, NotFoundError, is_error_payload
from istos.logging import get_logger

_logger = get_logger("agent.approval")

APPROVALS_KEY = ".istos/approvals"
APPROVALS_WILDCARD = ".istos/approvals/*"
DECIDE_WILDCARD = ".istos/approvals/*/decide"


class ApprovalState(str, Enum):
    PENDING = "pending"    # filed, waiting on a human
    APPROVED = "approved"  # cleared to run
    DENIED = "denied"      # a human said no
    EXPIRED = "expired"    # nobody decided in time


class ApprovalDenied(IstosError):
    """A human refused the tool call."""

    def __init__(self, message: str = "Tool call denied", **kwargs: Any):
        super().__init__(message, code="approval_denied", status=403, **kwargs)


class ApprovalTimeout(IstosError):
    """Nobody decided before the deadline, so the call did not happen."""

    def __init__(self, message: str = "Approval timed out", **kwargs: Any):
        super().__init__(message, code="approval_timeout", status=504, **kwargs)


@dataclass
class ApprovalRequest:
    """One pending (or settled) request for a human decision."""

    id: str
    tool: str
    arguments: Dict[str, Any] = field(default_factory=dict)
    prefix: Optional[str] = None
    state: ApprovalState = ApprovalState.PENDING
    requested_at: float = field(default_factory=time.time)
    expires_at: float = 0.0            # 0 → no deadline
    reason: Optional[str] = None       # why this tool needs a human
    requester: Optional[str] = None    # which service asked
    conversation_id: Optional[str] = None
    decided_by: Optional[str] = None
    decided_at: float = 0.0
    note: Optional[str] = None         # the human's comment, shown to the model

    def to_dict(self) -> dict:
        d = asdict(self)
        d["state"] = self.state.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "ApprovalRequest":
        data = dict(d)
        data["state"] = ApprovalState(data.get("state", "pending"))
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})

    @property
    def is_expired(self) -> bool:
        return self.expires_at > 0 and time.time() >= self.expires_at


class ApprovalGate:
    """Pending approvals for one node: durable state plus the waiters.

    Built by :meth:`Istos.approvals`, which also registers the two fabric keys.
    Construct one directly only when driving it yourself (a test, or an agent
    that is not an Istos app).

    State is written through to the app's ``StoragePlugin``, so with Redis or
    SQLAlchemy a restart can still list and settle what was outstanding; with the
    in-memory default the pending set dies with the process. The waiter itself is
    always in-memory — an agent that has crashed is no longer waiting, and its
    recovered request is stale by definition.
    """

    def __init__(
        self,
        *,
        storage: Any = None,
        timeout_s: Optional[float] = 300.0,
        service_name: str = "istos",
        node_id: Optional[str] = None,
        on_request: Optional[Callable[[ApprovalRequest], Union[None, Awaitable[None]]]] = None,
    ) -> None:
        self._storage = storage
        self.timeout_s = timeout_s
        self._service = service_name
        # A chunk of its own per process: replicas of one service would otherwise
        # share a key, and `@handle` answers a shared key from exactly one of them.
        self._node = node_id or uuid.uuid4().hex[:8]
        # Called with each new request — notify a dashboard, page someone. Public
        # so it can be set after construction (a notifier often needs the gate).
        self.on_request = on_request
        self._requests: Dict[str, ApprovalRequest] = {}
        self._waiters: Dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self._loaded = False

    # --- fabric keys ---

    @property
    def key(self) -> str:
        """Where this node lists its pending requests."""
        return f"{APPROVALS_KEY}/{key_chunk(self._service)}-{self._node}"

    @property
    def decide_key(self) -> str:
        """Where this node accepts decisions."""
        return f"{self.key}/decide"

    # --- persistence ---

    def _index_key(self) -> str:
        return f"approvals:{key_chunk(self._service)}:index"

    def _req_key(self, request_id: str) -> str:
        return f"approvals:{key_chunk(self._service)}:req:{request_id}"

    async def _write(self, req: ApprovalRequest) -> None:
        if self._storage is None:
            return
        try:
            await self._storage.put(self._req_key(req.id), req.to_dict())
            pending = sorted(
                r.id for r in self._requests.values() if r.state == ApprovalState.PENDING
            )
            await self._storage.put(self._index_key(), pending)
        except Exception:
            _logger.exception("Could not persist approval %s", req.id)

    async def load(self) -> None:
        """Recover requests still pending from a previous run. Runs once."""
        if self._loaded:
            return
        self._loaded = True
        if self._storage is None:
            return
        try:
            for request_id in await self._storage.get(self._index_key()) or []:
                raw = await self._storage.get(self._req_key(request_id))
                if isinstance(raw, dict) and request_id not in self._requests:
                    self._requests[request_id] = ApprovalRequest.from_dict(raw)
        except Exception:  # recovery is best-effort — never block the gate
            _logger.exception("Could not recover pending approvals")

    # --- filing and settling ---

    async def request(
        self,
        tool: str,
        arguments: Optional[Dict[str, Any]] = None,
        *,
        prefix: Optional[str] = None,
        reason: Optional[str] = None,
        requester: Optional[str] = None,
        conversation_id: Optional[str] = None,
        timeout_s: Optional[float] = -1.0,
    ) -> ApprovalRequest:
        """File a request and return it, without waiting. ``timeout_s`` defaults
        to the gate's; pass ``None`` for no deadline."""
        await self.load()
        ttl = self.timeout_s if timeout_s == -1.0 else timeout_s
        req = ApprovalRequest(
            id=uuid.uuid4().hex,
            tool=tool,
            arguments=dict(arguments or {}),
            prefix=prefix,
            reason=reason,
            requester=requester or self._service,
            conversation_id=conversation_id,
            expires_at=time.time() + ttl if ttl else 0.0,
        )
        async with self._lock:
            self._requests[req.id] = req
            self._waiters[req.id] = asyncio.Event()
        await self._write(req)
        _logger.info(
            "Approval %s pending for tool %s", req.id, tool,
            extra={"approval_id": req.id, "tool": tool, "conversation_id": conversation_id},
        )
        if self.on_request is not None:
            try:
                result = self.on_request(req)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:  # a broken notifier must not strand the request
                _logger.exception("approval on_request callback failed")
        return req

    async def wait(
        self, request_id: str, *, timeout_s: Optional[float] = -1.0,
    ) -> ApprovalRequest:
        """Block until the request is settled.

        Returns the approved request (its ``arguments`` are what to run — an
        approver may have edited them). Raises :class:`ApprovalDenied` on a no,
        :class:`ApprovalTimeout` when the deadline passes, and
        :class:`NotFoundError` for an unknown id.
        """
        async with self._lock:
            req = self._requests.get(request_id)
            event = self._waiters.get(request_id)
        if req is None:
            raise NotFoundError(f"No approval request {request_id!r}")
        if req.state != ApprovalState.PENDING:
            return self._settled(req)

        # The gate's own deadline and the caller's, whichever comes first.
        budget = self.timeout_s if timeout_s == -1.0 else timeout_s
        if req.expires_at:
            remaining = req.expires_at - time.time()
            budget = remaining if budget is None else min(budget, remaining)
        if event is None:  # recovered from storage: nobody in this process waits
            raise ApprovalTimeout(
                f"Approval {request_id} was filed by an earlier run of this node",
                details={"id": request_id},
            )
        try:
            if budget is not None and budget <= 0:
                raise asyncio.TimeoutError
            await asyncio.wait_for(event.wait(), timeout=budget)
        except asyncio.TimeoutError:
            await self._expire(request_id)
            raise ApprovalTimeout(
                f"Nobody decided approval {request_id} in time; {req.tool} did not run",
                details={"id": request_id, "tool": req.tool},
            ) from None
        finally:
            async with self._lock:
                self._waiters.pop(request_id, None)
        async with self._lock:
            settled = self._requests.get(request_id, req)
        return self._settled(settled)

    def _settled(self, req: ApprovalRequest) -> ApprovalRequest:
        if req.state == ApprovalState.APPROVED:
            return req
        if req.state == ApprovalState.DENIED:
            raise ApprovalDenied(
                f"{req.tool} was denied" + (f": {req.note}" if req.note else ""),
                details={"id": req.id, "tool": req.tool, "by": req.decided_by},
            )
        raise ApprovalTimeout(
            f"Approval {req.id} expired; {req.tool} did not run",
            details={"id": req.id, "tool": req.tool},
        )

    async def decide(
        self,
        request_id: str,
        *,
        approved: bool,
        by: Optional[str] = None,
        note: Optional[str] = None,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> Optional[ApprovalRequest]:
        """Settle a request. Returns it, or ``None`` when this gate never had it
        (so a fan-out decide can ask every node and only the holder acts).

        ``arguments`` replaces what the model proposed — an approver may correct a
        value rather than reject the call outright. First decision wins; a second
        one returns the already-settled request untouched.
        """
        await self.load()
        async with self._lock:
            req = self._requests.get(request_id)
            if req is None:
                return None
            if req.state != ApprovalState.PENDING:
                return req
            if req.is_expired:
                req.state = ApprovalState.EXPIRED
            else:
                req.state = ApprovalState.APPROVED if approved else ApprovalState.DENIED
                if approved and arguments is not None:
                    req.arguments = dict(arguments)
            req.decided_by = by
            req.note = note
            req.decided_at = time.time()
            event = self._waiters.get(request_id)
        await self._write(req)
        _logger.info(
            "Approval %s %s by %s", req.id, req.state.value, by or "?",
            extra={"approval_id": req.id, "tool": req.tool, "state": req.state.value},
        )
        if event is not None:
            event.set()
        return req

    async def approve(self, request_id: str, **kwargs: Any) -> Optional[ApprovalRequest]:
        return await self.decide(request_id, approved=True, **kwargs)

    async def deny(self, request_id: str, **kwargs: Any) -> Optional[ApprovalRequest]:
        return await self.decide(request_id, approved=False, **kwargs)

    async def _expire(self, request_id: str) -> None:
        async with self._lock:
            req = self._requests.get(request_id)
            if req is None or req.state != ApprovalState.PENDING:
                return
            req.state = ApprovalState.EXPIRED
            req.decided_at = time.time()
        await self._write(req)

    # --- reads ---

    async def get(self, request_id: str) -> Optional[ApprovalRequest]:
        await self.load()
        async with self._lock:
            return self._requests.get(request_id)

    async def pending(self) -> List[dict]:
        """Every request still awaiting a decision, oldest first.

        Requests past their deadline are settled as ``expired`` here rather than
        being offered to an operator who can no longer affect the outcome.
        """
        await self.load()
        async with self._lock:
            stale = [
                r for r in self._requests.values()
                if r.state == ApprovalState.PENDING and r.is_expired
            ]
            live = [
                r.to_dict() for r in self._requests.values()
                if r.state == ApprovalState.PENDING and not r.is_expired
            ]
        for req in stale:
            await self._expire(req.id)
        live.sort(key=lambda r: r["requested_at"])
        return live

    async def gate(
        self,
        tool: str,
        arguments: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """File a request, wait for it, and return the arguments cleared to run.

        Raises :class:`ApprovalDenied` or :class:`ApprovalTimeout` otherwise, so
        a caller that gets a return value can act::

            args = await gate.gate("billing-refund", {"order_id": "o1"})
            await tool.call(args)
        """
        req = await self.request(tool, arguments, **kwargs)
        return (await self.wait(req.id)).arguments


def register_approval_handlers(app: Any, gate: ApprovalGate, **handle_kwargs: Any) -> None:
    """Serve ``gate`` on the fabric: pending requests, and a decide endpoint.

    Called by :meth:`Istos.approvals`. Extra keyword arguments (``authorizer=``)
    go to ``@app.handle`` — deciding an approval is a privileged operation, so
    protect it in production.
    """

    @app.handle(gate.key, **handle_kwargs)
    async def _pending() -> dict:
        """Approval requests this node is waiting on."""
        return {"service": app._service_name, "pending": await gate.pending()}

    @app.handle(gate.decide_key, **handle_kwargs)
    async def _decide(
        request_id: str,
        approved: bool = False,
        by: Optional[str] = None,
        note: Optional[str] = None,
    ) -> dict:
        """Approve or deny one request. ``matched`` is false if it is not ours."""
        # Selector params arrive as text and the yes is the dangerous direction,
        # so anything that is not an explicit affirmative denies.
        if not isinstance(approved, bool):
            approved = str(approved).strip().lower() in ("true", "1", "yes", "y")
        req = await gate.decide(request_id, approved=approved, by=by, note=note)
        if req is None:
            return {"matched": False}
        return {"matched": True, "request": req.to_dict()}


async def list_approvals(app: Any, *, timeout_s: float = 3.0, **query_kwargs: Any) -> List[dict]:
    """Every pending approval on the fabric, from any node.

    Asks ``.istos/approvals/*``: one key per waiting node, so all of them answer::

        for req in await list_approvals(app):
            print(req["id"], req["tool"], req["arguments"])
    """
    replies = await app.query_once(
        APPROVALS_WILDCARD, timeout_s=timeout_s, consolidate_replies=False, **query_kwargs
    )
    if replies is None:
        return []
    if not isinstance(replies, list):
        replies = [replies]
    out: List[dict] = []
    for reply in replies:
        if not isinstance(reply, dict) or is_error_payload(reply):
            continue
        for req in reply.get("pending") or []:
            if isinstance(req, dict):
                out.append({**req, "service": reply.get("service")})
    out.sort(key=lambda r: r.get("requested_at") or 0)
    return out


async def decide_approval(
    app: Any,
    request_id: str,
    *,
    approved: bool,
    by: Optional[str] = None,
    note: Optional[str] = None,
    timeout_s: float = 3.0,
    **query_kwargs: Any,
) -> dict:
    """Settle a request from anywhere on the mesh, without knowing which node holds it.

    Asks every ``.istos/approvals/*/decide`` key and returns the settled request
    from the one node that had it::

        await decide_approval(app, req["id"], approved=False, by="amir",
                              note="wrong order")

    Raises :class:`~istos.errors.NotFoundError` when no node recognised the id —
    it was already settled, it expired, or the waiting node is gone.
    """
    replies = await app.query_once(
        DECIDE_WILDCARD,
        request_id=request_id, approved=approved, by=by, note=note,
        timeout_s=timeout_s, consolidate_replies=False, **query_kwargs,
    )
    if replies is None:
        replies = []
    if not isinstance(replies, list):
        replies = [replies]
    for reply in replies:
        if isinstance(reply, dict) and reply.get("matched"):
            request: dict = reply.get("request") or {}
            return request
    raise NotFoundError(
        f"No node is holding approval {request_id!r}",
        details={"id": request_id},
    )
