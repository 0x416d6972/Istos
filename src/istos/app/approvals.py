"""Human-in-the-loop: the node's approval gate and its two fabric keys."""

import warnings
from typing import TYPE_CHECKING, Any, Optional

from istos.app._base import IstosBase
from istos.errors import IstosSecurityWarning
from istos.security.authz import Authorizer

if TYPE_CHECKING:
    from istos.agent.approval import ApprovalGate


class _ApprovalMixin(IstosBase):
    """Human approval for irreversible tools: ``app.approvals()``."""

    def approvals(
        self,
        *,
        timeout_s: Optional[float] = 300.0,
        authorizer: Optional[Authorizer] = None,
        on_request: Optional[Any] = None,
    ) -> "ApprovalGate":
        """This node's approval gate, serving pending requests on the fabric.

            gate = app.approvals(timeout_s=600, authorizer=require_roles("ops"))

            @app.channel("agent/chat", durable=True)
            async def chat(s: ChannelSession):
                await drive_channel(s, model, tools, approvals=gate)

        Pass it to :func:`~istos.agent.run_agent` / ``drive_channel`` and a tool
        marked ``approval=True`` stops there until a human decides. Two keys are
        registered, both unique to this process so a wildcard reaches every
        waiting node: ``.istos/approvals/<service>-<node>`` lists what is pending
        and ``…/decide`` settles one. From the approver's side use
        :func:`~istos.agent.approval.list_approvals` and
        :func:`~istos.agent.approval.decide_approval`, which fan out over those
        keys, or put the HTTP gateway in front for a browser.

        Requests are written through to the app's storage, so with Redis or
        SQLAlchemy an operator can still see what was outstanding after a restart
        (the agent that was waiting, however, is gone — that request is stale and
        expires).

        Calling this more than once returns the same gate. ``timeout_s=None``
        waits indefinitely; otherwise an undecided request expires and the tool
        does not run. **Deciding is privileged** — anyone who can reach the decide
        key can authorize an irreversible action — so pass ``authorizer`` (it
        layers on top of the app-wide one) in production.
        """
        from istos.agent.approval import ApprovalGate, register_approval_handlers

        if self._approval_gate is not None:
            assert isinstance(self._approval_gate, ApprovalGate)
            return self._approval_gate

        gate = ApprovalGate(
            storage=self._storage,
            timeout_s=timeout_s,
            service_name=self._service_name,
            on_request=on_request,
        )
        if authorizer is None and self._authorizer is None:
            warnings.warn(
                f"{gate.decide_key} is reachable by any peer with no "
                "authorization: anything on the fabric could approve a tool call "
                "a human was meant to gate. Pass app.approvals(authorizer=...) "
                "or Istos(authorizer=...).",
                IstosSecurityWarning,
                stacklevel=2,
            )
        register_approval_handlers(self, gate, authorizer=authorizer)
        self._approval_gate = gate
        return gate
