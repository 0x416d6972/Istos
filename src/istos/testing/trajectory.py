"""Record an agent run, replay it in CI.

An agent's behaviour depends on a model you do not control, so the usual test
shapes do not fit: a live model makes the suite slow, flaky, and expensive, and a
hand-written fake drifts from what the model actually does.

So record one real run — every completion the model returned and every tool
result that came back off the fabric — and keep it as a file. Replaying pins the
model and the mesh to what was recorded, leaving your own code as the only
variable: the prompt, the tool catalogue, the loop, the approval gates. If a
refactor changes which tools get called or what the agent finally says, the
replay says so, offline and in milliseconds.

Recording::

    traj, events = await record_agent(
        model, tools, [], name="refund-flow", user="refund order o-1",
    )
    traj.save("trajectories/refund-flow.json")

Replaying (no model, no mesh, no network)::

    result = await replay(Trajectory.load("trajectories/refund-flow.json"))
    assert result.ok, result.diff

What a replay cannot tell you is whether the model would still answer that way —
that is what an eval against a live model is for (see :mod:`istos.testing.evals`).
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from istos.agent.loop import AgentEvent, run_agent
from istos.agent.model import Model, ModelReply, ToolCall
from istos.agent.tools import MeshTool, tool_name

TRAJECTORY_VERSION = 1


@dataclass
class ModelTurn:
    """One completion the model produced, as it came back."""

    content: Optional[str] = None
    tool_calls: List[dict] = field(default_factory=list)  # {id, name, arguments}
    model: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[dict] = None
    tools_offered: List[str] = field(default_factory=list)  # catalogue at this turn

    @classmethod
    def from_reply(cls, reply: ModelReply, tools_offered: Sequence[str] = ()) -> "ModelTurn":
        return cls(
            content=reply.content,
            tool_calls=[
                {"id": tc.id, "name": tc.name, "arguments": tc.arguments}
                for tc in reply.tool_calls
            ],
            model=reply.model,
            finish_reason=reply.finish_reason,
            usage=reply.usage,
            tools_offered=list(tools_offered),
        )

    def to_reply(self) -> ModelReply:
        return ModelReply(
            content=self.content,
            tool_calls=[
                ToolCall(
                    id=str(tc.get("id") or ""),
                    name=str(tc.get("name") or ""),
                    arguments=dict(tc.get("arguments") or {}),
                )
                for tc in self.tool_calls
            ],
            model=self.model,
            finish_reason=self.finish_reason,
            usage=self.usage,
        )


@dataclass
class ToolOutcome:
    """One tool call and what it returned."""

    name: str
    arguments: Dict[str, Any] = field(default_factory=dict)
    content: str = ""
    error: bool = False
    tool_call_id: Optional[str] = None


@dataclass
class Trajectory:
    """One recorded agent run: what was asked, what the model did, what came back."""

    name: str = ""
    system: Optional[str] = None
    user_turns: List[str] = field(default_factory=list)
    model_turns: List[ModelTurn] = field(default_factory=list)
    tools: List[ToolOutcome] = field(default_factory=list)
    events: List[dict] = field(default_factory=list)
    approvals: List[dict] = field(default_factory=list)
    service: Optional[str] = None
    recorded_at: float = field(default_factory=time.time)
    version: int = TRAJECTORY_VERSION

    # --- derived views, the ones assertions are usually about ---

    @property
    def tool_names(self) -> List[str]:
        """Tool names in call order."""
        return [t.name for t in self.tools]

    @property
    def final_message(self) -> Optional[str]:
        for event in reversed(self.events):
            if event.get("kind") == "message":
                content = event.get("content")
                return None if content is None else str(content)
        return None

    # --- serialization ---

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "name": self.name,
            "system": self.system,
            "service": self.service,
            "recorded_at": self.recorded_at,
            "user_turns": list(self.user_turns),
            "model_turns": [asdict(t) for t in self.model_turns],
            "tools": [asdict(t) for t in self.tools],
            "events": list(self.events),
            "approvals": list(self.approvals),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Trajectory":
        version = int(d.get("version") or 1)
        if version > TRAJECTORY_VERSION:
            raise ValueError(
                f"Trajectory format v{version} is newer than this Istos understands "
                f"(v{TRAJECTORY_VERSION})"
            )
        return cls(
            name=d.get("name") or "",
            system=d.get("system"),
            user_turns=list(d.get("user_turns") or []),
            model_turns=[ModelTurn(**t) for t in d.get("model_turns") or []],
            tools=[ToolOutcome(**t) for t in d.get("tools") or []],
            events=list(d.get("events") or []),
            approvals=list(d.get("approvals") or []),
            service=d.get("service"),
            recorded_at=d.get("recorded_at") or 0.0,
            version=version,
        )

    def save(self, path: Union[str, Path]) -> Path:
        """Write the trajectory as JSON, creating parent directories."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.to_dict(), indent=2, default=str))
        return target

    @classmethod
    def load(cls, path: Union[str, Path]) -> "Trajectory":
        traj = cls.from_dict(json.loads(Path(path).read_text()))
        if not traj.name:
            traj.name = Path(path).stem
        return traj


def event_to_dict(event: AgentEvent) -> dict:
    """An :class:`AgentEvent` as a plain dict, dropping what was never set."""
    out = {
        "kind": event.kind,
        "content": event.content,
        "name": event.name,
        "arguments": event.arguments,
        "tool_call_id": event.tool_call_id,
        "error": event.error,
    }
    if event.approval_id is not None:
        out["approval_id"] = event.approval_id
    return {k: v for k, v in out.items() if v is not None or k == "content"}


class RecordingModel:
    """Wraps a :class:`~istos.agent.Model` and keeps every reply it produced."""

    def __init__(self, inner: Model) -> None:
        self._inner = inner
        self.turns: List[ModelTurn] = []

    async def complete(
        self, messages: List[dict], *, tools: Optional[List[dict]] = None,
    ) -> ModelReply:
        reply = await self._inner.complete(messages, tools=tools)
        offered = [
            t.get("function", {}).get("name", "") for t in tools or []
        ]
        self.turns.append(ModelTurn.from_reply(reply, offered))
        return reply


class TrajectoryRecorder:
    """Collects a run into a :class:`Trajectory`.

    Use it directly when you drive the loop yourself (inside a ``@channel``, say)
    rather than through :func:`record_agent`::

        rec = TrajectoryRecorder(name="refund-flow", system=SYSTEM)
        model = rec.model(OpenAIChatModel(...))
        rec.user("refund order o-1")
        async for event in run_agent(model, tools, messages):
            rec.observe(event)
            ...
        rec.build().save("trajectories/refund-flow.json")
    """

    def __init__(
        self,
        *,
        name: str = "",
        system: Optional[str] = None,
        service: Optional[str] = None,
    ) -> None:
        self.name = name
        self.system = system
        self.service = service
        self._model: Optional[RecordingModel] = None
        self._events: List[dict] = []
        self._tools: List[ToolOutcome] = []
        self._approvals: List[dict] = []
        self._users: List[str] = []
        self._pending: Dict[str, dict] = {}   # tool_call_id → the call, awaiting its result

    def model(self, inner: Model) -> RecordingModel:
        """Wrap the model so its replies are recorded."""
        self._model = RecordingModel(inner)
        return self._model

    def user(self, text: str) -> None:
        self._users.append(text)

    def observe(self, event: AgentEvent) -> None:
        """Feed one loop event."""
        self._events.append(event_to_dict(event))
        if event.kind == "tool_call":
            self._pending[str(event.tool_call_id)] = {
                "name": event.name or "",
                "arguments": dict(event.arguments or {}),
            }
        elif event.kind == "tool_result":
            call = self._pending.pop(str(event.tool_call_id), None)
            self._tools.append(ToolOutcome(
                name=event.name or (call or {}).get("name", ""),
                arguments=(call or {}).get("arguments", {}),
                content="" if event.content is None else str(event.content),
                error=event.error,
                tool_call_id=event.tool_call_id,
            ))
        elif event.kind in ("approval_request", "approval_decision"):
            self._approvals.append({
                "kind": event.kind,
                "id": event.approval_id,
                "tool": event.name,
                "arguments": event.arguments,
                "content": event.content,
                "error": event.error,
            })

    def build(self) -> Trajectory:
        return Trajectory(
            name=self.name,
            system=self.system,
            service=self.service,
            user_turns=list(self._users),
            model_turns=list(self._model.turns) if self._model is not None else [],
            tools=list(self._tools),
            events=list(self._events),
            approvals=list(self._approvals),
        )


async def record_agent(
    model: Model,
    tools: Sequence[MeshTool],
    messages: List[dict],
    *,
    name: str = "",
    user: Optional[Union[str, Sequence[str]]] = None,
    system: Optional[str] = None,
    **run_kwargs: Any,
) -> Tuple[Trajectory, List[AgentEvent]]:
    """Run the agent once and record it.

    ``user`` is appended to ``messages`` as user turns (a string, or several run
    one after another) and ``system`` is prepended when ``messages`` is empty, so
    the common case is a single call. Everything else goes to
    :func:`~istos.agent.run_agent`.
    """
    rec = TrajectoryRecorder(name=name, system=system)
    recording = rec.model(model)

    if system and not any(m.get("role") == "system" for m in messages):
        messages.insert(0, {"role": "system", "content": system})
    turns = [user] if isinstance(user, str) else list(user or [])

    events: List[AgentEvent] = []
    # No user turn at all still runs one pass, for messages built by the caller.
    for text in turns or [""]:
        if text:
            rec.user(text)
            messages.append({"role": "user", "content": text})
        async for event in run_agent(recording, tools, messages, **run_kwargs):
            rec.observe(event)
            events.append(event)
    return rec.build(), events


class ReplayExhausted(RuntimeError):
    """The loop asked for more completions than were recorded."""


class ReplayModel:
    """Returns the recorded completions, in order. No network, no model.

    ``tools_offered`` records the catalogue each recorded turn was given, so a
    replay can notice that a tool disappeared from the agent's catalogue even when
    the model never called it.
    """

    def __init__(self, turns: Union[Trajectory, Sequence[ModelTurn]]) -> None:
        self.turns = list(turns.model_turns if isinstance(turns, Trajectory) else turns)
        self.offered: List[List[str]] = []
        self._next = 0

    async def complete(
        self, messages: List[dict], *, tools: Optional[List[dict]] = None,
    ) -> ModelReply:
        self.offered.append([t.get("function", {}).get("name", "") for t in tools or []])
        if self._next >= len(self.turns):
            raise ReplayExhausted(
                f"The loop asked for completion {self._next + 1} but only "
                f"{len(self.turns)} were recorded"
            )
        turn = self.turns[self._next]
        self._next += 1
        return turn.to_reply()

    @property
    def used(self) -> int:
        return self._next


def replay_tools(traj: Trajectory) -> List[MeshTool]:
    """Stub tools that hand back what the recording captured.

    One tool per recorded name. A repeated call replays that tool's outcomes in
    order; calls beyond what was recorded return a marker the replay reports as a
    mismatch rather than pretending to succeed.
    """
    queued: Dict[str, List[ToolOutcome]] = {}
    for outcome in traj.tools:
        queued.setdefault(outcome.name, []).append(outcome)

    # A tool the recording shows was gated stays gated, so the replay exercises
    # the same path rather than quietly skipping it.
    gated = {
        a.get("tool") for a in traj.approvals if a.get("kind") == "approval_request"
    }

    def _make(name: str) -> MeshTool:
        remaining = list(queued[name])

        async def _canned(**_: Any) -> str:
            if not remaining:
                return f"__istos_replay_unrecorded__: {name}"
            return remaining.pop(0).content

        # A recorded name is already a tool name, so keep it verbatim.
        return MeshTool(
            name, name=name, invoke=_canned, description=f"replay of {name}",
            approval=name in gated,
        )

    return [_make(name) for name in queued]


def replay_gate(traj: Trajectory) -> Any:
    """A gate that answers the way the recorded human did, in the same order.

    Replay is about the code, not the operator: a run that was approved replays
    approved and one that was refused replays refused, without anyone watching.
    """
    from istos.agent.approval import ApprovalGate

    decisions = [a for a in traj.approvals if a.get("kind") == "approval_decision"]
    queue = [not bool(a.get("error")) for a in decisions]
    gate = ApprovalGate(timeout_s=5.0)

    async def _decide(req: Any) -> None:
        approved = queue.pop(0) if queue else True
        await gate.decide(req.id, approved=approved, by="replay")

    gate.on_request = _decide
    return gate


def check_against_app(traj: Trajectory, app: Any) -> List[str]:
    """Check a recording against a live app's registry.

    A replay cannot see this: the stubs answer whatever the recording said, so a
    handler that was renamed or whose signature changed still "works". Ask the app
    directly instead — do these tools still exist, and do the recorded arguments
    still satisfy their current signatures?

        assert not check_against_app(Trajectory.load(path), app)

    Returns one string per problem; empty means the recording still matches the
    code. This is what ``istos eval --app`` runs.
    """
    from istos.validation import SchemaValidationError, validate_params

    handlers = {tool_name(h.prefix): h for h in getattr(app, "_handlers", [])}
    problems: List[str] = []
    for outcome in traj.tools:
        handler = handlers.get(outcome.name)
        if handler is None:
            problems.append(f"{outcome.name} is no longer a handler on this app")
            continue
        try:
            validate_params(
                handler.func, dict(outcome.arguments),
                skip_params=getattr(handler, "_injected_params", None),
            )
        except SchemaValidationError as exc:
            problems.append(
                f"{outcome.name} no longer accepts the recorded arguments "
                f"{outcome.arguments}: {exc}"
            )
    return problems


@dataclass
class ReplayResult:
    """What a replay produced, and how it differed from the recording."""

    trajectory: Trajectory
    replayed: Trajectory
    diff: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.diff

    @property
    def tool_names(self) -> List[str]:
        return self.replayed.tool_names

    @property
    def final_message(self) -> Optional[str]:
        return self.replayed.final_message


async def replay(
    traj: Trajectory,
    *,
    tools: Optional[Sequence[MeshTool]] = None,
    approvals: Any = None,
    compare_text: bool = True,
    **run_kwargs: Any,
) -> ReplayResult:
    """Re-run a recorded trajectory against the current code.

    The model is pinned to the recorded completions and, by default, tools hand
    back the recorded results — so a difference means *your* code changed: the
    catalogue, the loop, an approval gate. Pass ``tools`` to replay against real
    :class:`~istos.agent.MeshTool` objects instead (their ``invoke`` still runs,
    which is useful for pure local tools and wrong for anything with effects).

    ``diff`` lists what moved: tool calls made or skipped, arguments changed, the
    final message, unrecorded calls, an exhausted recording. ``ok`` is
    ``diff == []``.

    A recording that went through an approval gate replays through one too: unless
    you pass ``approvals``, a stand-in answers the way the recorded human did.
    """
    model = ReplayModel(traj)
    catalog = list(tools) if tools is not None else replay_tools(traj)
    if approvals is None and any(t.requires_approval for t in catalog):
        approvals = replay_gate(traj)

    rec = TrajectoryRecorder(name=traj.name, system=traj.system)
    messages: List[dict] = []
    if traj.system:
        messages.append({"role": "system", "content": traj.system})

    diff: List[str] = []
    try:
        for text in traj.user_turns or [""]:
            if text:
                rec.user(text)
                messages.append({"role": "user", "content": text})
            async for event in run_agent(
                model, catalog, messages, approvals=approvals, **run_kwargs
            ):
                rec.observe(event)
    except ReplayExhausted as exc:
        diff.append(str(exc))
    except Exception as exc:  # a loop that now raises is the most important diff
        diff.append(f"replay raised {type(exc).__name__}: {exc}")

    replayed = rec.build()
    replayed.model_turns = list(traj.model_turns)
    diff.extend(_diff(traj, replayed, model, compare_text=compare_text))
    return ReplayResult(trajectory=traj, replayed=replayed, diff=diff)


def _diff(
    expected: Trajectory,
    actual: Trajectory,
    model: ReplayModel,
    *,
    compare_text: bool,
) -> List[str]:
    out: List[str] = []

    if expected.tool_names != actual.tool_names:
        out.append(
            f"tool calls changed: recorded {expected.tool_names}, "
            f"replayed {actual.tool_names}"
        )
    for i, (want, got) in enumerate(zip(expected.tools, actual.tools)):
        if want.name == got.name and want.arguments != got.arguments:
            out.append(
                f"call {i + 1} to {want.name}: arguments changed, "
                f"recorded {want.arguments}, replayed {got.arguments}"
            )
    for outcome in actual.tools:
        if outcome.content.startswith("__istos_replay_unrecorded__"):
            out.append(f"{outcome.name} was called more often than it was recorded")

    if compare_text and expected.final_message != actual.final_message:
        out.append(
            f"final message changed: recorded {expected.final_message!r}, "
            f"replayed {actual.final_message!r}"
        )

    # Only compare the catalogue for turns that actually ran.
    for i, (turn, offered) in enumerate(zip(expected.model_turns, model.offered)):
        missing = [name for name in turn.tools_offered if name not in offered]
        if missing:
            out.append(f"turn {i + 1}: tools no longer offered: {missing}")

    recorded_approvals = [a for a in expected.approvals if a["kind"] == "approval_request"]
    replayed_approvals = [a for a in actual.approvals if a["kind"] == "approval_request"]
    if len(recorded_approvals) != len(replayed_approvals):
        out.append(
            f"approval requests changed: recorded {len(recorded_approvals)}, "
            f"replayed {len(replayed_approvals)}"
        )
    return out
