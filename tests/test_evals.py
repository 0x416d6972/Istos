"""Recording agent trajectories, replaying them, and grading eval cases."""

import asyncio
import json

import pytest

from istos import ApprovalGate, MeshTool, ModelReply, ToolCall
from istos.testing import (
    EvalCase,
    ReplayModel,
    Trajectory,
    TrajectoryRecorder,
    format_eval_report,
    record_agent,
    replay,
    replay_tools,
    run_eval,
)
from istos.testing.trajectory import ReplayExhausted


class _ScriptedModel:
    """A model with a fixed script — stands in for the real one when recording."""

    def __init__(self, replies: list) -> None:
        self._replies = list(replies)
        self.offered: list = []

    async def complete(self, messages, *, tools=None) -> ModelReply:
        self.offered.append([t["function"]["name"] for t in tools or []])
        if not self._replies:
            return ModelReply(content="nothing left to say")
        return self._replies.pop(0)


def _tools(calls: list) -> list:
    async def refund(order_id: str) -> dict:
        calls.append(("refund", order_id))
        return {"refunded": order_id}

    async def lookup(order_id: str) -> dict:
        calls.append(("lookup", order_id))
        return {"order_id": order_id, "total": 25}

    return [
        MeshTool("billing/refund", invoke=refund, description="Refund an order"),
        MeshTool("billing/lookup", invoke=lookup, description="Look an order up"),
    ]


def _script() -> list:
    return [
        ModelReply(tool_calls=[
            ToolCall(id="c1", name="billing-lookup", arguments={"order_id": "o-1"})
        ]),
        ModelReply(tool_calls=[
            ToolCall(id="c2", name="billing-refund", arguments={"order_id": "o-1"})
        ]),
        ModelReply(content="Refunded order o-1."),
    ]


async def _recorded(calls=None) -> Trajectory:
    traj, _ = await record_agent(
        _ScriptedModel(_script()),
        _tools(calls if calls is not None else []),
        [],
        name="refund-flow",
        user="refund order o-1",
        system="You are support.",
    )
    return traj


# ---------------------------------------------------------------------------
# recording
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_record_captures_the_whole_run():
    calls: list = []
    traj = await _recorded(calls)

    assert calls == [("lookup", "o-1"), ("refund", "o-1")]
    assert traj.name == "refund-flow"
    assert traj.system == "You are support."
    assert traj.user_turns == ["refund order o-1"]
    assert traj.tool_names == ["billing-lookup", "billing-refund"]
    assert traj.tools[0].arguments == {"order_id": "o-1"}
    assert traj.final_message == "Refunded order o-1."
    # Three completions, and each one records the catalogue it was offered.
    assert len(traj.model_turns) == 3
    assert traj.model_turns[0].tools_offered == ["billing-refund", "billing-lookup"]
    assert [e["kind"] for e in traj.events][-2:] == ["message", "done"]


@pytest.mark.asyncio
async def test_a_trajectory_round_trips_through_a_file(tmp_path):
    traj = await _recorded()
    path = traj.save(tmp_path / "nested" / "refund-flow.json")
    assert json.loads(path.read_text())["name"] == "refund-flow"

    loaded = Trajectory.load(path)
    assert loaded.tool_names == traj.tool_names
    assert loaded.final_message == traj.final_message
    assert loaded.model_turns[1].tool_calls[0]["name"] == "billing-refund"


def test_a_newer_format_is_refused_rather_than_misread():
    with pytest.raises(ValueError, match="newer than this Istos"):
        Trajectory.from_dict({"version": 99, "name": "x"})


def test_load_names_an_unnamed_trajectory_after_its_file(tmp_path):
    path = tmp_path / "checkout.json"
    path.write_text(json.dumps({"version": 1, "user_turns": ["hi"]}))
    assert Trajectory.load(path).name == "checkout"


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_replay_reproduces_a_recording_without_a_model_or_a_mesh():
    traj = await _recorded()
    result = await replay(traj)
    assert result.ok, result.diff
    assert result.tool_names == ["billing-lookup", "billing-refund"]
    assert result.final_message == "Refunded order o-1."


@pytest.mark.asyncio
async def test_replay_notices_a_tool_that_is_gone():
    traj = await _recorded()
    # The refund tool disappeared from the catalogue: the loop now reports an
    # unknown tool instead of calling it.
    surviving = [t for t in replay_tools(traj) if t.name != "billing-refund"]
    result = await replay(traj, tools=surviving)
    assert not result.ok
    assert any("no longer offered" in d for d in result.diff)


@pytest.mark.asyncio
async def test_replay_notices_changed_arguments():
    traj = await _recorded()
    traj.tools[1].arguments = {"order_id": "o-999"}     # as if the code changed
    result = await replay(traj)
    assert any("arguments changed" in d for d in result.diff)


@pytest.mark.asyncio
async def test_replay_notices_a_different_answer():
    traj = await _recorded()
    traj.events.append({"kind": "message", "content": "Something else entirely."})
    result = await replay(traj)
    assert any("final message changed" in d for d in result.diff)
    # ...and can be told not to care about the wording.
    assert (await replay(traj, compare_text=False)).ok


@pytest.mark.asyncio
async def test_replay_reports_an_exhausted_recording():
    traj = await _recorded()
    traj.model_turns = traj.model_turns[:1]     # recording cut short
    result = await replay(traj)
    assert any("only 1 were recorded" in d for d in result.diff)


@pytest.mark.asyncio
async def test_replay_model_hands_back_the_recorded_turns_in_order():
    traj = await _recorded()
    model = ReplayModel(traj)
    first = await model.complete([])
    assert first.tool_calls[0].name == "billing-lookup"
    await model.complete([])
    third = await model.complete([])
    assert third.content == "Refunded order o-1."
    assert model.used == 3
    with pytest.raises(ReplayExhausted):
        await model.complete([])


@pytest.mark.asyncio
async def test_a_gated_recording_replays_through_a_gate():
    """The approval path is part of the behaviour, so it is part of the replay."""
    calls: list = []
    tools = _tools(calls)
    tools[0].approval = "moves real money"
    gate = ApprovalGate()
    approved: list = []

    async def approve(req):
        approved.append(req.tool)
        await gate.approve(req.id, by="amir")

    gate.on_request = approve

    traj, _ = await record_agent(
        _ScriptedModel(_script()), tools, [],
        name="gated-refund", user="refund order o-1", approvals=gate,
    )
    assert approved == ["billing-refund"]
    assert [a["kind"] for a in traj.approvals] == [
        "approval_request", "approval_decision",
    ]

    # No gate passed: the replay stands one in and answers as the human did.
    result = await replay(traj)
    assert result.ok, result.diff
    assert [a["kind"] for a in result.replayed.approvals] == [
        "approval_request", "approval_decision",
    ]


@pytest.mark.asyncio
async def test_a_recorded_denial_replays_as_a_denial():
    calls: list = []
    tools = _tools(calls)
    tools[0].approval = True
    gate = ApprovalGate()
    gate.on_request = lambda req: gate.deny(req.id, by="amir", note="no")

    traj, _ = await record_agent(
        _ScriptedModel(_script()), tools, [],
        name="denied-refund", user="refund order o-1", approvals=gate,
    )
    assert ("refund", "o-1") not in calls

    result = await replay(traj)
    assert result.ok, result.diff
    decision = [a for a in result.replayed.approvals if a["kind"] == "approval_decision"]
    assert decision[0]["error"] is True


# ---------------------------------------------------------------------------
# eval cases
# ---------------------------------------------------------------------------
def _case(**kwargs) -> EvalCase:
    base = {"name": "refund", "user": "refund order o-1"}
    base.update(kwargs)
    return EvalCase(**base)


@pytest.mark.asyncio
async def test_a_passing_case():
    report = await run_eval(
        [_case(expect_tools=["billing-lookup", "billing-refund"], expect_text="refunded")],
        model=lambda: _ScriptedModel(_script()),
        tools=_tools([]),
    )
    assert report.ok
    assert report.passed == 1
    assert report.results[0].trajectory is not None
    assert "1/1 passed" in format_eval_report(report)


@pytest.mark.asyncio
async def test_failures_name_what_went_wrong():
    report = await run_eval(
        [
            _case(name="wrong order", expect_tools=["billing-refund", "billing-lookup"]),
            _case(name="forbidden", forbid_tools=["billing-refund"]),
            _case(name="wrong text", expect_text="cancelled"),
            _case(name="over budget", max_tool_calls=1),
        ],
        model=lambda: _ScriptedModel(_script()),
        tools=_tools([]),
    )
    assert not report.ok
    assert report.failed == 4
    failures = {r.case.name: r.failures for r in report.results}
    assert "in order" in failures["wrong order"][0]
    assert "forbidden" in failures["forbidden"][0]
    assert "cancelled" in failures["wrong text"][0]
    assert "budget" in failures["over budget"][0]

    text = format_eval_report(report, verbose=True)
    assert "[FAIL] wrong order" in text
    assert "0/4 passed, 4 failed" in text


@pytest.mark.asyncio
async def test_extra_tool_calls_between_the_expected_ones_are_fine():
    # An agent that looks something up on the way is not failing the case.
    report = await run_eval(
        [_case(expect_tools=["billing-refund"])],
        model=lambda: _ScriptedModel(_script()),
        tools=_tools([]),
    )
    assert report.ok


@pytest.mark.asyncio
async def test_a_failing_tool_fails_the_case_by_default():
    async def broken(**_):
        raise RuntimeError("upstream is down")

    report = await run_eval(
        [_case(expect_tools=["billing-refund"])],
        model=lambda: _ScriptedModel([
            ModelReply(tool_calls=[
                ToolCall(id="c1", name="billing-refund", arguments={"order_id": "o-1"})
            ]),
            ModelReply(content="I could not refund it."),
        ]),
        tools=[MeshTool("billing/refund", invoke=broken)],
    )
    assert not report.ok
    assert "returned an error" in report.results[0].failures[0]


@pytest.mark.asyncio
async def test_a_custom_check_can_assert_anything_about_the_run():
    def flags_a_refund_of_o1(traj) -> str | None:
        for call in traj.tools:
            if call.name == "billing-refund" and call.arguments.get("order_id") == "o-1":
                return "refunded the order we said not to"
        return None

    report = await run_eval(
        [_case(check=flags_a_refund_of_o1)],
        model=lambda: _ScriptedModel(_script()),
        tools=_tools([]),
    )
    assert report.results[0].failures == ["refunded the order we said not to"]


@pytest.mark.asyncio
async def test_a_run_that_blows_up_is_reported_not_raised():
    class _Broken:
        async def complete(self, messages, *, tools=None):
            raise RuntimeError("model unreachable")

    report = await run_eval([_case()], model=_Broken(), tools=_tools([]))
    assert not report.ok
    assert "model unreachable" in (report.results[0].error or "")
    assert "! RuntimeError" in format_eval_report(report)


@pytest.mark.asyncio
async def test_a_case_can_bring_its_own_tools():
    calls: list = []
    only_lookup = [_tools(calls)[1]]
    report = await run_eval(
        [_case(tools=only_lookup, expect_tools=["billing-lookup"], expect_no_error=False)],
        model=lambda: _ScriptedModel(_script()),
        tools=[],
    )
    assert report.results[0].trajectory.tool_names[0] == "billing-lookup"


@pytest.mark.asyncio
async def test_eval_runs_can_be_saved_and_replayed_later(tmp_path):
    report = await run_eval(
        [_case(expect_text="refunded")],
        model=lambda: _ScriptedModel(_script()),
        tools=_tools([]),
    )
    saved = [t.save(tmp_path / f"{t.name}.json") for t in report.trajectories()]
    assert len(saved) == 1
    result = await replay(Trajectory.load(saved[0]))
    assert result.ok, result.diff


# ---------------------------------------------------------------------------
# the recorder, driven by hand
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_the_recorder_can_be_driven_directly():
    from istos.agent import run_agent

    rec = TrajectoryRecorder(name="by-hand", system="sys", service="agent")
    model = rec.model(_ScriptedModel(_script()))
    rec.user("refund order o-1")
    messages = [{"role": "system", "content": "sys"}, {"role": "user", "content": "x"}]
    async for event in run_agent(model, _tools([]), messages):
        rec.observe(event)

    traj = rec.build()
    assert traj.service == "agent"
    assert traj.tool_names == ["billing-lookup", "billing-refund"]
    assert traj.user_turns == ["refund order o-1"]


# ---------------------------------------------------------------------------
# istos eval
# ---------------------------------------------------------------------------
def _write_app(tmp_path, monkeypatch, body: str) -> str:
    """Write a module holding an Istos app and make it importable."""
    import sys

    (tmp_path / "evalapp.py").write_text(body)
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("evalapp", None)
    return "evalapp"


def test_cli_eval_passes_on_a_faithful_recording(tmp_path, capsys):
    from istos.cli import main

    asyncio.run(_recorded()).save(tmp_path / "refund-flow.json")
    main(["eval", str(tmp_path)])
    out = capsys.readouterr().out
    assert "[PASS] refund-flow" in out
    assert "1/1 trajectories reproduced" in out


def test_cli_eval_exits_nonzero_when_behaviour_moved(tmp_path, capsys):
    from istos.cli import main

    traj = asyncio.run(_recorded())
    traj.model_turns = traj.model_turns[:1]
    traj.save(tmp_path / "refund-flow.json")

    with pytest.raises(SystemExit) as exc:
        main(["eval", str(tmp_path)])
    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "[FAIL] refund-flow" in out
    assert "0/1 trajectories reproduced" in out


def test_cli_eval_complains_about_a_missing_path(tmp_path):
    from istos.cli import main

    with pytest.raises(SystemExit) as exc:
        main(["eval", str(tmp_path / "nope")])
    assert exc.value.code == 1


def test_cli_eval_complains_about_an_empty_directory(tmp_path):
    from istos.cli import main

    with pytest.raises(SystemExit):
        main(["eval", str(tmp_path)])


def test_cli_eval_checks_the_recording_against_the_live_app(
    tmp_path, monkeypatch, capsys
):
    """The valuable failure: the code drifted from what the agent recorded."""
    from istos.cli import main

    asyncio.run(_recorded()).save(tmp_path / "refund-flow.json")
    module = _write_app(tmp_path, monkeypatch, '''
from istos import Istos

istos = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


@istos.handle("billing/lookup")
async def lookup(order_id: str) -> dict:
    """Look an order up."""
    return {"order_id": order_id}


@istos.handle("billing/refund")
async def refund(order_id: int) -> dict:      # was a str when this was recorded
    """Refund an order."""
    return {"refunded": order_id}
''')

    with pytest.raises(SystemExit):
        main(["eval", str(tmp_path), "--app", module])
    out = capsys.readouterr().out
    assert "no longer accepts the recorded arguments" in out


def test_cli_eval_flags_a_tool_that_left_the_app(tmp_path, monkeypatch, capsys):
    from istos.cli import main

    asyncio.run(_recorded()).save(tmp_path / "refund-flow.json")
    module = _write_app(tmp_path, monkeypatch, '''
from istos import Istos

istos = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


@istos.handle("billing/lookup")
async def lookup(order_id: str) -> dict:
    """Look an order up."""
    return {"order_id": order_id}
''')

    with pytest.raises(SystemExit):
        main(["eval", str(tmp_path), "--app", module])
    assert "billing-refund is no longer a handler" in capsys.readouterr().out


def test_cli_eval_accepts_an_app_module_without_a_named_attribute(
    tmp_path, monkeypatch, capsys
):
    from istos.cli import main

    asyncio.run(_recorded()).save(tmp_path / "refund-flow.json")
    module = _write_app(tmp_path, monkeypatch, '''
from istos import Istos

app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


@app.handle("billing/lookup")
async def lookup(order_id: str) -> dict:
    """Look an order up."""
    return {"order_id": order_id}


@app.handle("billing/refund")
async def refund(order_id: str) -> dict:
    """Refund an order."""
    return {"refunded": order_id}
''')

    main(["eval", str(tmp_path / "refund-flow.json"), "--app", module])
    assert "[PASS]" in capsys.readouterr().out


def test_load_app_needs_something_to_load(tmp_path, monkeypatch):
    from istos.cli import _load_app

    module = _write_app(tmp_path, monkeypatch, "value = 1\n")
    with pytest.raises(ValueError, match="no 'istos' or 'app' attribute"):
        _load_app(module)
