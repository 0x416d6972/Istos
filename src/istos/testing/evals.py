"""Eval cases for agents — assert on a whole trajectory, not one return value.

A tool test asserts a return value. An agent test has to assert something looser
but more useful: that the agent *reached for the right tool*, with plausible
arguments, without touching the ones it must not, and said something recognisable
at the end. That is what an :class:`EvalCase` states::

    cases = [
        EvalCase(
            name="refund happy path",
            user="please refund order o-1",
            expect_tools=["billing-refund"],
            expect_text="refunded",
        ),
        EvalCase(
            name="no refund without an order",
            user="give me money",
            forbid_tools=["billing-refund"],
        ),
    ]

    report = await run_eval(cases, model=lambda: OpenAIChatModel(...), tools=tools)
    assert report.ok, format_eval_report(report)

Each case runs its own conversation and is recorded, so a run can be saved and
replayed later without the model (see :mod:`istos.testing.trajectory`).

Judgement stays out of here on purpose: ``expect_text`` is a substring, not an
LLM grader. Pass ``check=`` for anything sharper — it receives the trajectory and
returns a failure string, or None.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Sequence, Union

from istos.agent.model import Model
from istos.agent.tools import MeshTool
from istos.testing.trajectory import Trajectory, record_agent

ModelSource = Union[Model, Callable[[], Model]]
Check = Callable[[Trajectory], Optional[str]]


@dataclass
class EvalCase:
    """One thing an agent should (or should not) do."""

    name: str
    user: Union[str, Sequence[str]]
    system: Optional[str] = None
    expect_tools: Sequence[str] = ()      # these names, in this order (gaps allowed)
    forbid_tools: Sequence[str] = ()      # none of these may be called
    expect_text: Optional[str] = None     # substring of the final message, case-insensitive
    expect_no_error: bool = True          # no tool_result may come back failed
    max_tool_calls: Optional[int] = None
    max_steps: int = 8
    check: Optional[Check] = None          # your own assertion over the trajectory
    tools: Optional[Sequence[MeshTool]] = None   # override the shared catalogue

    def evaluate(self, traj: Trajectory) -> List[str]:
        """Every way ``traj`` failed this case. Empty means it passed."""
        failures: List[str] = []
        called = traj.tool_names

        if self.expect_tools and not _is_subsequence(list(self.expect_tools), called):
            failures.append(
                f"expected tools {list(self.expect_tools)} in order, called {called}"
            )
        forbidden = [name for name in called if name in set(self.forbid_tools)]
        if forbidden:
            failures.append(f"called forbidden tool(s) {sorted(set(forbidden))}")

        if self.expect_text is not None:
            final = traj.final_message or ""
            if self.expect_text.lower() not in final.lower():
                failures.append(
                    f"expected {self.expect_text!r} in the final message, got {final!r}"
                )
        if self.expect_no_error:
            failed = [t.name for t in traj.tools if t.error]
            if failed:
                failures.append(f"tool(s) returned an error: {failed}")
        if self.max_tool_calls is not None and len(called) > self.max_tool_calls:
            failures.append(
                f"made {len(called)} tool calls, budget was {self.max_tool_calls}"
            )
        if self.check is not None:
            problem = self.check(traj)
            if problem:
                failures.append(problem)
        return failures


@dataclass
class EvalResult:
    """One case's outcome."""

    case: EvalCase
    trajectory: Optional[Trajectory] = None
    failures: List[str] = field(default_factory=list)
    error: Optional[str] = None       # the run itself blew up
    duration_s: float = 0.0

    @property
    def passed(self) -> bool:
        return not self.failures and self.error is None


@dataclass
class EvalReport:
    """Every case's outcome, plus the tally."""

    results: List[EvalResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return len(self.results) - self.passed

    def trajectories(self) -> List[Trajectory]:
        return [r.trajectory for r in self.results if r.trajectory is not None]


def _is_subsequence(wanted: List[str], actual: List[str]) -> bool:
    """Whether ``wanted`` appears in ``actual`` in order (other calls may sit
    between). An agent taking an extra look at something is not a failure."""
    it = iter(actual)
    return all(any(name == got for got in it) for name in wanted)


async def run_case(
    case: EvalCase,
    *,
    model: ModelSource,
    tools: Sequence[MeshTool] = (),
    **run_kwargs: Any,
) -> EvalResult:
    """Run one case in a conversation of its own and grade it."""
    started = time.monotonic()
    # A model has .complete(); anything else callable is a factory for one.
    instance = model if hasattr(model, "complete") else model()  # type: ignore[operator]
    catalog = list(case.tools if case.tools is not None else tools)
    try:
        traj, _ = await record_agent(
            instance, catalog, [],
            name=case.name, user=case.user, system=case.system,
            max_steps=case.max_steps, **run_kwargs,
        )
    except Exception as exc:
        return EvalResult(
            case=case,
            error=f"{type(exc).__name__}: {exc}",
            duration_s=time.monotonic() - started,
        )
    return EvalResult(
        case=case,
        trajectory=traj,
        failures=case.evaluate(traj),
        duration_s=time.monotonic() - started,
    )


async def run_eval(
    cases: Sequence[EvalCase],
    *,
    model: ModelSource,
    tools: Sequence[MeshTool] = (),
    **run_kwargs: Any,
) -> EvalReport:
    """Run every case, sequentially, and collect the report.

    ``model`` is a model or a zero-argument factory. Pass a factory when the model
    holds per-run state (a :class:`~istos.testing.trajectory.ReplayModel`, a
    scripted stub) — each case then gets its own.

    Cases run one at a time on purpose: they share the fabric, and a parallel run
    would make a failure depend on what else was in flight.
    """
    report = EvalReport()
    for case in cases:
        report.results.append(
            await run_case(case, model=model, tools=tools, **run_kwargs)
        )
    return report


def format_eval_report(report: EvalReport, *, verbose: bool = False) -> str:
    """A short text report — the CLI's output, and readable in a pytest failure."""
    lines: List[str] = []
    for result in report.results:
        mark = "PASS" if result.passed else "FAIL"
        lines.append(f"[{mark}] {result.case.name}  ({result.duration_s:.2f}s)")
        if result.error:
            lines.append(f"       ! {result.error}")
        for failure in result.failures:
            lines.append(f"       - {failure}")
        if verbose and result.trajectory is not None:
            lines.append(f"       tools: {result.trajectory.tool_names}")
            lines.append(f"       final: {result.trajectory.final_message!r}")
    total = len(report.results)
    lines.append("")
    lines.append(f"{report.passed}/{total} passed" + (
        f", {report.failed} failed" if report.failed else ""
    ))
    return "\n".join(lines)
