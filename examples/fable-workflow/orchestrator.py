"""The orchestrator — fable-loop's bookends, and the node that owns the queues.

It runs the plan (classify, define done, gather evidence, fill the intent gate),
hands the work to the mesh, and reports what came back. It does not do the work
itself and it does not judge it.

Istos has no broker, so one node holds each queue's jobs, leases and dead-letter
list. That is this one. Start it last: it owns the queues the other nodes are
waiting on.

    python orchestrator.py                  # run the loop once, print, exit
    python orchestrator.py --task "..."     # your own ask, against the same fixture
    python orchestrator.py --serve          # stay up; drive it over HTTP instead

The loop is an async generator of progress events. Nothing in it prints. The CLI
renders those events to a terminal and the SSE route forwards them to curl — two
front-ends over one loop, rather than a copy of the loop per front-end.
"""

import argparse
import asyncio
import sys
import uuid
from pathlib import Path

import llm
import method
import queues
from istos import Istos

SCENARIO = Path(__file__).parent / "scenario"

# The default ask. Billing's numbers are the reference, and they are correct —
# the trap is that 2026-06-01 reads 5 either way, so an agent that spot-checks a
# single day sees agreement and stops.
DEFAULT_TASK = (
    "Billing (which buckets by UTC day and is the system of record) shows 2 calls "
    "on 2026-05-31 and 1 on 2026-06-02. Running `python report.py` prints 1 and 2 "
    "for those days instead, and customers in Tokyo flagged the mismatch. Figure "
    "out why and fix report.py so it matches the rules in the README."
)

REFERENCE_OUTPUT = "2026-05-31 2\n2026-06-01 5\n2026-06-02 1"

# A full run is four or five minutes against a local 9B. The SSE default of 60s
# would cut the stream off somewhere around the evidence round.
HTTP_STREAM_TIMEOUT_S = 1800.0


def load_scenario() -> dict:
    return {
        "spec": (SCENARIO / "README.md").read_text(),
        "source": (SCENARIO / "report.py").read_text(),
        "fixtures": {"calls.json": (SCENARIO / "calls.json").read_text()},
    }


def build_app(*, http_port: int = None) -> Istos:
    app = Istos(service_name="fable-orchestrator", http_port=http_port)

    for lens in ("spec", "code", "runtime", "api"):
        app.queue(queues.evidence_queue(lens), keep_results=True, lease_s=600)

    # One delivery is one fix-verify cycle, so the method's hard bound is this
    # number. Spend it and the job dead-letters — which is the hand-back.
    app.queue(
        queues.ACT,
        keep_results=True,
        max_attempts=queues.MAX_FIX_VERIFY_CYCLES,
        lease_s=900,
        retry_backoff_s=1.0,
    )
    app.queue(queues.JUDGE, keep_results=True, lease_s=600)

    @app.stream("fable/run", http="GET /run", http_timeout_s=HTTP_STREAM_TIMEOUT_S)
    async def run(task: str = DEFAULT_TASK):
        """Drive the loop and stream each phase out as it happens.

        Over the fabric this is a multi-reply queryable; over HTTP the gateway
        turns each yielded event into an SSE frame. A run takes minutes, so
        streaming is not a flourish — a single blocking response would spend most
        of its life looking indistinguishable from a hang.
        """
        async for event in run_loop(app, task):
            yield event

    return app


async def gather_evidence(app: Istos, scenario: dict, task: str) -> dict:
    """Step 2 — every lens at once, each on its own queue.

    `asyncio.gather` is what makes the batch a batch. The method asks for
    independent lookups to go out together rather than in a chain; here that is
    the code's shape, not an instruction the model has to remember.

    One honest caveat: with a single LM Studio behind them, the three distillation
    calls queue up on one GPU, so this is concurrency in structure rather than
    wall-clock speedup. Point the lenses at different endpoints and it becomes
    real.
    """
    payload = {"task": task, **scenario}

    async def one(lens: str) -> dict:
        queue = queues.evidence_queue(lens)
        job_id = await app.enqueue(queue, {"lens": lens, **payload})
        return await queues.wait_for(app, queue, job_id)

    lenses = ("spec", "code", "runtime")
    results = await asyncio.gather(*(one(lens) for lens in lenses))
    return dict(zip(lenses, results))


async def look_up_apis(app: Istos, scenario: dict, intent: dict) -> dict:
    """The method's second round of lookups — one round, then one follow-up.

    The first round could not have done this: you cannot know which APIs a fix
    turns on until you know what the fix is. So the model names them, and the api
    lens reads what they actually are off the installed stdlib.

    This exists because of a failure that showed up every single run without it.
    The model knew the right idea — convert to UTC before taking the date — and
    then reached for `.astimezone()` with no argument, which converts to local
    time and silently reintroduces the bug it was sent to fix. It was not
    confused about timezones; it misremembered a signature. The docstring says
    `tz -> convert to local time in new timezone tz` and settles it.
    """
    named = await llm.ask(
        method.API_NAMES_SYSTEM,
        f"The fix that is about to be made:\n{intent['recommendation']}\n\n"
        f"The code it will be made to:\n---\n{scenario['source']}\n---\n\n"
        f"Which standard-library APIs does getting this right depend on?",
        schema=method.API_NAMES_SCHEMA,
        schema_name="api_names",
    )
    if not named["names"]:
        return {"docs": "(no APIs named)", "entries": [], "surprises": []}

    queue = queues.evidence_queue("api")
    job_id = await app.enqueue(queue, {"lens": "api", "names": named["names"]})
    found = await queues.wait_for(app, queue, job_id)
    return {
        "docs": found["citations"][0],
        "entries": found["entries"],
        "surprises": found["surprises"],
    }


async def run_loop(app: Istos, task: str):
    """The loop, as a stream of progress events.

    Yields dicts; renders nothing. Every phase reports what it found the moment
    it finds it, so a caller watching over SSE sees the same thing the terminal
    does, at the same time.
    """
    scenario = load_scenario()

    shape = await llm.ask(
        method.CLASSIFY_SYSTEM,
        f"The ask:\n{task}",
        schema=method.CLASSIFY_SCHEMA,
        schema_name="classify",
    )
    yield {"phase": "classify", **shape}

    if shape["shape"] == "plan_first":
        yield {
            "phase": "stopped",
            "why": "Plan-first: this needs approval before any change.",
        }
        return

    done = await llm.ask(
        method.DONE_SYSTEM,
        f"The ask:\n{task}\n\nThe project's rules:\n---\n{scenario['spec']}\n---",
        schema=method.DONE_SCHEMA,
        schema_name="done",
    )
    yield {"phase": "done", **done}

    try:
        evidence = await gather_evidence(app, scenario, task)
    except queues.JobFailed as exc:
        yield {
            "phase": "error",
            "why": f"Evidence gathering failed: {exc}",
            "hint": "Is an evidence node running? `python evidence.py`",
        }
        return

    for lens, found in evidence.items():
        yield {
            "phase": "evidence",
            "lens": lens,
            "finding": found["finding"],
            "surprises": found["surprises"],
        }

    intent = await llm.ask(
        method.INTENT_SYSTEM,
        f"TASK:\n{task}\n\n"
        f"EVIDENCE — the code lens (fills X):\n{evidence['code']['finding']}\n"
        f"cited: {'; '.join(evidence['code']['citations'])}\n\n"
        f"EVIDENCE — the runtime lens, observed by running it (fills Y):\n"
        f"{evidence['runtime']['finding']}\n"
        f"it actually printed:\n{evidence['runtime']['observed']['stdout']}\n\n"
        f"EVIDENCE — the spec lens (fills Z):\n{evidence['spec']['finding']}\n"
        f"cited: {'; '.join(evidence['spec']['citations'])}\n\n"
        f"Fill all three slots, then decide who is wrong.",
        schema=method.INTENT_SCHEMA,
        schema_name="intent",
    )
    yield {"phase": "intent", "intent_line": method.intent_line(intent), **intent}

    apis = await look_up_apis(app, scenario, intent)
    yield {"phase": "apis", "entries": apis["entries"], "surprises": apis["surprises"]}

    job_id = await app.enqueue(
        queues.ACT,
        {
            "task_id": str(uuid.uuid4()),
            "task": task,
            "intent": intent,
            "api_docs": apis["docs"],
            "reference_output": REFERENCE_OUTPUT,
            **scenario,
        },
    )

    try:
        outcome = await queues.wait_for(app, queues.ACT, job_id)
    except queues.JobFailed:
        # The hand-back. Three cycles failed, so the queue stopped trying — the
        # method's bound, honoured by nothing having to choose to honour it. The
        # dead-letter record already holds what was tried and the last word on why.
        yield {
            "phase": "dead_lettered",
            "dead_letters": await app.dead_letters(queues.ACT),
        }
        return

    # The executor's own outcome: "verified", or "handed_back" when the code was
    # right and the request was not.
    yield {"phase": outcome["outcome"], **outcome}


# --- the terminal front-end ---------------------------------------------------

HEADINGS = {
    "classify": "── classifying ──",
    "done": "── defining done ──",
    "intent": "── intent gate ──",
    "apis": "── looking up the APIs the fix turns on (the follow-up round) ──",
}

_MARKS = {"found": "✓", "not_found": "✗ misremembered", "skipped": "– skipped"}


def render(event: dict) -> None:
    """Print one progress event. Returns nothing; the exit code comes from main."""
    phase = event["phase"]
    if phase in HEADINGS:
        print(f"\n{HEADINGS[phase]}", flush=True)

    if phase == "classify":
        print(f"  shape={event['shape']} trivial={event['trivial']}", flush=True)
        print(f"  {event['why']}", flush=True)

    elif phase == "done":
        print(f"  done when: {event['criterion']}", flush=True)
        print(f"  verified by: {event['how_verified']}", flush=True)
        for assumption in event["assumptions"]:
            print(f"  assuming: {assumption}", flush=True)
        print("\n── gathering evidence (spec | code | runtime, in one batch) ──", flush=True)

    elif phase == "evidence":
        print(f"  [{event['lens']}] {event['finding']}", flush=True)
        for surprise in event["surprises"]:
            print(f"  [{event['lens']}] surprise: {surprise}", flush=True)

    elif phase == "intent":
        print(f"  {event['intent_line']}", flush=True)
        print(
            f"  agree={event['agree']} should_edit_code={event['should_edit_code']}",
            flush=True,
        )
        if event["conflict"]:
            print(f"  conflict: {event['conflict']}", flush=True)
        print(f"  recommendation: {event['recommendation']}", flush=True)

    elif phase == "apis":
        for entry in event["entries"]:
            print(f"  {_MARKS[entry['status']]} {entry['name']}", flush=True)
        for surprise in event["surprises"]:
            print(f"  surprise: {surprise}", flush=True)
        print("\n── acting (the executor owns this; the judge decides if it took) ──", flush=True)

    elif phase in ("stopped", "error"):
        print(f"\n{event['why']}", flush=True)
        if event.get("hint"):
            print(event["hint"], file=sys.stderr, flush=True)

    elif phase == "dead_lettered":
        _render_dead_lettered(event)

    elif phase == "handed_back":
        _render_finding(event)

    elif phase == "verified":
        _render_verified(event)


def _render_dead_lettered(event: dict) -> None:
    print("\n" + "=" * 72, flush=True)
    print("HANDED BACK — 3 fix-verify cycles failed, so the job dead-lettered.", flush=True)
    print("=" * 72, flush=True)
    for dead in event["dead_letters"]:
        print(f"\nTried {dead['attempts']} times. The judge's last word:\n", flush=True)
        print(dead["last_error"], flush=True)
    print(
        "\nNothing was changed on disk — the runs happened in scratch copies.\n"
        "The scenario is untouched and the next run starts clean.",
        flush=True,
    )


def _render_verified(event: dict) -> None:
    print("\n" + "=" * 72, flush=True)
    print(f"{event['verdict']} — verified by a judge that re-ran it.", flush=True)
    print("=" * 72, flush=True)
    print(f"\n{event['rationale']}", flush=True)
    print(f"\nTook {event['attempts_taken']} fix-verify cycle(s).", flush=True)
    print(f"\n{event['intent_line']}\n", flush=True)
    print("The change:", flush=True)
    print(event["diff"], flush=True)
    print("What it prints now, observed by the judge on its own machine:", flush=True)
    for line in event["observed_output"].splitlines():
        print(f"    {line}", flush=True)
    print(f"\nThe judge: {event['judge_reasoning']}", flush=True)
    if event["caveats"]:
        print("\nCaveats:", flush=True)
        for caveat in event["caveats"]:
            print(f"  - {caveat}", flush=True)
    print(
        "\nThe edit lived in scratch copies only — scenario/report.py still has the "
        "bug, so the demo is repeatable.",
        flush=True,
    )


def _render_finding(event: dict) -> None:
    print("\n" + "=" * 72, flush=True)
    print("NO CHANGE MADE — the code was right and the request was not.", flush=True)
    print("=" * 72, flush=True)
    print(f"\n{event['finding']}\n", flush=True)
    print(event["intent_line"], flush=True)
    print(f"\n{event['recommendation']}", flush=True)


# Nonzero only where a human has to do something. Refusing to edit correct code
# is a result, not a failure, so `handed_back` exits 0.
FAILED_PHASES = {"error", "dead_lettered"}


async def cli(task: str) -> int:
    app = build_app()
    async with app.serving():
        # The other nodes claim from queues this process owns, so give their
        # sessions a moment to find it before the first job goes out.
        await asyncio.sleep(1.0)

        exit_code = 0
        async for event in run_loop(app, task):
            render(event)
            if event["phase"] in FAILED_PHASES:
                exit_code = 1
        return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(description="Fable workflow orchestrator")
    parser.add_argument("--task", default=DEFAULT_TASK, help="The ask to run the loop on.")
    parser.add_argument(
        "--serve",
        nargs="?",
        type=int,
        const=8080,
        default=None,
        metavar="PORT",
        help="Stay up and serve GET /run (SSE) on PORT instead of running once.",
    )
    args = parser.parse_args()

    asyncio.run(llm.preflight())

    if args.serve is None:
        sys.exit(asyncio.run(cli(args.task)))

    app = build_app(http_port=args.serve)
    print(f"orchestrator up — curl -N http://127.0.0.1:{args.serve}/run", flush=True)
    app.run()


if __name__ == "__main__":
    main()
