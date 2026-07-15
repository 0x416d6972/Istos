"""The orchestrator — fable-loop's bookends, and the node that owns the queues.

It runs the plan (classify, define done, gather evidence, fill the intent gate),
hands the work to the mesh, and reports what came back. It does not do the work
itself and it does not judge it.

Istos has no broker, so one node holds each queue's jobs, leases and dead-letter
list. That is this one. Start it last: it owns the queues the other nodes are
waiting on.

    python orchestrator.py
    python orchestrator.py --task "..."     # your own ask, against the same fixture
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


def load_scenario() -> dict:
    return {
        "spec": (SCENARIO / "README.md").read_text(),
        "source": (SCENARIO / "report.py").read_text(),
        "fixtures": {"calls.json": (SCENARIO / "calls.json").read_text()},
    }


def build_app() -> Istos:
    app = Istos(service_name="fable-orchestrator")

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


async def look_up_apis(app: Istos, scenario: dict, intent: dict) -> str:
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
        return "(no APIs named)"

    queue = queues.evidence_queue("api")
    job_id = await app.enqueue(queue, {"lens": "api", "names": named["names"]})
    found = await queues.wait_for(app, queue, job_id)

    for entry in found["entries"]:
        mark = {"found": "✓", "not_found": "✗ misremembered", "skipped": "– skipped"}
        print(f"  {mark[entry['status']]} {entry['name']}", flush=True)
    for surprise in found["surprises"]:
        print(f"  surprise: {surprise}", flush=True)

    return found["citations"][0]


async def run_loop(app: Istos, task: str) -> int:
    scenario = load_scenario()

    print("\n── classifying ──", flush=True)
    shape = await llm.ask(
        method.CLASSIFY_SYSTEM,
        f"The ask:\n{task}",
        schema=method.CLASSIFY_SCHEMA,
        schema_name="classify",
    )
    print(f"  shape={shape['shape']} trivial={shape['trivial']}", flush=True)
    print(f"  {shape['why']}", flush=True)

    if shape["shape"] == "plan_first":
        print("\nPlan-first: this needs approval before any change. Stopping.", flush=True)
        return 0

    print("\n── defining done ──", flush=True)
    done = await llm.ask(
        method.DONE_SYSTEM,
        f"The ask:\n{task}\n\nThe project's rules:\n---\n{scenario['spec']}\n---",
        schema=method.DONE_SCHEMA,
        schema_name="done",
    )
    print(f"  done when: {done['criterion']}", flush=True)
    print(f"  verified by: {done['how_verified']}", flush=True)
    for assumption in done["assumptions"]:
        print(f"  assuming: {assumption}", flush=True)

    print("\n── gathering evidence (spec | code | runtime, in one batch) ──", flush=True)
    try:
        evidence = await gather_evidence(app, scenario, task)
    except queues.JobFailed as exc:
        print(f"\nEvidence gathering failed: {exc}", file=sys.stderr)
        print("Is an evidence node running? `python evidence.py`", file=sys.stderr)
        return 1

    for lens, found in evidence.items():
        print(f"  [{lens}] {found['finding']}", flush=True)
        for surprise in found["surprises"]:
            print(f"  [{lens}] surprise: {surprise}", flush=True)

    print("\n── intent gate ──", flush=True)
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
    print(f"  {method.intent_line(intent)}", flush=True)
    print(f"  agree={intent['agree']} should_edit_code={intent['should_edit_code']}", flush=True)
    if intent["conflict"]:
        print(f"  conflict: {intent['conflict']}", flush=True)
    print(f"  recommendation: {intent['recommendation']}", flush=True)

    print("\n── looking up the APIs the fix turns on (the follow-up round) ──", flush=True)
    api_docs = await look_up_apis(app, scenario, intent)

    print("\n── acting (the executor owns this; the judge decides if it took) ──", flush=True)
    job_id = await app.enqueue(
        queues.ACT,
        {
            "task_id": str(uuid.uuid4()),
            "task": task,
            "intent": intent,
            "api_docs": api_docs,
            "reference_output": REFERENCE_OUTPUT,
            **scenario,
        },
    )

    try:
        outcome = await queues.wait_for(app, queues.ACT, job_id)
    except queues.JobFailed:
        return await report_dead(app, task)

    return report_success(outcome)


async def report_dead(app: Istos, task: str) -> int:
    """The hand-back. Three cycles failed, so the queue stopped trying.

    The method says to stop after three and hand back what was tried, the actual
    output, and the current hypothesis. The dead-letter record holds exactly that,
    which is why the bound lives on the queue rather than in a prompt: nothing had
    to choose to honour it.
    """
    print("\n" + "=" * 72, flush=True)
    print("HANDED BACK — 3 fix-verify cycles failed, so the job dead-lettered.", flush=True)
    print("=" * 72, flush=True)

    for dead in await app.dead_letters(queues.ACT):
        print(f"\nTried {dead['attempts']} times. The judge's last word:\n", flush=True)
        print(dead["last_error"], flush=True)

    print(
        "\nNothing was changed on disk — the runs happened in scratch copies.\n"
        "The scenario is untouched and the next run starts clean.",
        flush=True,
    )
    return 1


def report_success(outcome: dict) -> int:
    """Step 6 — outcome first, then the evidence, then the caveats."""
    print("\n" + "=" * 72, flush=True)

    if outcome["outcome"] == "handed_back":
        print("NO CHANGE MADE — the code was right and the request was not.", flush=True)
        print("=" * 72, flush=True)
        print(f"\n{outcome['finding']}\n", flush=True)
        print(outcome["intent_line"], flush=True)
        print(f"\n{outcome['recommendation']}", flush=True)
        return 0

    print(f"{outcome['verdict']} — verified by a judge that re-ran it.", flush=True)
    print("=" * 72, flush=True)
    print(f"\n{outcome['rationale']}", flush=True)
    print(f"\nTook {outcome['attempts_taken']} fix-verify cycle(s).", flush=True)
    print(f"\n{outcome['intent_line']}\n", flush=True)
    print("The change:", flush=True)
    print(outcome["diff"], flush=True)
    print("What it prints now, observed by the judge on its own machine:", flush=True)
    for line in outcome["observed_output"].splitlines():
        print(f"    {line}", flush=True)
    print(f"\nThe judge: {outcome['judge_reasoning']}", flush=True)

    if outcome["caveats"]:
        print("\nCaveats:", flush=True)
        for caveat in outcome["caveats"]:
            print(f"  - {caveat}", flush=True)

    print(
        "\nThe edit lived in scratch copies only — scenario/report.py still has the "
        "bug, so the demo is repeatable.",
        flush=True,
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Fable workflow orchestrator")
    parser.add_argument("--task", default=DEFAULT_TASK, help="The ask to run the loop on.")
    args = parser.parse_args()

    asyncio.run(llm.preflight())
    app = build_app()

    async def go() -> int:
        async with app.serving():
            # The other nodes claim from queues this process owns, so give their
            # sessions a moment to find it before the first job goes out.
            await asyncio.sleep(1.0)
            return await run_loop(app, args.task)

    sys.exit(asyncio.run(go()))


if __name__ == "__main__":
    main()
