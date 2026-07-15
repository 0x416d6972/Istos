"""The executor — method Steps 4 and 5, act and verify.

    python executor.py
    FABLE_LIE=1 python executor.py     # claim success without doing the work

It proposes an edit, applies it, runs the result, and asks the judge. What it
cannot do is mark its own work done. Returning acks the job; raising nacks it and
the queue redelivers. The judge's verdict decides which happens, and the judge is
somewhere else on the mesh. An executor that decides it is finished is not making
a claim anyone downstream has to believe.

That is the difference from the same loop in one context. A model that has just
written a patch is the worst available reviewer of it, and asking it to check its
own work mostly produces a confident second opinion from the same reasoning that
produced the bug.

The retry carries the critique with it. Raising `Refuted(summary)` nacks the job
with that summary as its error, and the queue hands it to the next attempt as
`ctx.last_error` — so attempt 2 is told exactly why attempt 1 was rejected,
whichever process picks it up. Three attempts and the job dead-letters, which is
the method's "after 3 failed fix-verify cycles, stop and hand back".

`FABLE_LIE=1` shows the mechanism working. The executor then reports the numbers
the task asked for without changing or running anything — the single most common
agent fraud, and the one a self-graded loop cannot catch, because the claim and
the check come from the same place.
"""

import os
from contextlib import asynccontextmanager

import llm
import method
import queues
import sandbox
from istos import Istos, JobContext

LIE = os.environ.get("FABLE_LIE") == "1"

# The critique travels as the nack's error string. Keep it useful but bounded —
# it rides in every subsequent claim reply for this job.
MAX_CRITIQUE_CHARS = 1200

istos = Istos(service_name="fable-executor")


class Refuted(RuntimeError):
    """The judge rejected the work. Raising nacks the job: one more cycle."""


@asynccontextmanager
async def on_start(app):
    await llm.preflight()
    print(f"executor node up{' (LYING)' if LIE else ''}", flush=True)
    yield


istos.lifespan = on_start


@istos.worker(queues.ACT)
async def act(job, ctx: JobContext):
    note = f"attempt {ctx.attempt}/{ctx.max_attempts}"
    if ctx.is_last_attempt:
        note += " — last one before it dead-letters"
    print(f"  [executor] claimed a job ({note})", flush=True)

    intent = job["intent"]
    if not intent["should_edit_code"]:
        # The spec sided with the code: the task's premise is what is wrong. The
        # deliverable is the finding, not a patch. Acked — refusing to edit is a
        # correct outcome, not a failure.
        print("  [executor] handed_back", flush=True)
        return {
            "outcome": "handed_back",
            "intent_line": method.intent_line(intent),
            "finding": intent["conflict"],
            "recommendation": intent["recommendation"],
        }

    if LIE:
        # Change nothing, run nothing, report the numbers the task asked for.
        #
        # The lie has to be an actual lie. Claiming the reference output after a
        # real edit is not one: the model usually gets this fix right, the claim
        # then matches the re-run, and there is nothing to catch. The fraud worth
        # demonstrating is the one that shows up in the wild — "should work now",
        # on work that was never done.
        patched = job["source"]
        claimed_output = job["reference_output"]
        rationale = "Converted the timestamps to UTC before bucketing."
        print("  [executor] claiming success, having changed and run nothing", flush=True)
    else:
        edit = await propose_edit(job, ctx)
        patched = sandbox.apply_edits(job["source"], edit["edits"])
        rationale = edit["rationale"]
        print(f"  [executor] {len(edit['edits'])} edit(s): {rationale[:80]}", flush=True)

        run = await sandbox.run_report(patched, job["fixtures"])
        claimed_output = str(run["stdout"])
        print(f"  [executor] ran it, got:\n{_indent(claimed_output)}", flush=True)

    intent_line = method.intent_line(intent)
    judge_id = await istos.enqueue(
        queues.JUDGE,
        {
            "task": job["task"],
            "spec": job["spec"],
            "original_source": job["source"],
            "patched_source": patched,
            "fixtures": job["fixtures"],
            "claimed_output": claimed_output,
            "reference_output": job["reference_output"],
            "intent_line": intent_line,
        },
    )
    ruling = await queues.wait_for(istos, queues.JUDGE, judge_id)

    if ruling["verdict"] == "REFUTED":
        raise Refuted(_critique(ruling))

    print("  [executor] verified", flush=True)
    return {
        "outcome": "verified",
        "verdict": ruling["verdict"],
        "attempts_taken": ctx.attempt,
        "intent_line": intent_line,
        "rationale": rationale,
        "diff": ruling["diff"],
        "observed_output": ruling["observed_output"],
        "judge_reasoning": ruling["reasoning"],
        "caveats": [f["evidence"] for f in ruling["frauds"]],
    }


async def propose_edit(job, ctx: JobContext) -> dict:
    """Ask for the smallest edit. The schema will not accept a rewrite."""
    retry_note = ""
    if ctx.is_retry and ctx.last_error:
        retry_note = (
            f"\nAttempt {ctx.attempt - 1} at this was REFUTED by the judge:\n"
            f"---\n{ctx.last_error}\n---\n"
            f"Do not repeat it. Address that specifically.\n"
        )

    return await llm.ask(
        method.EDIT_SYSTEM,
        f"TASK:\n{job['task']}\n\n"
        f"THE SPEC (README.md):\n---\n{job['spec']}\n---\n\n"
        f"THE INTENT GATE, already filled from the evidence:\n"
        f"{method.intent_line(job['intent'])}\n\n"
        f"WHAT TO DO:\n{job['intent']['recommendation']}\n\n"
        f"THE APIS INVOLVED, read from this machine's stdlib just now. These are\n"
        f"the real signatures — use them over anything you remember:\n"
        f"---\n{job['api_docs']}\n---\n"
        f"{retry_note}\n"
        f"report.py, in full:\n---\n{job['source']}\n---\n\n"
        f"Give the exact-string edits. Each `old` must be copied\n"
        f"character-for-character from the source above and appear exactly once.",
        schema=method.EDIT_SCHEMA,
        schema_name="edit",
    )


def _critique(ruling: dict) -> str:
    parts = [ruling["reasoning"]]
    for fraud in ruling["frauds"]:
        parts.append(f"{fraud['kind']}: {fraud['evidence']}")
    if ruling["smallest_fix"]:
        parts.append(f"Smallest fix: {ruling['smallest_fix']}")
    return "\n".join(parts)[:MAX_CRITIQUE_CHARS]


def _indent(text: str) -> str:
    return "\n".join(f"      {line}" for line in (text or "(nothing)").splitlines())


if __name__ == "__main__":
    istos.run()
