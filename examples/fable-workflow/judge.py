"""fable-judge as a node of its own.

    python judge.py

The judge's stance is that a report is a set of claims, not evidence. That stance
is hard to hold when the thing reporting and the thing judging are the same model
in the same context — it has already decided the work is good, and it grades its
own homework with its own reasoning still in front of it.

Here the judge is a different process. It receives the task, the spec, both
versions of the source, and the output the executor CLAIMED. It shares no
filesystem with the executor, so it cannot inspect the executor's working copy
even if it wanted to. It writes the patched source into its own scratch directory
and runs it. That re-run is ground truth.

Verification has two halves, and the second is easy to lose: the done criterion
must pass, observed — AND the work must not have broken anything around it. A
judge that only checks "did it do what it said" will wave through a patch that
ran cleanly, reported honestly, and produced the wrong answer. That is not a
hypothetical: it is what this judge did before it was given the criterion, and it
wrote a paragraph of fluent reasoning about why the wrong numbers were fine.

So three things are decided in code rather than by the model, because they are
matters of fact:

  - the claimed output not matching the re-run is false completion, exactly;
  - a non-zero exit code is a failure, exactly;
  - the observed output not matching the reference the task stated is a failed
    done criterion, exactly.

Asking a 9B model to rule on those would be asking it to re-derive `!=`, and it
gets it wrong in a way that reads convincingly. The model is asked only what
genuinely needs reading: does the change betray the spec, and does the diff go
beyond the ask.
"""

import difflib
from contextlib import asynccontextmanager

import llm
import method
import queues
import sandbox
from istos import Istos

istos = Istos(service_name="fable-judge")


@asynccontextmanager
async def on_start(app):
    await llm.preflight()
    print("judge node up", flush=True)
    yield


istos.lifespan = on_start


def unified_diff(original: str, patched: str) -> str:
    lines = difflib.unified_diff(
        original.splitlines(keepends=True),
        patched.splitlines(keepends=True),
        fromfile="report.py (original)",
        tofile="report.py (patched)",
        n=3,
    )
    return "".join(lines) or "(no change)"


@istos.worker(queues.JUDGE)
async def judge(job):
    """Re-run the work, then rule on it."""
    print("  [judge] re-running the claim…", flush=True)

    rerun = await sandbox.run_report(job["patched_source"], job["fixtures"])
    actual = str(rerun["stdout"])
    claimed = str(job["claimed_output"])
    reference = str(job["reference_output"])
    diff = unified_diff(job["original_source"], job["patched_source"])

    frauds = []
    if actual != claimed:
        frauds.append(
            {
                "kind": "false_completion",
                "evidence": (
                    f"The executor claimed this output:\n{claimed or '(nothing)'}\n\n"
                    f"Re-running the patched source produces:\n{actual or '(nothing)'}"
                ),
            }
        )
    if rerun["exit_code"] != 0:
        frauds.append(
            {
                "kind": "false_completion",
                "evidence": (
                    f"The patched source exits {rerun['exit_code']}.\n"
                    f"stderr:\n{rerun['stderr']}"
                ),
            }
        )

    # Half (a) of verification: the done criterion, observed. Honesty is not
    # correctness — an executor can report exactly what it got and still be wrong.
    criterion_met = actual == reference
    failures = []
    if not criterion_met:
        failures.append(
            f"The done criterion does not pass. The task states the reference is:\n"
            f"{reference}\n\nRe-running the patched source gives:\n{actual or '(nothing)'}"
        )

    ruling = await llm.ask(
        method.JUDGE_SYSTEM,
        f"TASK GIVEN TO THE WORKER:\n{job['task']}\n\n"
        f"THE SPEC (README.md), which outranks the task framing:\n---\n{job['spec']}\n---\n\n"
        f"THE DIFF the worker produced:\n---\n{diff}\n---\n\n"
        f"ITS STATED INTENT:\n{job['intent_line']}\n\n"
        f"THE DONE CRITERION — what the task says the reference shows:\n"
        f"---\n{reference}\n---\n\n"
        f"WHAT YOU OBSERVED BY RE-RUNNING IT JUST NOW (ground truth):\n"
        f"---\n{actual or '(nothing)'}\n---\n"
        f"exit code: {rerun['exit_code']}\n"
        f"matches the reference: {criterion_met}\n\n"
        f"Rule on it. Do not re-derive whether the output matches the reference —\n"
        f"that is settled above. Say WHY the diff produces what it produces, check\n"
        f"it against the spec's bucketing rule, and check the diff for changes the\n"
        f"task did not ask for.",
        schema=method.JUDGE_SCHEMA,
        schema_name="verdict",
    )

    # What was established in code is not up for discussion. The model's verdict
    # stands only where the model was the one deciding.
    if frauds or failures:
        ruling["verdict"] = "REFUTED"
        ruling["frauds"] = frauds + list(ruling.get("frauds") or [])
        ruling["reasoning"] = "\n".join(failures + [ruling["reasoning"]])
        if not ruling.get("smallest_fix"):
            ruling["smallest_fix"] = "Make the output match the reference."

    ruling["observed_output"] = actual
    ruling["criterion_met"] = criterion_met
    ruling["diff"] = diff

    marks = ", ".join(f["kind"] for f in ruling["frauds"]) or "none"
    print(f"  [judge] {ruling['verdict']} (frauds: {marks})", flush=True)
    return ruling


if __name__ == "__main__":
    istos.run()
