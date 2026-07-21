"""Evidence gatherers — one lens per primary source.

    python evidence.py

Each lens has its own queue, so a node only ever claims work it can actually do.
Run a second copy and the two compete for the same queues, which is how you add
capacity — no coordination, the owner is the arbiter.

The three main lenses are not arbitrary. The method's intent gate reads:

    INTENT: code does <X>; the task expects <Y>; the spec says <Z>

so there is one lens per slot: `code` reads the source (X), `runtime` runs it and
watches (Y), `spec` reads the stated rules (Z). Each sees only its own source and
is told to report only what that source says. None can quietly fill a slot with a
guess about another's, because none is shown the others.

That separation is the point. A single model asked to fill all three slots will
happily derive Z from X — it reads the code, decides that must have been the
intent, and the gate passes while the spec was never opened. Three nodes with
three inputs cannot do that.

The fourth lens, `api`, answers the follow-up round and asks the model nothing.
"""

from contextlib import asynccontextmanager

import apidocs
import llm
import method
import queues
import sandbox
from istos import Istos

istos = Istos(service_name="fable-evidence")

# A distillation should be short, but "short" at a 9B is not 1500 tokens: the code
# lens in particular fills `finding`, `citations` and `surprises` and was running
# past the default budget, so LM Studio returned finish_reason=length and the job
# dead-lettered. temperature=0 makes that deterministic — every retry hit the same
# wall — so the budget is the thing to move, not the retry count.
EVIDENCE_TOKENS = 3000


@asynccontextmanager
async def on_start(app):
    # Fail here rather than three phases into someone else's run.
    await llm.preflight()
    print("evidence node up — spec, code, runtime, api", flush=True)
    yield


istos.lifespan = on_start


@istos.worker(queues.evidence_queue("spec"))
async def spec_lens(job):
    """Z — what the rules actually say. Never sees the code."""
    print("  [evidence:spec] gathering…", flush=True)
    found = await llm.ask(
        method.EVIDENCE_SYSTEM,
        f"Your lens: the SPEC. You are reading the project's stated rules.\n"
        f"You have NOT seen the code and must not speculate about it.\n\n"
        f"The task someone was given:\n{job['task']}\n\n"
        f"README.md:\n---\n{job['spec']}\n---\n\n"
        f"What do the rules require of the daily buckets? Quote the exact rule.",
        schema=method.EVIDENCE_SCHEMA,
        schema_name="evidence",
        max_tokens=EVIDENCE_TOKENS,
    )
    return _done("spec", found)


@istos.worker(queues.evidence_queue("code"))
async def code_lens(job):
    """X — what the code does. Never sees the spec."""
    print("  [evidence:code] gathering…", flush=True)
    found = await llm.ask(
        method.EVIDENCE_SYSTEM,
        f"Your lens: the CODE. You are reading the source as written.\n"
        f"You have NOT seen the spec. Do not guess what it is supposed to do —\n"
        f"report what it does do.\n\n"
        f"The task someone was given:\n{job['task']}\n\n"
        f"report.py:\n---\n{job['source']}\n---\n\n"
        f"How does this bucket each event into a day? Name the exact call that\n"
        f"decides the day, and say which day it yields for a timestamp written\n"
        f"with a +09:00 offset.",
        schema=method.EVIDENCE_SCHEMA,
        schema_name="evidence",
        max_tokens=EVIDENCE_TOKENS,
    )
    return _done("code", found)


@istos.worker(queues.evidence_queue("runtime"))
async def runtime_lens(job):
    """Y — what actually comes out. Runs it rather than reasoning about it."""
    print("  [evidence:runtime] running it…", flush=True)
    run = await sandbox.run_report(job["source"], job["fixtures"])

    found = await llm.ask(
        method.EVIDENCE_SYSTEM,
        f"Your lens: the RUNTIME. This output is not a prediction — it is what\n"
        f"the program printed just now.\n\n"
        f"The task someone was given:\n{job['task']}\n\n"
        f"$ python report.py\n---\n{run['stdout'] or '(no output)'}\n---\n"
        f"exit code: {run['exit_code']}\n"
        f"{('stderr: ' + str(run['stderr'])) if run['stderr'] else ''}\n\n"
        f"Report the observed counts per day, and whether they match what the\n"
        f"task says the reference shows.",
        schema=method.EVIDENCE_SCHEMA,
        schema_name="evidence",
        max_tokens=EVIDENCE_TOKENS,
    )
    # The raw run travels with the distillation. Downstream compares against the
    # numbers, not the model's paraphrase of them.
    found["observed"] = run
    return _done("runtime", found)


@istos.worker(queues.evidence_queue("api"))
async def api_lens(job):
    """What the APIs really are, read off the installed stdlib.

    The only lens that asks the model nothing. The names come in from the
    follow-up round and go straight to `inspect`; there is no judgement to make
    about what a signature says, and inviting one would just add a second chance
    to misremember it.
    """
    print(f"  [evidence:api] looking up {len(job['names'])} name(s)…", flush=True)
    entries = apidocs.look_up_all(job["names"])
    bogus = [e["name"] for e in entries if e["status"] == "not_found"]
    return _done(
        "api",
        {
            "finding": f"Looked up {len(entries)} API name(s) against this interpreter's stdlib.",
            "citations": [apidocs.render(entries)],
            "surprises": [f"{name} does not exist — it was misremembered" for name in bogus],
            "entries": entries,
        },
    )


def _done(lens: str, found: dict) -> dict:
    found["lens"] = lens
    print(f"  [evidence:{lens}] {found['finding'][:90]}", flush=True)
    return found


if __name__ == "__main__":
    istos.run()
