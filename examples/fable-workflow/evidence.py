"""Evidence gatherers — one lens per primary source.

Run one of these, or three on three machines. Each lens has its own queue, so a
node only ever claims work it can actually do; a claim is blind, and a single
shared queue would hand a `spec` node a `code` job that it could only nack.

The three lenses are not arbitrary. The method's intent gate reads:

    INTENT: code does <X>; the task expects <Y>; the spec says <Z>

so there is one lens per slot: `code` reads the source (X), `runtime` runs it and
watches (Y), `spec` reads the stated rules (Z). Each sees only its own source and
is told to report only what that source says. None can quietly fill a slot with a
guess about another's, because none is shown the others.

That separation is the point. A single model asked to fill all three slots will
happily derive Z from X — it reads the code, decides that must have been the
intent, and the gate passes while the spec was never opened. Three nodes with
three inputs cannot do that.

Usage:
    python evidence.py                # serve all three lenses
    python evidence.py --lens spec    # serve one, to spread across machines
"""

import argparse
import asyncio

import apidocs
import llm
import method
import sandbox
from istos import Istos

LENSES = ("spec", "code", "runtime", "api")


def queue_for(lens: str) -> str:
    return f"jobs/fable/evidence/{lens}"


async def gather_spec(job):
    """Z — what the rules actually say. Never sees the code."""
    return await llm.ask(
        method.EVIDENCE_SYSTEM,
        f"Your lens: the SPEC. You are reading the project's stated rules.\n"
        f"You have NOT seen the code and must not speculate about it.\n\n"
        f"The task someone was given:\n{job['task']}\n\n"
        f"README.md:\n---\n{job['spec']}\n---\n\n"
        f"What do the rules require of the daily buckets? Quote the exact rule.",
        schema=method.EVIDENCE_SCHEMA,
        schema_name="evidence",
    )


async def gather_code(job):
    """X — what the code does. Never sees the spec."""
    return await llm.ask(
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
    )


async def gather_runtime(job):
    """Y — what actually comes out. Runs it rather than reasoning about it."""
    run = await sandbox.run_report(job["source"], job["fixtures"])

    distilled = await llm.ask(
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
    )
    # The raw run travels with the distillation. Downstream compares against the
    # numbers, not the model's paraphrase of them.
    distilled["observed"] = run
    return distilled


async def gather_api(job):
    """What the APIs really are, read off the installed stdlib.

    The only lens that asks the model nothing. The names come in from the
    follow-up round and go straight to `inspect`; there is no judgement to make
    about what a signature says, and inviting one would just add a second chance
    to misremember it.
    """
    entries = apidocs.look_up_all(job["names"])
    bogus = [e["name"] for e in entries if e["status"] == "not_found"]
    return {
        "finding": f"Looked up {len(entries)} API name(s) against this interpreter's stdlib.",
        "citations": [apidocs.render(entries)],
        "surprises": [f"{name} does not exist — it was misremembered" for name in bogus],
        "entries": entries,
    }


GATHERERS = {
    "spec": gather_spec,
    "code": gather_code,
    "runtime": gather_runtime,
    "api": gather_api,
}


def build_app(lenses) -> Istos:
    app = Istos(service_name=f"fable-evidence-{'-'.join(lenses)}")

    for lens in lenses:
        # Bind late: the loop variable must not leak into the closure.
        def make(lens=lens):
            @app.worker(queue_for(lens))
            async def gather(job):
                print(f"  [evidence:{lens}] gathering…", flush=True)
                result = await GATHERERS[lens](job)
                result["lens"] = lens
                print(f"  [evidence:{lens}] {result['finding'][:90]}", flush=True)
                return result

        make()

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Fable evidence gatherer")
    parser.add_argument(
        "--lens",
        choices=(*LENSES, "all"),
        default="all",
        help="Which lens this node serves. Default: all three in one process.",
    )
    args = parser.parse_args()
    lenses = LENSES if args.lens == "all" else (args.lens,)

    asyncio.run(llm.preflight())
    print(f"evidence node up — serving {', '.join(lenses)}", flush=True)
    build_app(lenses).run()


if __name__ == "__main__":
    main()
