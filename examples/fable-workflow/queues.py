"""The queue topology, in one place.

Every step of the loop that carries a payload is a job rather than an RPC. That
is not stylistic: `@query` passes its parameters in the Zenoh *selector*
(`prefix?a=1;b=2`, URL-quoted), which is the right shape for "move 10cm" and the
wrong shape for a source file. Jobs carry a serialized body, so the source, the
fixtures and the diff travel intact.

It buys the method's thresholds too, which is the better reason. ACT is declared
`max_attempts=3`, and one delivery is one fix-verify cycle — so the method's
"after 3 failed fix-verify cycles, stop and hand back" is the queue's retry bound
rather than a sentence the model is asked to remember. When it is spent, the job
dead-letters with its attempt count and last error, which is the hand-back.
"""

import asyncio
import time
from typing import Any, Dict

EVIDENCE = "jobs/fable/evidence"  # + /spec, /code, /runtime — one per lens
ACT = "jobs/fable/act"
JUDGE = "jobs/fable/judge"

# The method's hard bound: three failed fix-verify cycles and you stop.
MAX_FIX_VERIFY_CYCLES = 3


def evidence_queue(lens: str) -> str:
    return f"{EVIDENCE}/{lens}"


class JobFailed(RuntimeError):
    """A job dead-lettered, or aged out before it finished."""


async def wait_for(app: Any, prefix: str, job_id: str, *, timeout_s: float = 900.0) -> Dict:
    """Block until a job finishes, then hand back its result.

    Polls rather than subscribes: `result()` is the framework's read-side for a
    `keep_results` queue, and a local model takes minutes per phase, so a 1s tick
    costs nothing next to the work it is waiting on.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        record = await app.result(prefix, job_id)
        state = record["state"]
        if state == "done":
            return record["result"]
        if state == "dead":
            raise JobFailed(f"{prefix} job {job_id} dead-lettered")
        if state == "unknown":
            raise JobFailed(
                f"{prefix} job {job_id} is unknown — its result aged out, or no "
                f"owner holds it. Is the orchestrator still running?"
            )
        await asyncio.sleep(1.0)
    raise JobFailed(f"{prefix} job {job_id} did not finish within {timeout_s:.0f}s")
