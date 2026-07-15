"""Runs the scenario and reports what actually came out.

This is the part of the method that no prompt can do: "verify by observation"
means something ran and was watched. Reading code and nodding is not verifying.

Both the executor and the judge run the report through here. They do it from
source carried in the message, not from a shared directory — mesh nodes have no
filesystem in common, and a judge that trusted the executor's working copy would
be trusting the thing it is supposed to doubt.

The subprocess is a plain `python report.py` with a timeout. It is NOT a security
sandbox: it runs whatever the model proposed, with this process's permissions.
Fine against a fixture you can read; not a pattern to lift into production.
"""

import asyncio
import sys
import tempfile
from pathlib import Path
from typing import Dict, List

RUN_TIMEOUT_S = 30


async def run_report(source: str, fixtures: Dict[str, str]) -> Dict[str, object]:
    """Write `source` as report.py alongside `fixtures`, run it, return what happened.

    Returns stdout, stderr and the exit code. A crash is a result, not an
    exception — a traceback is evidence the caller needs to see.
    """
    with tempfile.TemporaryDirectory(prefix="fable-run-") as tmp:
        workdir = Path(tmp)
        (workdir / "report.py").write_text(source)
        for name, content in fixtures.items():
            (workdir / name).write_text(content)

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "report.py",
                cwd=str(workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=RUN_TIMEOUT_S
            )
        except asyncio.TimeoutError:
            proc.kill()
            return {
                "stdout": "",
                "stderr": f"timed out after {RUN_TIMEOUT_S}s",
                "exit_code": -1,
            }

        return {
            "stdout": stdout.decode(errors="replace").strip(),
            "stderr": stderr.decode(errors="replace").strip(),
            "exit_code": proc.returncode,
        }


def apply_edits(source: str, edits: List[Dict[str, str]]) -> str:
    """Apply exact-string replacements in order, or refuse.

    The method's "precise edits over rewrites" is enforced here rather than
    requested: an `old` that does not appear, or appears more than once, is a
    failed edit and not a licence to rewrite the file.

    A fix often needs more than one edit — the line that was wrong, plus the
    import it now needs. That is still surgical. What is not expressible is
    replacing the file, because every edit has to name text that is already
    there.
    """
    if not edits:
        raise ValueError("no edits proposed")

    patched = source
    for i, edit in enumerate(edits, 1):
        old, new = edit["old"], edit["new"]
        if not old:
            raise ValueError(f"edit {i} had an empty `old`; that is a rewrite, not an edit")
        occurrences = patched.count(old)
        if occurrences == 0:
            raise ValueError(
                f"edit {i} did not apply: `old` does not appear in the source. "
                f"It was probably retyped from memory rather than copied. Wanted:\n{old}"
            )
        if occurrences > 1:
            raise ValueError(
                f"edit {i} is ambiguous: `old` appears {occurrences} times; "
                f"it needs enough surrounding context to be unique. Wanted:\n{old}"
            )
        patched = patched.replace(old, new)
    return patched
