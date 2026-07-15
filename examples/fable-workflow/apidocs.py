"""Looks up what an API actually is, from the installed stdlib.

The method's evidence rule is blunt about this: never invent an API signature
from recall; read the current docs, or failing that the installed package source.
A model's memory of a signature is the single most reliable place for a plausible
patch to go wrong, because the wrongness is invisible — the code reads fine and
fails at runtime, or worse, runs and quietly does something else.

Nothing here asks a model anything. It resolves a dotted name against the
interpreter that is going to run the code and reports the real signature and the
real docstring. A name that will not resolve is the most useful answer it can
give: the model made it up, and that is the finding.
"""

import importlib
import inspect
import sys
from typing import Any, Dict, List

MAX_DOC_CHARS = 700


def _root_is_stdlib(dotted: str) -> bool:
    """Only stdlib names get looked up.

    The names come from a model, and resolving one means importing it. Against a
    stdlib-only fixture this costs nothing and keeps a hallucinated
    `os.system`-adjacent import from being executed on a whim.
    """
    root = dotted.split(".")[0]
    return root in sys.stdlib_module_names


def _resolve(dotted: str) -> Any:
    """Find the object behind a dotted name.

    The split between module path and attribute path is not knowable up front —
    `datetime.datetime.astimezone` is module `datetime`, then two attributes — so
    try the longest importable prefix and walk the rest with getattr.
    """
    parts = dotted.split(".")
    for i in range(len(parts), 0, -1):
        try:
            module = importlib.import_module(".".join(parts[:i]))
        except ImportError:
            continue
        obj: Any = module
        for attr in parts[i:]:
            obj = getattr(obj, attr)  # AttributeError → the name is wrong
        return obj
    raise ImportError(f"no importable module in {dotted!r}")


def look_up(dotted: str) -> Dict[str, str]:
    """Resolve one name. Never raises — a failed lookup is a result."""
    if not _root_is_stdlib(dotted):
        return {
            "name": dotted,
            "status": "skipped",
            "detail": f"{dotted.split('.')[0]!r} is not a stdlib module; not looked up.",
        }
    try:
        obj = _resolve(dotted)
    except (ImportError, AttributeError) as exc:
        return {
            "name": dotted,
            "status": "not_found",
            "detail": f"This name does not exist. It was misremembered. ({exc})",
        }

    try:
        signature = f"{dotted.split('.')[-1]}{inspect.signature(obj)}"
    except (TypeError, ValueError):
        signature = repr(obj)  # constants and C-level objects have no signature

    doc = (inspect.getdoc(obj) or "(no docstring)").strip()
    if len(doc) > MAX_DOC_CHARS:
        doc = doc[:MAX_DOC_CHARS] + "…"

    return {"name": dotted, "status": "found", "signature": signature, "detail": doc}


def look_up_all(names: List[str]) -> List[Dict[str, str]]:
    return [look_up(name) for name in names]


def render(entries: List[Dict[str, str]]) -> str:
    """Format the lookups for a prompt."""
    lines = []
    for entry in entries:
        if entry["status"] == "found":
            lines.append(f"{entry['name']}\n  signature: {entry['signature']}\n  {entry['detail']}")
        else:
            lines.append(f"{entry['name']}\n  [{entry['status'].upper()}] {entry['detail']}")
    return "\n\n".join(lines) or "(nothing looked up)"
