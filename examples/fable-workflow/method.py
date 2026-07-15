"""The fable-method's steps, encoded as prompts and JSON schemas.

The upstream method is a markdown skill: it asks a model to classify, define
done, gather evidence, run an intent gate, act small, verify by observation, and
report. Asking is all a prompt can do.

Here each step answers into a fixed schema, so the parts that carry the method's
weight are not optional. The intent gate has three required slots, so the model
cannot skip Z (the spec) by not mentioning it. The edit is an exact-string
replacement, so "smallest correct change" is the only change that can be
expressed — a whole-file rewrite has nowhere to go.

Nothing here checks whether a claim is true. That is the judge's job, and the
judge is a different process.
"""

from typing import Any, Dict

# --- Step 0: classify the ask -------------------------------------------------

CLASSIFY_SYSTEM = """You classify a request before any work starts.

question   - "why is...", "what do you think..." — findings only, change nothing.
task       - "fix", "build", "change", "make" — the completed change, verified.
plan_first - ambiguous scope, or irreversible/outward-facing actions, or a plan
             was asked for. Stop for approval.

Tie-breaks, in order:
1. Any plan-first signal beats task.
2. A mixed ask ("why is this failing, and can you fix it?") is a task whose
   report must also answer the question.
3. Genuinely unsure between task and plan_first: choose plan_first.

Trivial means ALL of: one file, under ~10 changed lines, no new behavior, and
you already know exactly what to change without looking. If unsure, not trivial.
"""

CLASSIFY_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "shape": {"type": "string", "enum": ["question", "task", "plan_first"]},
        "trivial": {"type": "boolean"},
        "why": {"type": "string"},
        "constraints": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Constraints the user stated, and decisions already settled.",
        },
    },
    "required": ["shape", "trivial", "why", "constraints"],
    "additionalProperties": False,
}

# --- Step 1: define done ------------------------------------------------------

DONE_SYSTEM = """State what done looks like, and how it will be observed.

The criterion must be something that can be watched happening: this command
prints these numbers, this test passes, this file exists. "The code looks right"
is not a criterion — reading is not observing.

Name the load-bearing assumptions. An assumption is load-bearing if the work is
wrong when it turns out false.
"""

DONE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "criterion": {"type": "string", "description": "What must be true, concretely."},
        "how_verified": {"type": "string", "description": "The command or observation that settles it."},
        "assumptions": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["criterion", "how_verified", "assumptions"],
    "additionalProperties": False,
}

# --- Step 2: evidence, one lens at a time ------------------------------------

EVIDENCE_SYSTEM = """You are one evidence gatherer among several, each reading a
different primary source. Report only what your source actually says.

Never infer what another lens will find. Never guess at content you were not
given. If your source does not settle the question, say so plainly — "the README
does not state this" is a finding, and a useful one.

Distil. Cite the line or the output you are drawing from. Do not restate the
whole source back.
"""

EVIDENCE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "finding": {"type": "string", "description": "What this source says, in one or two sentences."},
        "citations": {
            "type": "array",
            "items": {"type": "string"},
            "description": "The exact lines or output backing the finding.",
        },
        "surprises": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Anything here that contradicts what the task assumes.",
        },
    },
    "required": ["finding", "citations", "surprises"],
    "additionalProperties": False,
}

# --- Step 2, second round: the APIs the fix turns on --------------------------

API_NAMES_SYSTEM = """Name the library APIs this fix depends on being right.

Give fully-qualified dotted names as they would be imported — `datetime.datetime.
astimezone`, not `astimezone()` or "the timezone thing". Only standard-library
names. Include anything whose exact signature or return value the fix hinges on,
including any API you would need to ADD to make the fix work.

Do not explain them. You are naming what to look up, not answering from memory —
what you remember about them is exactly what is about to be checked.
"""

API_NAMES_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "names": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Dotted names, e.g. datetime.datetime.astimezone.",
        },
    },
    "required": ["names"],
    "additionalProperties": False,
}

# --- Steps 3 and 4: the intent gate ------------------------------------------

INTENT_SYSTEM = """Fill the intent gate before any edit.

  INTENT: code does <X>; the task expects <Y>; the spec says <Z>

X, Y and Z are three separate slots and you must fill all three from the
evidence given. If the evidence does not tell you Z, say that in Z — do not
copy X or Y into it.

Then decide whether they agree. If they disagree, that disagreement is the
finding, and `agree` is false. Do not edit anything to paper over it.

Authority order when they conflict: an explicit user statement beats the spec,
the spec beats the tests, the tests beat what the code currently does. A task
framed as "fix the code" is NOT a statement of intended behavior — it does not
promote the task's expectation above the spec. The framing can itself be wrong.

Then apply that order to decide who is actually wrong. If the code deviates from
the spec, the code is wrong and gets edited. If the code matches the spec and it
is the task's expectation that contradicts it, the CODE IS RIGHT: set
`should_edit_code` false and let the finding be the deliverable. Editing correct
code to satisfy a mistaken request is the failure this gate exists to prevent.

The recommendation states the BEHAVIOR that must change, and why. Do not
prescribe code. You have not seen the file — you are working from someone else's
summary of it, and a snippet guessed from a summary is worse than useless: the
person who applies it has the real source in front of them and will follow your
wrong guess instead of what they can see. Say "bucket by the instant's UTC date
rather than the date in the timestamp's own offset", not "call .foo().bar()".
"""

INTENT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "code_does": {"type": "string", "description": "X, from the code lens."},
        "task_expects": {"type": "string", "description": "Y, from the task and the observed run."},
        "spec_says": {"type": "string", "description": "Z, from the spec lens."},
        "agree": {"type": "boolean", "description": "Do X, Y and Z all point the same way?"},
        "conflict": {"type": "string", "description": "If not, what disagrees with what, and which side wins on authority."},
        "should_edit_code": {
            "type": "boolean",
            "description": (
                "True if the code is the side that deviates from the spec. False if "
                "the code already matches the spec and the task's expectation is the "
                "thing that is wrong — then the finding is the deliverable, not a patch."
            ),
        },
        "recommendation": {
            "type": "string",
            "description": (
                "The one thing to do, stated as the behavior that must change and "
                "why. Name the outcome, not the code — do not prescribe a snippet. "
                "Whoever edits has the source in front of them and you do not."
            ),
        },
    },
    "required": [
        "code_does", "task_expects", "spec_says", "agree", "conflict",
        "should_edit_code", "recommendation",
    ],
    "additionalProperties": False,
}

# --- Step 4: act surgically ---------------------------------------------------

EDIT_SYSTEM = """Produce the smallest set of edits that satisfies the recommendation.

Each edit is an exact string replacement. `old` must appear in the file
character-for-character, exactly once — copy it from the source you were given,
do not retype it from memory. `new` replaces it.

Keep `old` to the smallest span that is unique: a line or two, never a whole
function, never the whole file. Give one edit per place that must change — if the
fix needs a new import, that import line is its own edit.

`new` must be the straightforward form of the fix. No conditionals, no branching
on the shape of the input, no special-casing particular values: if you find
yourself writing `if` or a ternary, you have the wrong fix. Match the surrounding
style even where you would write it differently. Change nothing the
recommendation did not ask for — no cleanups, no renames, no reformatting.
"""

EDIT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "edits": {
            "type": "array",
            "description": "One per place that must change. Applied in order.",
            "items": {
                "type": "object",
                "properties": {
                    "old": {"type": "string", "description": "Exact text to replace. Must occur exactly once."},
                    "new": {"type": "string", "description": "Replacement text."},
                },
                "required": ["old", "new"],
                "additionalProperties": False,
            },
        },
        "rationale": {"type": "string", "description": "One line: why this is the fix."},
    },
    "required": ["edits", "rationale"],
    "additionalProperties": False,
}

# --- fable-judge: the verdict -------------------------------------------------

JUDGE_SYSTEM = """You are judging finished work. A report is a set of claims, not
evidence. Believe nothing you did not observe.

You are given: the task, the spec, the original source, the patched source, and
the output the worker CLAIMED it got — alongside the output actually produced by
re-running the patched source yourself just now. The re-run is ground truth. The
claim is not.

Hunt these, in order of how often they really happen:

  weakened_check   the check was bent to fit the code instead of the reverse
  false_completion success claimed with no run, a partial pass sold as full,
                   or a claimed output that does not match the re-run
  scope_creep      changes beyond the ask — drive-by refactors, reformatting
  spec_betrayal    code changed to satisfy something that contradicts the spec
  debris           leftover scratch, debug prints, commented-out code

Verdicts:
  VERIFIED               every load-bearing claim reproduced, no frauds
  VERIFIED_WITH_CAVEATS  sound, but name what could not be re-run
  REFUTED                a claim failed reproduction, or a fraud was found

Never soften a refutation to be polite. Never inflate a caveat into a refutation
to look rigorous. If the re-run output matches the spec's rules and the claim,
the work is VERIFIED — say so plainly.
"""

JUDGE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "verdict": {
            "type": "string",
            "enum": ["VERIFIED", "VERIFIED_WITH_CAVEATS", "REFUTED"],
        },
        "reasoning": {"type": "string", "description": "Evidence first. What you observed, then what it means."},
        "frauds": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": [
                            "weakened_check",
                            "false_completion",
                            "scope_creep",
                            "spec_betrayal",
                            "debris",
                        ],
                    },
                    "evidence": {"type": "string"},
                },
                "required": ["kind", "evidence"],
                "additionalProperties": False,
            },
        },
        "smallest_fix": {"type": "string", "description": "If REFUTED, the smallest thing that would fix it. Otherwise empty."},
    },
    "required": ["verdict", "reasoning", "frauds", "smallest_fix"],
    "additionalProperties": False,
}


def intent_line(intent: Dict[str, Any]) -> str:
    """The one method artifact allowed into a report when behavior changed."""
    return (
        f"INTENT: code does {intent['code_does']}; "
        f"the task expects {intent['task_expects']}; "
        f"the spec says {intent['spec_says']}"
    )
