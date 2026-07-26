"""Turning free text into a tool name or a key chunk.

Two directions, one place. A ``@handle`` prefix becomes the tool name an LLM
sees (:func:`tool_name`) — the MCP adapter and the agent loop's mesh tools must
agree on it, since a model that discovered a tool over MCP and an agent that
calls it over Zenoh have to arrive at the same name. And free text (a service
name) becomes a Zenoh key chunk (:func:`key_chunk`), where selector syntax and
the chunk separator are what cannot survive.
"""

from __future__ import annotations

import re

# OpenAI and MCP tool names allow [A-Za-z0-9_-]. Key expressions use '/' as the
# chunk separator and may carry '.' or other free text, none of which survive.
_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9_-]")

# `*`, `?`, `#`, `$` are selector syntax and `/` separates chunks, so free text
# cannot be dropped into a key as-is. Dots are legal inside a chunk.
_UNSAFE_CHUNK = re.compile(r"[^A-Za-z0-9_.-]")


def tool_name(prefix: str) -> str:
    """The tool name for a key expression (or an agent name).

    Every character outside ``[A-Za-z0-9_-]`` becomes ``-``, so ``math/add``
    is ``math-add``. Distinct prefixes can collide (``a/b`` and ``a.b`` both give
    ``a-b``); pass an explicit ``name`` to :class:`~istos.agent.MeshTool` when
    that matters.
    """
    return _UNSAFE_NAME.sub("-", prefix)


def key_chunk(text: str, *, default: str = "istos") -> str:
    """One safe chunk of a Zenoh key expression, built from free text.

    Selector characters and separators become ``-``; a value that scrubs away to
    nothing (or to dashes alone) falls back to ``default``.
    """
    chunk = _UNSAFE_CHUNK.sub("-", text or "").strip("-")
    return chunk or default
