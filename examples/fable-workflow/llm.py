"""Talks to a local model through LM Studio.

LM Studio serves the OpenAI chat-completions shape, so this is a thin aiohttp
POST. aiohttp is already an Istos dependency, so the example needs nothing that
`pip install istos` did not already bring in.
"""

import json
import os
from typing import Any, AsyncIterator, Dict, List, Optional

import aiohttp

BASE_URL = os.environ.get("FABLE_LLM_URL", "http://127.0.0.1:1234/v1")
MODEL = os.environ.get("FABLE_LLM_MODEL", "qwen/qwen3.5-9b")

# A 9B model on consumer hardware is not fast, and the judge prompt carries two
# copies of the source. Give it room rather than tuning a tight timeout.
TIMEOUT_S = float(os.environ.get("FABLE_LLM_TIMEOUT_S", "300"))


class LLMError(RuntimeError):
    """The model was unreachable, or answered with something unusable."""


def _extract(message: Dict[str, Any]) -> str:
    """Pull the answer out of a chat message.

    Qwen3.5 is a hybrid reasoning model. When it thinks, LM Studio routes the
    whole answer into `reasoning_content` and leaves `content` an empty string,
    so reading `content` alone silently yields "". We ask for thinking to be off
    (see `ask`) and still fall back, because the split depends on the LM Studio
    build and the loaded model.
    """
    content = (message.get("content") or "").strip()
    if content:
        return content
    return (message.get("reasoning_content") or "").strip()


async def ask(
    system: str,
    user: str,
    *,
    schema: Optional[Dict[str, Any]] = None,
    schema_name: str = "reply",
    max_tokens: int = 1500,
    temperature: float = 0.0,
) -> Any:
    """Send one prompt and return the reply.

    With `schema`, LM Studio constrains decoding to that JSON Schema and the
    parsed object comes back. Without one, you get the raw text.
    """
    body: Dict[str, Any] = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        # Thinking costs minutes per phase at 9B and buys little on prompts this
        # constrained — every phase here answers into a fixed schema.
        "reasoning_effort": "none",
    }
    if schema is not None:
        body["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": schema_name, "strict": True, "schema": schema},
        }

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{BASE_URL}/chat/completions", json=body) as resp:
                if resp.status != 200:
                    detail = await resp.text()
                    raise LLMError(f"LM Studio returned {resp.status}: {detail[:400]}")
                payload = await resp.json()
    except aiohttp.ClientError as exc:
        raise LLMError(
            f"Could not reach LM Studio at {BASE_URL}. Is the server running "
            f"with {MODEL} loaded? ({exc})"
        ) from exc

    try:
        choice = payload["choices"][0]
        text = _extract(choice["message"])
    except (KeyError, IndexError) as exc:
        raise LLMError(f"Unexpected response shape: {json.dumps(payload)[:400]}") from exc

    # Say what actually went wrong. A reply cut off at the token limit is still
    # syntactically broken JSON, and reporting it as a parse failure sends you
    # hunting the schema instead of the budget.
    if choice.get("finish_reason") == "length":
        raise LLMError(
            f"The model hit the {max_tokens}-token limit mid-reply, so the answer is "
            f"truncated. It ran long — usually a sign the prompt let it ramble. "
            f"Got: {text[:200]}…"
        )

    if not text:
        raise LLMError("The model returned an empty reply.")
    if schema is None:
        return text
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise LLMError(f"Expected JSON for {schema_name}, got: {text[:400]}") from exc


async def stream_tokens(
    messages: List[Dict[str, str]],
    *,
    think: bool = False,
    max_tokens: int = 1024,
    temperature: float = 0.7,
) -> AsyncIterator[str]:
    """Yield tokens as the model produces them.

    `ask` waits for the whole answer because every caller in the method wants a
    finished object to validate. Chat is the opposite: the point is watching it
    arrive.

    LM Studio streams the OpenAI shape — `data: {...}` frames ending in
    `data: [DONE]`. The thinking split from `_extract` applies here too: with
    thinking on the tokens come through `delta.reasoning_content` and
    `delta.content` stays empty, so take whichever is populated.
    """
    body: Dict[str, Any] = {
        "model": MODEL,
        "messages": messages,
        "stream": True,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if not think:
        body["reasoning_effort"] = "none"

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{BASE_URL}/chat/completions", json=body) as resp:
                if resp.status != 200:
                    detail = await resp.text()
                    raise LLMError(f"LM Studio returned {resp.status}: {detail[:400]}")

                async for raw in resp.content:
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line.startswith("data:"):
                        continue
                    payload = line[5:].strip()
                    if payload == "[DONE]":
                        return
                    try:
                        delta = json.loads(payload)["choices"][0].get("delta", {})
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue  # keepalives and partial frames are not fatal
                    text = delta.get("content") or delta.get("reasoning_content") or ""
                    if text:
                        yield text
    except aiohttp.ClientError as exc:
        raise LLMError(
            f"Could not reach LM Studio at {BASE_URL}. Is the server running "
            f"with {MODEL} loaded? ({exc})"
        ) from exc


async def models() -> List[str]:
    """List what LM Studio currently has loaded. Used for the startup check."""
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{BASE_URL}/models") as resp:
                payload = await resp.json()
    except aiohttp.ClientError as exc:
        raise LLMError(f"Could not reach LM Studio at {BASE_URL}: {exc}") from exc
    return [m["id"] for m in payload.get("data", [])]


async def preflight() -> None:
    """Fail loudly at startup instead of three phases into a run."""
    available = await models()
    if MODEL not in available:
        raise LLMError(
            f"{MODEL} is not loaded in LM Studio. Loaded: {', '.join(available) or 'nothing'}. "
            f"Set FABLE_LLM_MODEL to one of those, or load {MODEL}."
        )
