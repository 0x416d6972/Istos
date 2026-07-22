"""Model protocol and an OpenAI-compatible chat client.

The loop only needs :meth:`Model.complete`. Bring your own, or use
:class:`OpenAIChatModel` against OpenAI, LM Studio, vLLM, or anything else that
speaks ``/v1/chat/completions``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

import aiohttp

from istos.logging import get_logger

_logger = get_logger("agent.model")


@dataclass
class ToolCall:
    """One function call the model asked for."""

    id: str
    name: str
    arguments: Dict[str, Any]


@dataclass
class ModelReply:
    """What the model returned for one completion turn.

    ``model``, ``finish_reason``, and ``usage`` (``{prompt_tokens,
    completion_tokens, …}``) are optional telemetry an adapter may fill in; the
    loop puts them on its completion span. A custom model can leave them unset.
    """

    content: Optional[str] = None
    tool_calls: List[ToolCall] = field(default_factory=list)
    model: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None


@runtime_checkable
class Model(Protocol):
    async def complete(
        self,
        messages: List[dict],
        *,
        tools: Optional[List[dict]] = None,
    ) -> ModelReply:
        """Next assistant turn. ``tools`` is the OpenAI tools array, or None."""
        ...


class ModelError(RuntimeError):
    """The model was unreachable, or answered with something unusable."""


class OpenAIChatModel:
    """Thin ``/v1/chat/completions`` client (non-streaming, with tool calls).

    Uses aiohttp — already an Istos dependency — so no extra install for the
    common OpenAI-compatible local servers::

        model = OpenAIChatModel(
            base_url="http://127.0.0.1:1234/v1",
            model="qwen/qwen3.5-9b",
        )
    """

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: Optional[str] = None,
        timeout_s: float = 120.0,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        extra_body: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.extra_body = extra_body or {}

    async def complete(
        self,
        messages: List[dict],
        *,
        tools: Optional[List[dict]] = None,
    ) -> ModelReply:
        body: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            **self.extra_body,
        }
        if self.max_tokens is not None:
            body["max_tokens"] = self.max_tokens
        if tools:
            body["tools"] = tools
            body["tool_choice"] = "auto"

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.base_url}/chat/completions",
                    json=body,
                    headers=headers,
                ) as resp:
                    if resp.status != 200:
                        detail = await resp.text()
                        raise ModelError(
                            f"chat/completions returned {resp.status}: {detail[:400]}"
                        )
                    payload = await resp.json()
        except aiohttp.ClientError as exc:
            raise ModelError(
                f"Could not reach model at {self.base_url}: {exc}"
            ) from exc

        try:
            message = payload["choices"][0]["message"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ModelError(
                f"Unexpected response shape: {json.dumps(payload)[:400]}"
            ) from exc

        finish = payload["choices"][0].get("finish_reason")
        usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else None

        content = message.get("content")
        if isinstance(content, str):
            content = content.strip() or None
        else:
            content = None

        tool_calls: List[ToolCall] = []
        for raw in message.get("tool_calls") or []:
            try:
                fn = raw["function"]
                args_raw = fn.get("arguments") or "{}"
                if isinstance(args_raw, str):
                    arguments = json.loads(args_raw) if args_raw.strip() else {}
                elif isinstance(args_raw, dict):
                    arguments = args_raw
                else:
                    arguments = {}
                if not isinstance(arguments, dict):
                    arguments = {}
                tool_calls.append(
                    ToolCall(
                        id=str(raw.get("id") or uuid.uuid4().hex),
                        name=str(fn["name"]),
                        arguments=arguments,
                    )
                )
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                _logger.warning(
                    "Skipping malformed tool_call from model: %s", exc,
                    extra={"raw": raw},
                )
                continue

        return ModelReply(
            content=content,
            tool_calls=tool_calls,
            model=payload.get("model") or self.model,
            finish_reason=finish,
            usage=usage,
        )
