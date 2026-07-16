"""Your model, live, over the mesh.

    python chat.py
    curl -N --get http://127.0.0.1:8081/chat --data-urlencode "prompt=who are you?"

The rest of this example uses the model as machinery: every call answers into a
schema and nobody watches it think. This node is the other way round — one prompt
in, tokens out as they land.

Two doors onto the same model, and the choice is about direction, not transport:

  @stream  → one prompt, tokens back. SSE over HTTP, a queryable on the fabric.
             Stateless: it never remembers the last thing you asked.
  @channel → full duplex. The session stays open, so it holds the conversation
             and you can keep talking. WebSocket over HTTP.

`@stream` is what curl can drive, so it is the one to reach for first. `@channel`
is what a chat UI wants.

This node shares nothing with the workflow but the LM Studio client — separate
process, separate port, its own keys on the fabric. Run it alongside the others
or on its own; it does not need the queues and the queues do not need it.
"""

import os
from contextlib import asynccontextmanager

import llm
from istos import ChannelSession, Istos

CHAT_PORT = int(os.environ.get("FABLE_CHAT_PORT", "8081"))

SYSTEM = (
    "You are a helpful assistant running locally on the user's own machine. "
    "Answer plainly and briefly. Say when you do not know something."
)

istos = Istos(service_name="fable-chat", http_port=CHAT_PORT)


@asynccontextmanager
async def on_start(app):
    await llm.preflight()
    print(
        f"chat node up — {llm.MODEL}\n"
        f"  curl -N --get http://127.0.0.1:{CHAT_PORT}/chat "
        f'--data-urlencode "prompt=hello"',
        flush=True,
    )
    yield


istos.lifespan = on_start


@istos.stream("fable/chat", http="GET /chat", http_timeout_s=300.0)
async def chat(prompt: str, think: bool = False):
    """One prompt, tokens back as they arrive.

    `think=1` streams the model's reasoning instead of hiding it — Qwen3.5 is a
    hybrid, and with thinking on LM Studio routes the whole answer through the
    reasoning channel, so you get the working rather than the conclusion.
    """
    print(f"  [chat] {prompt[:70]}", flush=True)
    async for token in llm.stream_tokens(
        [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": prompt},
        ],
        think=think,
    ):
        yield token


@istos.channel("fable/chat/session", ws="/ws")
async def session(s: ChannelSession):
    """A conversation. The history lives here, for as long as the socket is open.

    This is the difference from `/chat`: the handler is still running between
    your turns, so "and what about the second one?" means something. Close the
    socket and the history goes with it — persistence is `durable=True` plus a
    SessionStore, which this example does not need.
    """
    print("  [chat] session opened", flush=True)
    history = [{"role": "system", "content": SYSTEM}]

    async for message in s:
        text = message.get("text") if isinstance(message, dict) else str(message)
        if not text:
            await s.send({"error": "send {\"text\": \"...\"}"})
            continue

        print(f"  [chat] {text[:70]}", flush=True)
        history.append({"role": "user", "content": text})

        reply = ""
        async for token in llm.stream_tokens(history):
            reply += token
            await s.send({"token": token})

        # Only the finished turn goes into the history — the tokens were the view.
        history.append({"role": "assistant", "content": reply})
        await s.send({"done": True})

    print("  [chat] session closed", flush=True)


if __name__ == "__main__":
    istos.run()
