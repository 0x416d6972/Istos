# Examples

Working Istos systems, not snippets. Each one runs, and each is built around a
problem that actually needs a mesh — if it would be simpler as a single script,
it does not belong here.

| Example | What it shows | Needs |
|---|---|---|
| [**fable-workflow**](fable-workflow/) | The [Fable Method](https://github.com/Sahir619/fable-method) as four cooperating nodes: parallel evidence gathering, an intent gate, and an adversarial judge in its own process. Work queues carry every payload, and `max_attempts=3` *is* the method's "stop after three failed cycles". | An OpenAI-compatible LLM endpoint (LM Studio, Ollama, vLLM, or a hosted API) |

## Running one

Every example is plain Python. Install Istos, then follow the example's own
README — most start several nodes, one per terminal, because that is the point:

```bash
pip install istos      # or: uv pip install istos
cd fable-workflow
python evidence.py     # …and the rest, in their own terminals
```

Istos is brokerless, so there is nothing to stand up first. Nodes discover each
other over Zenoh peer scouting on the local network. That also means the defaults
are **unauthenticated** — fine on your laptop, not fine anywhere else. See
[Security](../docs/user-guide/security.md) before pointing any of this at a real
network.

Examples are deliberately outside the test and lint gates, so they are free to
include fixtures with real bugs in them. `fable-workflow/scenario/` ships one on
purpose.
