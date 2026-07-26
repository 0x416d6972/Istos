# CLI

Istos installs an `istos` console script.

```bash
istos --help
```

## `istos version`

Print the installed package version:

```bash
istos version
# istos 0.1.0
```

## `istos new`

Scaffold a minimal service:

```bash
istos new my-service
```

Creates:

```
my-service/
  main.py        # @handle("service/status") + istos.run()
  test_main.py   # IstosTestClient example
```

```bash
cd my-service
uv pip install istos pytest pytest-asyncio
pytest test_main.py
python main.py
```

## `istos analyze`

Measure the structural health of a package — abstractness, instability, distance
from the main sequence, dependency cycles, and god-module candidates:

```bash
istos analyze
istos analyze --no-cycles --max-distance 0.4   # gate CI on architecture drift
```

See [Architecture Health](architecture-health.md) for how to read the metrics.

## `istos eval`

Replay recorded agent trajectories against the current code — no model, no mesh,
no network:

```bash
istos eval trajectories/                      # a directory of recordings
istos eval trajectories/refund-flow.json      # or just one
istos eval trajectories/ --app main:istos     # also check the tools still match
istos eval trajectories/ --ignore-text        # compare tool calls only
```

Each recording is replayed with its model turns pinned, so a difference means
*your* code changed: a tool no longer called, arguments that moved, a tool missing
from the catalogue, a different final answer. `--app` additionally imports the app
(`module:attr`, or a module exposing `istos` / `app`) and checks every recorded
tool still exists and still accepts the arguments that were recorded.

Exit code is non-zero if any trajectory fails to reproduce. See
[Testing](testing.md#testing-agents-record-then-replay) for how to record one.

## `istos docs`

Serve the MkDocs documentation site locally (requires `mkdocs` from the `dev` extra):

```bash
pip install 'istos[dev]'
istos docs --port 8000
```

Open `http://127.0.0.1:8000`. Use `--dir /path/to/repo` if you are not in the repository root that contains `mkdocs.yml`.

## Next Steps

- [Getting Started](getting-started.md)
- [Testing](testing.md)
- [Recipe: Scaffold a service](../recipes/scaffold-service.md)
