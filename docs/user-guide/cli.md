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
