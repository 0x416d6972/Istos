# Installation

## Prerequisites

Istos requires:

- Python 3.10, 3.11, 3.12, 3.13, or 3.14
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Installing from PyPI

=== "uv (Recommended)"

    ```bash
    uv pip install istos
    ```

=== "pip"

    ```bash
    pip install istos
    ```

This will install Istos with its core dependencies: `eclipse-zenoh`, `pydantic`, `pyyaml`, and `msgpack`.

## Optional Dependencies

Istos supports optional extras for additional functionality:

```bash
# Redis-backed distributed storage
uv pip install "istos[redis]"

# SQL-backed durability ledger (any SQLAlchemy DB — add your async driver)
uv pip install "istos[sqlalchemy]" asyncpg

# OpenTelemetry tracing
uv pip install "istos[otel]"

# Redis + SQLAlchemy + OpenTelemetry
uv pip install "istos[all]"
```

AsyncAPI's embedded UI uses `aiohttp`, which is a **core** dependency — no separate `web` extra.

## Installing from Source

For the latest development version:

```bash
git clone https://github.com/A111ir/Istos.git
cd Istos
uv pip install -e .
```

## Development Installation

For development and testing:

```bash
git clone https://github.com/A111ir/Istos.git
cd Istos
uv pip install -e ".[dev]"
```

This installs additional tools: `pytest`, `pytest-asyncio`, `mypy`, and `pylint`.

## Verifying Installation

```python
from istos import Istos

istos = Istos()
print("Istos is ready!")
```

!!! note "Zenoh Router"
    Istos uses Eclipse Zenoh for networking. For peer-to-peer mode (default), no external router is needed. For client mode or multi-network setups, you'll need a [Zenoh router](https://zenoh.io/docs/getting-started/installation/).
