"""Istos command-line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _cmd_version(_: argparse.Namespace) -> None:
    from importlib.metadata import version
    print(f"istos {version('istos')}")


def _cmd_new(args: argparse.Namespace) -> None:
    target = Path(args.name)
    if target.exists():
        print(f"Error: {target} already exists", file=sys.stderr)
        sys.exit(1)

    target.mkdir(parents=True)
    (target / "main.py").write_text(
        f'''"""Istos service: {args.name}"""

from istos import Istos

istos = Istos()


@istos.handle("service/status")
async def status() -> dict:
    return {{"service": "{args.name}", "status": "ok"}}


if __name__ == "__main__":
    istos.run()
'''
    )
    (target / "test_main.py").write_text(
        f'''import pytest
from istos.testing import IstosTestClient
from main import istos


@pytest.mark.asyncio
async def test_status():
    client = IstosTestClient(istos)
    result = await client.query("service/status")
    assert result["service"] == "{args.name}"
    assert result["status"] == "ok"
'''
    )
    print(f"Created Istos project at {target}/")
    print("  main.py       — service entry point")
    print("  test_main.py  — example test with IstosTestClient")


def _cmd_analyze(args: argparse.Namespace) -> None:
    import json
    from istos.fitness import analyze, format_report

    try:
        report = analyze(Path(args.path), package=args.package)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        print(format_report(report))

    failed = []
    if args.max_distance is not None:
        over = [c.name for c in report.components if c.distance > args.max_distance]
        if over:
            failed.append(f"distance > {args.max_distance}: {', '.join(over)}")
    if args.no_cycles and report.cycles:
        failed.append(f"{len(report.cycles)} dependency cycle(s)")
    if failed:
        print("\nFitness check failed: " + "; ".join(failed), file=sys.stderr)
        sys.exit(1)


def _cmd_docs(args: argparse.Namespace) -> None:
    import subprocess
    cmd = ["mkdocs", "serve", "-a", f"127.0.0.1:{args.port}"]
    if args.dir:
        cmd.extend(["-f", str(Path(args.dir) / "mkdocs.yml")])
    subprocess.run(cmd, check=False)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="istos", description="Istos CLI")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("version", help="Print installed version").set_defaults(
        func=_cmd_version
    )

    new_p = sub.add_parser("new", help="Scaffold a new Istos service")
    new_p.add_argument("name", help="Project directory name")
    new_p.set_defaults(func=_cmd_new)

    an_p = sub.add_parser(
        "analyze", help="Measure component health (abstractness/instability/distance)"
    )
    an_p.add_argument("path", nargs="?", default=".", help="Project or package directory")
    an_p.add_argument("--package", default=None, help="Package name if the project ships several")
    an_p.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    an_p.add_argument(
        "--max-distance", type=float, default=None,
        help="Exit non-zero if any component's distance exceeds this (CI gate)",
    )
    an_p.add_argument(
        "--no-cycles", action="store_true",
        help="Exit non-zero if any dependency cycle exists (CI gate)",
    )
    an_p.set_defaults(func=_cmd_analyze)

    docs_p = sub.add_parser("docs", help="Serve documentation locally")
    docs_p.add_argument("--port", type=int, default=8000)
    docs_p.add_argument("--dir", type=str, default=None)
    docs_p.set_defaults(func=_cmd_docs)

    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
