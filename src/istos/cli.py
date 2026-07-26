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


def _load_app(spec: str) -> object:
    """Import ``module:attr`` (or ``module`` with an ``istos``/``app`` attribute)."""
    import importlib

    module_name, _, attr = spec.partition(":")
    module = importlib.import_module(module_name)
    if attr:
        return getattr(module, attr)
    for candidate in ("istos", "app"):
        found = getattr(module, candidate, None)
        if found is not None:
            return found
    raise ValueError(
        f"{module_name} has no 'istos' or 'app' attribute — pass {module_name}:name"
    )


def _cmd_eval(args: argparse.Namespace) -> None:
    import asyncio

    from istos.testing.trajectory import Trajectory, check_against_app, replay

    root = Path(args.path)
    if root.is_dir():
        files = sorted(root.glob("*.json"))
    elif root.exists():
        files = [root]
    else:
        print(f"Error: {root} does not exist", file=sys.stderr)
        sys.exit(1)
    if not files:
        print(f"Error: no recorded trajectories (*.json) in {root}", file=sys.stderr)
        sys.exit(1)

    app = _load_app(args.app) if args.app else None

    async def _run() -> int:
        failed = 0
        for path in files:
            try:
                traj = Trajectory.load(path)
            except (ValueError, OSError) as exc:
                print(f"[FAIL] {path.name}\n       ! {exc}")
                failed += 1
                continue

            problems = check_against_app(traj, app) if app is not None else []
            result = await replay(traj, compare_text=not args.ignore_text)
            problems.extend(result.diff)

            if problems:
                failed += 1
                print(f"[FAIL] {traj.name}")
                for problem in problems:
                    print(f"       - {problem}")
            else:
                print(f"[PASS] {traj.name}  ({len(traj.tool_names)} tool call(s))")
        print()
        print(f"{len(files) - failed}/{len(files)} trajectories reproduced")
        return failed

    if asyncio.run(_run()):
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

    ev_p = sub.add_parser(
        "eval", help="Replay recorded agent trajectories against the current code"
    )
    ev_p.add_argument(
        "path", nargs="?", default="trajectories",
        help="A recorded trajectory (.json) or a directory of them",
    )
    ev_p.add_argument(
        "--app", default=None,
        help="module:attr of the Istos app, to also check tools still exist and "
             "accept the recorded arguments",
    )
    ev_p.add_argument(
        "--ignore-text", action="store_true",
        help="Do not compare the final assistant message (tool calls only)",
    )
    ev_p.set_defaults(func=_cmd_eval)

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
