"""Architecture fitness functions for measuring component health.

Implements Robert Martin's Abstractness / Instability / Distance metrics as
described in "Software Architecture: The Hard Parts" (ch. 4), adapted for
Python: a type counts as abstract when it is a ``typing.Protocol``, an
``abc.ABC``, or carries an ``@abstractmethod`` -- the real seams of a Python
codebase, rather than Java-style interfaces.

A *component* is a top-level subpackage or module of the package under
analysis (e.g. ``istos.consistency``, ``istos.routing``), never an individual
line or class. For each component we measure:

    Ce  efferent coupling   other components it imports from
    Ca  afferent coupling   other components that import it
    I   instability         Ce / (Ce + Ca)          0 = stable .. 1 = unstable
    A   abstractness        abstract types / all types
    D   distance            |A + I - 1|             0 = on the main sequence

Distance is diagnostic, not a score to drive to zero. Concrete leaf utilities
in Python legitimately sit at A=0, I=0 (D=1); that only signals pain when the
component is also large and volatile. Read the numbers, don't optimise them.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# A component is "balanced" when it sits within this distance of the main
# sequence; beyond it we name the zone it has drifted into.
MAIN_SEQUENCE_TOLERANCE = 0.5
# The "pain" zone is the book's *huge concrete blob everyone depends on*, not any
# small stable leaf. A concrete, off-sequence component is only flagged as pain
# when it is both large and widely imported; otherwise it is a benign leaf.
PAIN_LOC = 300
PAIN_CA = 4
# Single modules larger than this are flagged as god-module candidates.
GOD_MODULE_LOC = 800

# A type counts as abstract when it is one of these bases, carries an
# @abstractmethod, or is a module-level Callable alias (Python's duck-typed seam).
_ABSTRACT_BASES = {"Protocol", "ABC", "ABCMeta", "TypedDict"}


@dataclass
class Component:
    """Health metrics for one package-level component."""

    name: str
    efferent: int          # Ce
    afferent: int          # Ca
    abstract_types: int    # m_a
    concrete_types: int    # m_c
    loc: int
    modules: int

    @property
    def instability(self) -> float:
        total = self.efferent + self.afferent
        return self.efferent / total if total else 0.0

    @property
    def abstractness(self) -> float:
        total = self.abstract_types + self.concrete_types
        return self.abstract_types / total if total else 0.0

    @property
    def distance(self) -> float:
        return abs(self.abstractness + self.instability - 1.0)

    @property
    def zone(self) -> str:
        if self.distance <= MAIN_SEQUENCE_TOLERANCE:
            return "balanced"
        if self.abstractness + self.instability > 1:
            return "uselessness"
        if self.loc >= PAIN_LOC and self.afferent >= PAIN_CA:
            return "pain"
        return "leaf"

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "modules": self.modules,
            "loc": self.loc,
            "ce": self.efferent,
            "ca": self.afferent,
            "instability": round(self.instability, 3),
            "abstractness": round(self.abstractness, 3),
            "distance": round(self.distance, 3),
            "zone": self.zone,
        }


@dataclass
class Report:
    """The full result of analysing a package."""

    package: str
    components: List[Component]
    cycles: List[List[str]]
    god_modules: List[Tuple[str, int]]   # (dotted module, loc)

    @property
    def mean_distance(self) -> float:
        return sum(c.distance for c in self.components) / len(self.components) if self.components else 0.0

    def as_dict(self) -> dict:
        return {
            "package": self.package,
            "mean_distance": round(self.mean_distance, 3),
            "components": [c.as_dict() for c in self.components],
            "cycles": self.cycles,
            "god_modules": [{"module": m, "loc": n} for m, n in self.god_modules],
        }


def find_package(path: Path, package: Optional[str] = None) -> Tuple[str, Path]:
    """Locate the importable package to analyse under ``path``.

    Looks for ``src/<pkg>/__init__.py`` first (the src layout), then a
    top-level ``<pkg>/__init__.py``. Pass ``package`` to disambiguate when a
    project ships more than one.
    """
    path = path.resolve()
    if path.is_file():
        raise ValueError(f"{path} is a file; pass a project or package directory")

    if (path / "__init__.py").exists():
        return path.name, path

    roots = [path / "src", path]
    candidates: List[Path] = []
    for root in roots:
        if root.is_dir():
            candidates += [p.parent for p in root.glob("*/__init__.py")]
        if candidates:
            break

    if package:
        for c in candidates:
            if c.name == package:
                return package, c
        raise ValueError(f"package {package!r} not found under {path}")
    if not candidates:
        raise ValueError(f"no importable package found under {path}")
    if len(candidates) > 1:
        names = ", ".join(sorted(c.name for c in candidates))
        raise ValueError(f"multiple packages found ({names}); pass --package to choose")
    return candidates[0].name, candidates[0]


def _component_of_file(rel: Path) -> str:
    parts = rel.parts
    return parts[0] if len(parts) > 1 else rel.stem


def _is_abstract(node: ast.ClassDef) -> bool:
    bases: Set[str] = set()
    for b in node.bases:
        if isinstance(b, ast.Name):
            bases.add(b.id)
        elif isinstance(b, ast.Attribute):
            bases.add(b.attr)
    if bases & _ABSTRACT_BASES:
        return True
    for m in node.body:
        if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for d in m.decorator_list:
                name = d.id if isinstance(d, ast.Name) else d.attr if isinstance(d, ast.Attribute) else ""
                if name == "abstractmethod":
                    return True
    return False


def _callable_aliases(tree: ast.Module) -> int:
    """Count module-level ``Name = Callable[...]`` aliases.

    A callable alias is Python's one-method interface -- the pluggable contract
    behind hooks like ``Authorizer``. It is an abstract seam even though it is
    not a class, so it counts toward abstractness.
    """
    count = 0
    for node in tree.body:
        value = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            value = node.value
        if isinstance(value, ast.Subscript):
            base = value.value
            name = base.id if isinstance(base, ast.Name) else base.attr if isinstance(base, ast.Attribute) else ""
            if name == "Callable":
                count += 1
    return count


def _imported_modules(node: ast.AST, rel: Path, pkg: str) -> List[str]:
    """Dotted modules within ``pkg`` referenced by an import statement."""
    out: List[str] = []
    if isinstance(node, ast.Import):
        for a in node.names:
            if a.name == pkg or a.name.startswith(pkg + "."):
                out.append(a.name)
    elif isinstance(node, ast.ImportFrom):
        if node.level == 0:
            if node.module and (node.module == pkg or node.module.startswith(pkg + ".")):
                out.append(node.module)
        else:
            # Relative import: resolve against this file's own package path.
            pkg_parts = [pkg, *rel.parent.parts]
            up = node.level - 1
            if up <= len(pkg_parts):
                anchor = pkg_parts[: len(pkg_parts) - up]
                dotted = ".".join(anchor + ([node.module] if node.module else []))
                if dotted == pkg or dotted.startswith(pkg + "."):
                    out.append(dotted)
    return out


def _component_of_module(dotted: str, pkg: str) -> Optional[str]:
    tail = dotted[len(pkg) + 1:]
    if not tail:
        return None
    return tail.split(".")[0]


def _type_checking_lines(tree: ast.AST) -> Set[int]:
    """Line numbers inside ``if TYPE_CHECKING:`` blocks.

    Type-only imports are excluded from coupling: they are deliberately soft
    and never create a runtime import cycle or block independent deployment.
    """
    lines: Set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            test = node.test
            is_tc = (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
                isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
            )
            if is_tc:
                for stmt in node.body:
                    for sub in ast.walk(stmt):
                        if hasattr(sub, "lineno"):
                            lines.add(sub.lineno)
    return lines


def analyze(path: Path, package: Optional[str] = None) -> Report:
    """Analyse a package and return per-component health metrics."""
    pkg_name, pkg_dir = find_package(path, package)

    abstract: Dict[str, int] = {}
    concrete: Dict[str, int] = {}
    loc: Dict[str, int] = {}
    modules: Dict[str, int] = {}
    edges: Dict[str, Set[str]] = {}
    god_modules: List[Tuple[str, int]] = []

    for f in sorted(pkg_dir.rglob("*.py")):
        rel = f.relative_to(pkg_dir)
        # Skip the package facade: it re-exports rather than being a component.
        if rel == Path("__init__.py"):
            continue
        comp = _component_of_file(rel)
        try:
            tree = ast.parse(f.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue

        file_loc = sum(1 for line in f.read_text(encoding="utf-8").splitlines() if line.strip())
        loc[comp] = loc.get(comp, 0) + file_loc
        modules[comp] = modules.get(comp, 0) + 1
        abstract.setdefault(comp, 0)
        concrete.setdefault(comp, 0)
        edges.setdefault(comp, set())
        if file_loc > GOD_MODULE_LOC:
            dotted = pkg_name + "." + ".".join(rel.with_suffix("").parts)
            god_modules.append((dotted, file_loc))

        abstract[comp] += _callable_aliases(tree)
        tc_lines = _type_checking_lines(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                if _is_abstract(node):
                    abstract[comp] += 1
                else:
                    concrete[comp] += 1
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                if node.lineno in tc_lines:
                    continue
                for dotted in _imported_modules(node, rel, pkg_name):
                    target = _component_of_module(dotted, pkg_name)
                    if target and target != comp:
                        edges[comp].add(target)

    afferent: Dict[str, Set[str]] = {c: set() for c in edges}
    for src, targets in edges.items():
        for t in targets:
            afferent.setdefault(t, set()).add(src)

    components = [
        Component(
            name=c,
            efferent=len(edges.get(c, set())),
            afferent=len(afferent.get(c, set())),
            abstract_types=abstract.get(c, 0),
            concrete_types=concrete.get(c, 0),
            loc=loc.get(c, 0),
            modules=modules.get(c, 0),
        )
        for c in sorted(set(loc) | set(edges) | set(afferent))
    ]
    # Drop empty placeholder packages (no code, no coupling) -- they carry no signal.
    components = [
        c for c in components
        if c.loc or c.abstract_types or c.concrete_types or c.efferent or c.afferent
    ]
    components.sort(key=lambda c: c.distance, reverse=True)

    return Report(
        package=pkg_name,
        components=components,
        cycles=_find_cycles(edges),
        god_modules=sorted(god_modules, key=lambda x: x[1], reverse=True),
    )


def _find_cycles(edges: Dict[str, Set[str]]) -> List[List[str]]:
    """Strongly connected components of size > 1 in the component graph."""
    index: Dict[str, int] = {}
    low: Dict[str, int] = {}
    on_stack: Set[str] = set()
    stack: List[str] = []
    counter = [0]
    cycles: List[List[str]] = []

    def strongconnect(v: str) -> None:
        index[v] = low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        on_stack.add(v)
        for w in edges.get(v, set()):
            if w not in index:
                strongconnect(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            scc = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                scc.append(w)
                if w == v:
                    break
            if len(scc) > 1:
                cycles.append(sorted(scc))

    for v in list(edges):
        if v not in index:
            strongconnect(v)
    return cycles


def format_report(report: Report) -> str:
    """Render a report as a human-readable table."""
    lines = [f"Architecture health for '{report.package}'  (main sequence: A + I = 1)", ""]
    header = f"{'component':<16}{'mods':>5}{'loc':>6}{'Ce':>4}{'Ca':>4}{'I':>7}{'A':>7}{'D':>7}  zone"
    lines.append(header)
    lines.append("-" * len(header))
    for c in report.components:
        lines.append(
            f"{c.name:<16}{c.modules:>5}{c.loc:>6}{c.efferent:>4}{c.afferent:>4}"
            f"{c.instability:>7.2f}{c.abstractness:>7.2f}{c.distance:>7.2f}  {c.zone}"
        )
    lines.append("-" * len(header))
    lines.append(f"mean D = {report.mean_distance:.3f} over {len(report.components)} components")

    if report.cycles:
        lines.append("")
        lines.append("Dependency cycles (break these -- they defeat modular deployment):")
        for scc in report.cycles:
            lines.append("  " + " -> ".join(scc) + " -> " + scc[0])
    if report.god_modules:
        lines.append("")
        lines.append(f"God-module candidates (> {GOD_MODULE_LOC} loc):")
        for mod, n in report.god_modules:
            lines.append(f"  {mod}  ({n} loc)")

    lines.append("")
    lines.append(
        "Zones: 'balanced' near the main sequence; 'leaf' a small stable concrete "
        "utility (fine); 'pain' a large, widely-imported concrete blob (extract "
        "seams); 'uselessness' an abstraction with no callers (delete it)."
    )
    return "\n".join(lines)
