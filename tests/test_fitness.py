"""Tests for the architecture fitness-function analyzer."""
from pathlib import Path

import pytest

from istos.fitness import Component, analyze, find_package, format_report


def _write(root: Path, files: dict) -> None:
    for rel, body in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)


def test_component_metrics_math():
    c = Component(name="x", efferent=3, afferent=1, abstract_types=1, concrete_types=3, loc=40, modules=2)
    assert c.instability == 0.75
    assert c.abstractness == 0.25
    assert c.distance == pytest.approx(0.0)
    assert c.zone == "balanced"


def test_component_zones():
    # Large, widely-imported concrete blob -> real pain.
    blob = Component("blob", efferent=0, afferent=6, abstract_types=0, concrete_types=8, loc=900, modules=4)
    assert blob.zone == "pain"

    # Same shape but small and not widely used -> a benign leaf, not pain.
    leaf = Component("leaf", efferent=0, afferent=5, abstract_types=0, concrete_types=4, loc=40, modules=1)
    assert leaf.zone == "leaf"

    # Abstract but nobody depends on it -> uselessness.
    lonely_abstract = Component("iface", efferent=4, afferent=0, abstract_types=3, concrete_types=0, loc=10, modules=1)
    assert lonely_abstract.instability == 1.0 and lonely_abstract.abstractness == 1.0
    assert lonely_abstract.zone == "uselessness"


def test_find_package_flat_and_src_layout(tmp_path):
    _write(tmp_path / "flat", {"pkg/__init__.py": "", "pkg/a.py": ""})
    assert find_package(tmp_path / "flat") == ("pkg", tmp_path / "flat" / "pkg")

    _write(tmp_path / "srcl", {"src/pkg/__init__.py": "", "src/pkg/a.py": ""})
    name, path = find_package(tmp_path / "srcl")
    assert name == "pkg" and path == tmp_path / "srcl" / "src" / "pkg"

    # Pointing straight at the package directory also works.
    assert find_package(tmp_path / "flat" / "pkg") == ("pkg", tmp_path / "flat" / "pkg")


def test_find_package_ambiguous(tmp_path):
    _write(tmp_path, {"one/__init__.py": "", "two/__init__.py": ""})
    with pytest.raises(ValueError, match="multiple packages"):
        find_package(tmp_path)


def test_abstractness_counts_protocol_abc_and_abstractmethod(tmp_path):
    _write(tmp_path, {
        "pkg/__init__.py": "",
        "pkg/core.py": (
            "from typing import Protocol\n"
            "from abc import ABC, abstractmethod\n"
            "class Store(Protocol):\n    def get(self): ...\n"
            "class Base(ABC):\n    @abstractmethod\n    def run(self): ...\n"
            "class Impl:\n    def go(self): return 1\n"
        ),
    })
    report = analyze(tmp_path)
    core = next(c for c in report.components if c.name == "core")
    assert core.abstract_types == 2 and core.concrete_types == 1
    assert core.abstractness == pytest.approx(2 / 3)


def test_callable_alias_and_typeddict_count_as_abstract(tmp_path):
    _write(tmp_path, {
        "pkg/__init__.py": "",
        "pkg/hooks.py": (
            "from typing import Callable, TypedDict\n"
            "Authorizer = Callable[[str], bool]\n"
            "class Meta(TypedDict):\n    id: int\n"
            "class Impl:\n    def run(self): return 1\n"
        ),
    })
    report = analyze(tmp_path)
    hooks = next(c for c in report.components if c.name == "hooks")
    # Callable alias + TypedDict are abstract seams; Impl is concrete.
    assert hooks.abstract_types == 2 and hooks.concrete_types == 1
    assert hooks.abstractness == pytest.approx(2 / 3)


def test_coupling_and_instability(tmp_path):
    _write(tmp_path, {
        "pkg/__init__.py": "",
        "pkg/core.py": "class Thing:\n    pass\n",
        "pkg/service.py": "from pkg.core import Thing\n\nx = Thing()\n",
    })
    report = analyze(tmp_path)
    core = next(c for c in report.components if c.name == "core")
    service = next(c for c in report.components if c.name == "service")
    assert core.afferent == 1 and core.efferent == 0 and core.instability == 0.0
    assert service.efferent == 1 and service.afferent == 0 and service.instability == 1.0


def test_type_checking_imports_are_excluded(tmp_path):
    _write(tmp_path, {
        "pkg/__init__.py": "",
        "pkg/core.py": "class Thing:\n    pass\n",
        "pkg/service.py": (
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n    from pkg.core import Thing\n"
        ),
    })
    report = analyze(tmp_path)
    core = next(c for c in report.components if c.name == "core")
    assert core.afferent == 0   # the type-only import must not couple


def test_relative_imports_resolve(tmp_path):
    _write(tmp_path, {
        "pkg/__init__.py": "",
        "pkg/core.py": "class Thing:\n    pass\n",
        "pkg/sub/__init__.py": "",
        "pkg/sub/worker.py": "from ..core import Thing\n",
    })
    report = analyze(tmp_path)
    core = next(c for c in report.components if c.name == "core")
    assert "sub" in {c.name for c in report.components}
    assert core.afferent == 1   # sub -> core via a relative import


def test_cycle_detection(tmp_path):
    _write(tmp_path, {
        "pkg/__init__.py": "",
        "pkg/a.py": "import pkg.b\n",
        "pkg/b.py": "import pkg.a\n",
    })
    report = analyze(tmp_path)
    assert report.cycles == [["a", "b"]]


def test_empty_placeholder_packages_dropped(tmp_path):
    _write(tmp_path, {
        "pkg/__init__.py": "",
        "pkg/real.py": "class A:\n    pass\n",
        "pkg/empty/__init__.py": "",
    })
    report = analyze(tmp_path)
    names = {c.name for c in report.components}
    assert "real" in names and "empty" not in names


def test_format_report_smoke(tmp_path):
    _write(tmp_path, {"pkg/__init__.py": "", "pkg/a.py": "class A:\n    pass\n"})
    text = format_report(analyze(tmp_path))
    assert "Architecture health for 'pkg'" in text
    assert "main sequence" in text
