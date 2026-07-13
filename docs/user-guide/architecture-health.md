# Architecture Health

`istos analyze` measures the structural health of a codebase using the
Abstractness / Instability / Distance metrics from Robert Martin, popularised
for distributed systems in *Software Architecture: The Hard Parts*. It works on
any Python package, not just Istos services, and is designed to run in CI as an
architecture fitness function.

```bash
istos analyze                    # analyse the package in the current project
istos analyze path/to/project    # or point it somewhere
istos analyze --package mypkg    # disambiguate a multi-package repo
istos analyze --json             # machine-readable output
```

## What it measures

A **component** is a top-level subpackage or module of the package under
analysis (for example `myapp.orders` or `myapp.routing`) — never an individual
class or line. For each component:

| Metric | Meaning | Formula |
| --- | --- | --- |
| `Ce` | efferent coupling — other components it imports | |
| `Ca` | afferent coupling — other components that import it | |
| `I` | instability — how much it depends outward | `Ce / (Ce + Ca)` |
| `A` | abstractness — how much of it is "shape" vs code | `m_a / (m_a + m_c)` |
| `D` | distance from the main sequence | `\|A + I − 1\|` |

The **main sequence** is the line `A + I = 1`: abstract components should be
stable (many depend on them, they depend on little); concrete components should
be unstable (free to change). `D = 0` means a component sits on that line.

Type-only imports guarded by `if TYPE_CHECKING:` are excluded — they are soft by
design and never create a runtime cycle or block independent deployment.

### Abstractness in Python

Java-style interfaces are rare in Python, so a type is counted as abstract when
it is a `typing.Protocol`, an `abc.ABC`, carries an `@abstractmethod`, is a
`TypedDict`, or is a module-level `Callable[...]` alias — the last being Python's
one-method interface (the pluggable contract behind a hook like `Authorizer`).
Because idiomatic Python uses these only at genuine boundaries, concrete
components legitimately read a low `A`; that is expected, not a defect.

## Reading the result

The `zone` column names where a component has landed:

- **balanced** — within tolerance of the main sequence. Wiring/composition
  packages (`app`, `routing`, `primitives`) belong here: high instability, low
  abstractness.
- **leaf** — a small, stable, concrete utility (exceptions, contextvars,
  dataclasses). It reads far from the sequence only because `A = 0`; it is
  healthy and needs no action.
- **pain** — a *large, widely-imported* concrete blob: the real smell. Extract
  seams (`Protocol`s), or invert the dependencies so fewer components import it.
- **uselessness** — abstract but unstable: interfaces nobody implements. Delete
  them; do not add `Protocol`s without callers just to raise `A`.

Distance is diagnostic, not a score to optimise to zero. `pain` is reserved for
components that are concrete **and** over 300 lines **and** imported by four or
more others — the "huge concrete blob everyone depends on" the metric was built
to catch.

The report also flags **dependency cycles** (they defeat independent
deployment — break them) and **god-module candidates** (single modules over
800 lines).

## Gating CI

Both checks can fail the build so architecture drift is caught in review:

```bash
istos analyze --no-cycles            # non-zero exit if any import cycle exists
istos analyze --max-distance 0.4     # non-zero exit if any component drifts too far
```

Because Python's low abstractness pushes stable leaves past most distance
thresholds, prefer `--no-cycles` as a hard gate and treat `--max-distance` as a
guard on the components you expect to stay balanced.

## Istos's own report

Running `istos analyze` on the framework itself (its result is part of Istos's
own governance):

```
component        mods   loc  Ce  Ca      I      A      D  zone
context             1    92   0   7   0.00   0.00   1.00  leaf
fitness             1   341   0   1   0.00   0.00   1.00  leaf
logging             1   147   0   5   0.00   0.00   1.00  leaf
validation          1   103   0   4   0.00   0.00   1.00  leaf
messages            2   127   0   4   0.00   0.12   0.88  leaf
consistency         6   594   0   2   0.00   0.14   0.86  leaf
errors              1   115   1   6   0.14   0.10   0.76  leaf
di                  3   258   1   3   0.25   0.00   0.75  leaf
retry               1    53   1   3   0.25   0.00   0.75  leaf
discovery           2   254   1   2   0.33   0.00   0.67  leaf
security            2   258   2   5   0.29   0.17   0.55  leaf
http                5   282   1   3   0.25   0.25   0.50  balanced
observability       3   133   2   1   0.67   0.00   0.33  balanced
communication       5   893   2   2   0.50   0.18   0.32  balanced
middleware          3   165   2   3   0.40   0.29   0.31  balanced
routing             1   144   3   1   0.75   0.00   0.25  balanced
primitives         11  1492  12   2   0.86   0.00   0.14  balanced
queue               5   884   6   1   0.86   0.00   0.14  balanced
app                 7  2009  15   1   0.94   0.00   0.06  balanced
cli                 1   103   1   0   1.00   0.00   0.00  balanced
testing             2   194   6   0   1.00   0.00   0.00  balanced
```

How to read it:

- **No `pain`, no `uselessness`, no dependency cycles, no god-modules.** Nothing
  is a large concrete blob that everything imports, and no abstraction lacks
  callers.
- **The instability gradient is correct.** The stable kernel (`context`,
  `errors`, `messages`, `consistency`) has `I → 0` — many depend on it, it
  depends on almost nothing. The composition root (`app`, `I = 0.94`,
  `D = 0.06`) sits right on the main sequence: it wires everything together and
  nothing imports it. That is the Stable Dependencies Principle holding.
- **The `leaf` rows are intentional.** They read far from the sequence purely
  because concrete Python utilities have `A = 0`; they are small and stable, so
  the distance is a Python artifact, not debt. Abstract seams live exactly where
  extensibility matters — `StoragePlugin`, `ObjectStore`, `Serialize`,
  `Middleware`, and the `Authorizer` callable contract.

## Programmatic use

```python
from istos.fitness import analyze

report = analyze(".")
print(report.mean_distance)
for c in report.components:
    if c.zone == "uselessness":
        print(f"{c.name}: abstract with no callers")
```
