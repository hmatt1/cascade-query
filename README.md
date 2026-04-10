# Cascade Query

<p align="center">
  <a href="https://pypi.org/project/query-cascade/"><img alt="PyPI version" src="https://img.shields.io/pypi/v/query-cascade?color=3775A9&label=PyPI"></a>
  <a href="https://pypi.org/project/query-cascade/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/query-cascade?color=5A9"></a>
  <a href="https://pypi.org/project/query-cascade/"><img alt="Distribution format" src="https://img.shields.io/pypi/wheel/query-cascade?color=4CAF50"></a>
</p>

**PyPI:** [query-cascade](https://pypi.org/project/query-cascade/)

**Cascade Query** is a small Python library for incremental, on-demand computation. You write normal functions; the engine figures out what depends on what, caches results, and recomputes only what changed.

It fits the same mental space as a build system or an IDE’s analysis pipeline: lots of derived values, inputs that change in small steps, and a graph of steps you do not want to rerun from scratch every time.

---

## What you get

| Capability | What it means in practice |
|------------|---------------------------|
| **Lazy evaluation** | Nothing runs until something asks for a result. |
| **Dependency tracking** | The engine records which queries read which inputs and other queries. |
| **Targeted invalidation** | After a change, only downstream work that is still needed gets redone. |
| **Deduplication** | Identical in-flight requests share one computation. |
| **Snapshots** | Readers can pin a consistent view while inputs keep moving. |
| **Background work** | Submit work in the background; stale work can be cancelled safely. |
| **Replayable side effects** | Effects recorded through **accumulators** replay on cache hits so diagnostics stay consistent. |
| **Save / load** | Persist graph and cache state (SQLite-backed API below). |
| **Inspection** | Inspect the graph and trace events for debugging. |

---

## Quickstart

```python
from cascade import Engine

engine = Engine()

@engine.input
def text() -> str:
    return ""

@engine.query
def lint_count() -> int:
    return text().count("TODO")

text.set("TODO: one\nTODO: two")
assert lint_count() == 2

# Same value → no need to recompute downstream work.
text.set("TODO: one\nTODO: two")
assert lint_count() == 2
```

---

## Install (free-threaded Python)

The library targets **free-threaded CPython 3.14** so CPU-bound parallel work can scale without fighting the GIL. The package itself is **pure Python**; what matters is the interpreter you run with.

### Windows

1. Install **free-threaded** Python 3.14 from [python.org/downloads/windows](https://www.python.org/downloads/windows/)  
   - Look for the build that includes **free-threaded** (`python3.14t` / `py -3.14t`).  
   - In the full installer, enable the free-threaded binaries option if needed.

2. Install from PyPI:

```powershell
py -3.14t -m pip install -U query-cascade
```

3. Confirm import and GIL-off mode (example):

```powershell
py -3.14t -X gil=0 -c "import cascade, sys, sysconfig; print('cascade import ok from', cascade.__file__); print('Py_GIL_DISABLED=', sysconfig.get_config_var('Py_GIL_DISABLED')); print('GIL enabled?', sys._is_gil_enabled())"
```

You want to see `Py_GIL_DISABLED= 1` and `GIL enabled? False` when exercising the free-threaded + no-GIL path.

### Editable install (from this repo)

```bash
python3.14t -m pip install -e ".[dev]"
```

---

## Minimal API example

```python
from cascade import Engine

engine = Engine()
warnings = engine.accumulator("warnings")

@engine.input
def source(file_id: str) -> str:
    return ""

@engine.query
def parse(file_id: str) -> tuple[str, ...]:
    return tuple(line.strip() for line in source(file_id).splitlines() if line.strip())

@engine.query
def symbols(file_id: str) -> tuple[str, ...]:
    return tuple(row.split("=")[0].strip() for row in parse(file_id))
```

### API surface (cheat sheet)

- **`engine.input(fn)`** — Mutable roots; call **`.set(...)`** to publish new revisions.
- **`engine.query(fn)`** — On-demand queries with memoization; dependencies are captured automatically.
- **`engine.accumulator(name)`** — Thread-safe channel for side effects that the engine **replays on cache hits**.
- **`engine.snapshot()`** — Immutable read view (`Snapshot`) for snapshot-style isolation.
- **`engine.submit(query, *args, snapshot=...)`** — Background execution; work can be **cancelled** if inputs move on (`QueryCancelled`).
- **`engine.compute_many([(query, args), ...], workers=N)`** — Parallel run with a work-stealing scheduler.
- **`engine.inspect_graph()` / `engine.traces()`** — Graph and trace introspection.
- **`engine.save(path)` / `engine.load(path)`** — Persist or restore state (SQLite).
- **`engine.prune(roots)`** — Drop memoized nodes not reachable from the given roots.

---

## Guarantees (the short version)

- **Incremental updates:** Stale paths recompute; work that is still valid is reused.
- **Selective invalidation:** When a child signals “unchanged,” parents can stay valid without redoing everything above (the engine’s red/green style early bailout).
- **One compute, many waiters:** Concurrent identical requests share a single in-flight run.
- **Cycles:** Recursive query cycles raise **`CycleError`** (there is no fixed-point solver in core).
- **Stale background work:** Obsolete background queries raise **`QueryCancelled`**.

---

## Limitations (read before you bet the farm)

- **Interpreter:** Best results for parallel CPU work come from **free-threaded 3.14** with the GIL disabled at runtime. Other interpreters work for correctness, but threaded speedups may disappoint. Building or publishing the package from a non-free-threaded Python does not block users on free-threaded Python—there is no native extension ABI tied to GIL mode.
- **Persistence:** `save` / `load` are **point-in-time snapshots**, not a multi-process transactional store with WAL semantics.
- **Trust `load` like code:** Snapshots use versioned JSON; types are resolved via **`importlib`**. Only load files from sources you trust (similar caution to pickle or “data that names types”).
- **Side effects:** Only effects sent through **`Accumulator`** are replayed. Prints, network I/O, or writes inside query bodies are **not** magically replayed.
- **Cycles:** Dynamic cycles are detected and rejected; cyclic dataflow is **not** solved to a fixed point inside this library.

### Deliberately out of scope (for a small core)

- Nominal interning (`@interned`), tracked structs (`@tracked`)
- Fixed-point solvers for cyclic graphs
- Distributed or shared cache protocols

You can add these on top of the same query model if you need them.

---

## Is this a good fit? (checklist)

Strong fits look like **graphs of mostly pure steps** over **inputs that change a little at a time**, with **expensive recomputation** if you always recompute everything.

Answer Yes/No:

1. Can you express the logic as **inputs → derived queries** (`A → B → C`)?
2. Are derived values **mostly pure** functions of tracked inputs (side effects routed through accumulators)?
3. Do you **ask the same queries repeatedly**?
4. Do inputs usually change in **small increments** rather than full replacement every time?
5. Is a **full recompute** noticeably expensive?
6. Do you need **precise invalidation** (only affected downstream nodes)?
7. Do concurrent callers often request the **same keys**?
8. Do some readers need a **stable snapshot** while writes continue?
9. Can **background** work become **waste** when new writes land?
10. Would **graph/tracing** help you debug cache hits and invalidation?

**Rough read:** 9–10 Yes → excellent · 7–8 → strong · 5–6 → try a spike · 0–4 → probably not this abstraction.

### Problems that usually score high

- Incremental IDE analysis (parse, index, diagnostics, code actions)
- Monorepo “what tests ran” / impact planners
- Incremental static analysis or security scanning
- Compilers / DSL pipelines with live diagnostics and warning replay
- Policy-as-code over IaC (re-eval only what changed)
- Feature flags / entitlements from layered config
- Schema evolution impact
- Derived metrics / analytics compilers
- Build or asset graphs
- Rules engines with heavy duplicate concurrent requests

---

## Examples (in `examples/`)

| Script | What it shows |
|--------|----------------|
| `compiler_pipeline.py` | `source → parse → symbols → typecheck`, warnings accumulator, cache-hit narration |
| `dynamic_macro_expansion.py` | Query that **changes downstream dependencies** at runtime |
| `snapshot_isolation.py` | Snapshot reads while live inputs change |
| `concurrent_background_work.py` | Dedup under concurrency + cancellation after input changes |
| `persistence_and_inspection.py` | Save/load and graph summaries |
| `gil_parallel_speedup.py` | Threaded CPU benchmark: GIL vs free-threaded |

Run one:

```bash
python3.14t examples/compiler_pipeline.py
```

Run all (Unix-style shell):

```bash
for example in examples/*.py; do
  echo "Running $example"
  python3.14t "$example"
done
```

Examples print narration as they run so you can follow each behavior.

### Compare GIL vs free-threaded (same machine)

Install both **3.14** and **3.14t** if you want apples-to-apples. On Ubuntu (deadsnakes):

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.14 python3.14-venv python3.14t python3.14t-venv
python3.14 -m pip install -e .
python3.14t -m pip install -e .
```

Quick check on free-threaded build:

```bash
python3.14t -c "import sys, sysconfig; print('Py_GIL_DISABLED=', sysconfig.get_config_var('Py_GIL_DISABLED')); print('GIL enabled?', sys._is_gil_enabled())"
```

Same interpreter, toggle GIL at runtime:

```bash
PYTHON_GIL=1 python3.14t examples/gil_parallel_speedup.py --workers 8 --tasks 96 --rounds 300000 --repeats 5
PYTHON_GIL=0 python3.14t examples/gil_parallel_speedup.py --workers 8 --tasks 96 --rounds 300000 --repeats 5
```

Or compare `python3.14` vs `PYTHON_GIL=0 python3.14t` on the same script.

Compare **`median parallel seconds`** (lower is better) and **`threaded speedup in this runtime`** (higher is better). Keep args identical, reduce background load, and use `--repeats` (e.g. `5`) to smooth noise. On multi-core machines, **free-threaded + GIL off** usually wins clearly for this CPU-bound demo.

---

## Persistence and inspection

```python
engine.save("state.db")
engine.load("state.db")
print(engine.inspect_graph())
for event in engine.traces():
    print(event.event, event.key, event.detail)
```

---

## Design stance

The core is intentionally minimal: **pull-based** evaluation, dependency capture, red/green style bailout, dedup, snapshots, cancellation, accumulator replay, tracing, and persistence. That set is enough for many real pipelines without baking in advanced internals (e.g. fixed-point cycle solving or custom AST red/green structures). CPU-bound parallelism is expected to matter when you use **free-threaded CPython with the GIL disabled**.

---

## Development

### Tests (match main CI)

```bash
export PYTHON_GIL=0   # Windows: set PYTHON_GIL=0
python3.14t -m pip install -e ".[dev]"
python3.14t -c "import sys, sysconfig; print('Py_GIL_DISABLED=', sysconfig.get_config_var('Py_GIL_DISABLED')); print('GIL enabled?', sys._is_gil_enabled())"
python3.14t -m pytest -q \
  --ignore=tests/test_performance.py \
  --cov=src/cascade \
  --cov-branch \
  --cov-report=term-missing \
  --cov-fail-under=95
```

Branch coverage check (CI uses an equivalent step on `coverage.json`):

```bash
python3.14t - <<'PY'
import json
with open("coverage.json", encoding="utf-8") as fh:
    b = json.load(fh)["totals"]["percent_branches_covered"]
print(f"branch coverage: {b:.2f}%")
assert b >= 90.0
PY
```

Stateful fuzz:

```bash
PYTHON_GIL=0 python3.14t -m pytest -q tests/test_stateful_engine_invariants.py
```

Mutation testing:

```bash
PATH="$HOME/.local/bin:$PATH" PYTHON_GIL=0 mutmut run
PATH="$HOME/.local/bin:$PATH" PYTHON_GIL=0 mutmut results
```

Use the `mutmut` CLI (`mutmut run`), not `python -m mutmut run`. Bounded local loop:

```bash
PYTHON_GIL=0 MUTMUT_MAX_CHILDREN=2 ./scripts/mutation_fast.sh
```

Focused mutants:

```bash
PYTHON_GIL=0 MUTMUT_MAX_CHILDREN=2 ./scripts/mutation_fast.sh "<mutant-name>" "<mutant-name>"
```

See `docs/mutation_triage.md` for survivor triage.

### Formal model (TLA+)

Specs live under `docs/formal/`:

- `docs/formal/cascade_core.tla`
- `docs/formal/cascade_core.cfg`

Run TLC (example):

```bash
java -cp tla2tools.jar tlc2.TLC docs/formal/cascade_core.tla -config docs/formal/cascade_core.cfg
```

Checked properties include snapshot consistency, active-dependency validity (red/green alignment), and cancellation epoch monotonicity.

### Performance suite

Heavy behavior clusters around cache hits vs full recompute, concurrent dedup, `compute_many` throughput on free-threaded workloads, large-graph mutation vs rebuild, mark-green cost vs depth, and prune scaling.

```bash
python -m benchmarks.performance_suite --report-dir artifacts/performance --assert-thresholds
```

Outputs:

- `artifacts/performance/performance-report.json`
- `artifacts/performance/performance-report.md`

CI runs the same suite and uploads **`performance-report`**.

The **`compute-many-parallel-speedup`** scenario (and `tests/test_performance.py::test_compute_many_parallel_speedup_scenario`) is sensitive to CPU scheduling. On a busy laptop or small VM, thresholds may flap without a real regression. Mitigations:

- Re-run the test, or set **`CASCADE_QUERY_PARALLEL_PERF_RETRIES`** (e.g. `3`).
- To skip while iterating: **`CASCADE_QUERY_SKIP_PARALLEL_PERF=1`** (CI does not set this).

**Nightly:** `.github/workflows/nightly-performance.yml` runs a longer sweep (e.g. 8 runs) and publishes **`nightly-performance-report`**.

### Scale and stress tests

`tests/test_scale_behavior.py` covers large-graph invalidation, dynamic dependency churn, prune stress, persistence at scale, eviction under churn, and mixed concurrency (`submit` + `compute_many` + writes). The heaviest cases are marked **`@pytest.mark.slow`**; default `pytest` skips them via `pyproject.toml`.

Internal invariants are concentrated in `tests/test_internal_invariants.py` (via `engine._internals`) to limit coupling while keeping safety checks.

Default CI-like run (no perf file, no slow):

```bash
PYTHON_GIL=0 python3.14t -m pytest -q --ignore=tests/test_performance.py
```

Slow only:

```bash
pytest -q -m slow
```

Everything including slow:

```bash
pytest -q -m "slow or not slow"
```

### CI overview

- Workflow: `.github/workflows/ci.yml` (pushes and PRs).
- **Ruff** before tests.
- Separate **package build** (`python -m build`) to catch packaging issues early.
