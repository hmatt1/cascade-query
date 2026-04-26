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
| **Graph utilities** | **Subgraph** reachability on memoized nodes, optional **stats** (body time + LRU evictions), and **DOT / Mermaid** export for visualization. |

---

## Quickstart

```python
from cascade import Engine

engine = Engine()
calls = {"lines": 0, "mentions": 0}

@engine.input
def doc() -> str:
    return ""

@engine.input
def needle() -> str:
    return "TODO"

@engine.query
def lines() -> tuple[str, ...]:
    calls["lines"] += 1
    return tuple(line.rstrip() for line in doc().splitlines() if line.strip())

@engine.query
def mentions() -> int:
    calls["mentions"] += 1
    n = needle().upper()
    return sum(1 for line in lines() if n in line.upper())

@engine.query
def report() -> str:
    return f"{len(lines())} lines, {mentions()} matching {needle()}"

needle.set("TODO")
doc.set("# Notes\n\nShip the feature.\n\nTODO: add tests")
assert report() == "3 lines, 1 matching TODO"
assert calls == {"lines": 1, "mentions": 1}

# Only `needle` changed — `lines()` stays cached; `mentions()` reruns.
needle.set("SHIP")
assert report() == "3 lines, 1 matching SHIP"
assert calls == {"lines": 1, "mentions": 2}

# `doc` changed — `lines()` runs again, then downstream queries refresh.
doc.set("# Notes\n\nShip the feature.\n\nNothing to find")
assert report() == "3 lines, 1 matching SHIP"
assert calls == {"lines": 2, "mentions": 3}

# Same revision as before — nothing is recomputed.
doc.set("# Notes\n\nShip the feature.\n\nNothing to find")
assert report() == "3 lines, 1 matching SHIP"
assert calls == {"lines": 2, "mentions": 3}
```

---

## Install

The package metadata allows **CPython 3.12 and newer** (`requires-python >= 3.12`). The library is **pure Python**, so `pip` installs the same wheel everywhere. For **correctness and ordinary use**, any supported interpreter is fine.

**Parallel CPU work:** best results still come from **free-threaded CPython 3.14** with the GIL disabled at runtime (see below). On 3.12/3.13 with the standard GIL, threaded speedups for CPU-heavy graphs may be limited even though the API behaves the same.

### Standard Python (3.12+)

```bash
python -m pip install -U query-cascade
```

Use the same command with your `python`/`py` launcher for 3.12 or any newer version you rely on.

---

## Install (free-threaded Python 3.14, recommended for parallel CPU work)

The library is **optimized for** **free-threaded CPython 3.14** so CPU-bound parallel work can scale without fighting the GIL. What matters for throughput is the interpreter you run with, not how the wheel was built.

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

## API reference

Public symbols (all importable from `cascade`):

| Name | Role |
|------|------|
| **`Engine`** | Graph engine: registration, evaluation, persistence, tracing. |
| **`Accumulator`** | Named channel for side effects recorded during queries and **replayed on cache hits**. |
| **`Snapshot`** | Immutable handle pinning a **global revision** for consistent reads. |
| **`TraceEvent`** | One record from the in-memory trace log. |
| **`QueryKey`** | **Type alias** for memo/input keys: **`tuple[str, str, tuple[Any, ...]]`** (`kind`, function id, args). Use in type hints with **`prune`**, **`subgraph`**, etc. |
| **`export_dot`** / **`export_mermaid`** | Pure renderers from an **`inspect_graph()`**-shaped dict to **Graphviz DOT** or **Mermaid** `flowchart` text. |
| **`CycleError`** | Raised when a **query cycle** is detected during evaluation. |
| **`QueryCancelled`** | Raised when **background** work is invalidated by newer input revisions (subclass of **`CancellationError`**). |
| **`CancellationError`** | Base class for cancellation-style failures. |

```python
from cascade import (
    Accumulator,
    CancellationError,
    CycleError,
    Engine,
    QueryCancelled,
    QueryKey,
    Snapshot,
    TraceEvent,
    export_dot,
    export_mermaid,
)
```

### Example

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

### `Engine`

Constructor:

- **`Engine(*, max_entries: int = 10_000, trace_limit: int = 50_000, stats: bool = False, stats_eviction_recent_cap: int = 32, stats_clock: Callable[[], float] | None = None)`** — `max_entries` bounds memoized query nodes (LRU-style eviction when over capacity). `trace_limit` caps the number of **`TraceEvent`** records retained. With **`stats=True`**, the engine records **wall time** spent in successful query bodies (same monotonic clock as recompute tracing; overridable via **`stats_clock`** for tests) and LRU **eviction** counters (see **`stats_summary()`**). When **`stats=False`**, behavior matches prior releases aside from one indirect call to **`time.perf_counter`** per recompute (negligible vs. query work).

Properties and methods:

- **`revision`** *(read-only `int`)* — Monotonic counter bumped on each successful input publish; also carried on **`Snapshot`**.
- **`input(fn) → InputHandle`** — Register **`fn`** as a **mutable root**. The decorated callable’s parameters are the key (see **Input handles**); its return value is only used as the initial value before the first **`.set`**. Dependencies from queries are tracked automatically when the body calls the handle.
- **`query(fn) → QueryHandle`** — Register **`fn`** as a memoized query. The engine records which inputs and other queries run during each evaluation. Re-entrancy that forms a cycle raises **`CycleError`**.
- **`accumulator(name: str) → Accumulator`** — Return a named **`Accumulator`** (see below). Names are keys in the optional **`effects`** map passed to synchronous **`QueryHandle`** calls and **`submit`**.
- **`snapshot() → Snapshot`** — Capture **`Snapshot(revision=engine.revision)`** for use as **`snapshot=`** on reads. Reads through that snapshot see input and memo state as of that revision; live **`set`** calls do not disturb snapshot reads.
- **`submit(query, *args, *, snapshot=None, effects=None, executor=None) → concurrent.futures.Future`** — Run **`query(*args)`** on **`executor`**, or on a **lazily created** per-engine **`ThreadPoolExecutor`** when **`executor` is `None`**. The engine captures **`cancel_epoch`** at schedule time; if inputs move on before the task runs, the future may complete with **`QueryCancelled`**. **`snapshot`** defaults to a fresh **`snapshot()`** at submit time when omitted.
- **`compute_many(calls, *, workers=None, snapshot=None, effects=None) → list[Any]`** — Run many queries in parallel with a work-stealing scheduler. **`calls`** is a sequence of **`(query_handle, args_tuple)`**. Result order matches **`calls`**. **`workers`** defaults to a sensible value from **`len(calls)`** (capped); **`workers=0`** falls back to that same default. **`snapshot`** defaults to **`snapshot()`** when omitted. When **`effects`** is a dict, accumulator output is collected and appended into **`effects[name]`** deterministically in **call order** (not completion order).
- **`shutdown(*, wait: bool = True, cancel_futures: bool = False)`** — Shut down the **default** thread pool created by **`submit`**, if any. Call when discarding the engine if you need threads torn down promptly (for example in tests).
- **`traces() → list[TraceEvent]`** — Copy of recent trace events (subject to **`trace_limit`**).
- **`clear_traces()`** — Drop all buffered trace events.
- **`inspect_graph() → dict[str, Any]`** — Under the store lock, summarize memoized queries: **`revision`**, **`memo_count`**, **`input_count`**, **`nodes`** (string keys for memo entries), **`edges`** as **`(parent_key, dep_key)`** pairs for recorded dependencies (parent depends on dep). Key string format is unchanged since this helper was introduced.
- **`subgraph(roots, *, direction="deps") → dict[str, Any]`** — Same shape as **`inspect_graph()`**, filtered to memoized nodes and edges in the transitive closure of **`roots`**. Default **`direction="deps"`** walks toward dependencies (same edge direction as inspection). Use **`direction="dependents"`** for transitive dependents. Each root may be a **`QueryKey`** or a string equal to a **`nodes`** entry; unknown roots are ignored. Empty **`roots`** yields empty **`nodes`** / **`edges`**.
- **`enable_stats(enabled=True)`** / **`stats_summary() → dict[str, Any]`** / **`reset_stats()`** — Toggle aggregation, read **`by_key`** body times (seconds), LRU **`evictions_total`**, **`evictions_recent`** ring (size from **`stats_eviction_recent_cap`**), plus live **`memo_count`** / **`max_entries`**. Eviction counters count only LRU evictions, not **`prune`** or manual drops.
- **`prune(roots)`** — Remove memoized **query** nodes not reachable from **`roots`**, following dependency edges backward. Each root is a **`QueryKey`**: **`("query", query_handle.id, args_tuple)`** (the same shape the engine uses internally). Unknown roots are ignored safely.
- **`save(path: str)`** — Persist graph state (inputs, memos, dependents, trace buffer metadata, revision counters) into a **SQLite** file via a versioned JSON payload.
- **`load(path: str)`** — Restore from **`path`**. If the table is empty or missing payload data, returns without error. Clears in-flight futures. **Only load snapshots from trusted sources** (payloads name types for **`importlib`** resolution—see **Limitations**).

### Input handles

Returned by **`engine.input`**. Treat as the callable you defined:

- **`handle(*args, *, snapshot=None) → Any`** — Read the current value for that input key (optionally pinned to **`snapshot`**).
- **`handle.set(*args, value)`** — Publish a new value for key **`args`**. Variants: **`handle.set(value=x)`** when the input has no key parameters, or **`handle.set(key_arg, …, value=y)`** using the keyword. The last positional argument may also be the value when **`value`** is omitted (see tests for the exact **`set`** shapes). Returns the new **`engine.revision`** after the write.
- **`handle.id`** *(property, `str`)* — Stable string id (**`module:qualname`**) used in persistence and **`prune`** keys.

### Query handles

Returned by **`engine.query`**:

- **`handle(*args, *, snapshot=None, effects=None) → Any`** — Run or reuse the memoized query. When **`effects`** is a **`dict`**, accumulator **`push`** calls append to **`effects[accumulator_name]`** for this evaluation (including **replayed** effects on cache hits).
- **`handle.id`** *(property, `str`)* — Use in **`prune([("query", handle.id, args_tuple), …])`**.
- **`handle.raw`** *(property)* — The original decorated callable (for advanced use).

### `Accumulator`

- **`name`** *(attribute, `str`)* — Channel name used as the key in **`effects`** dicts.
- **`push(item)`** — Record **`item`** for the current query evaluation. **Must be called from inside a running query**; otherwise raises **`RuntimeError`**. On a memo **hit**, stored effects are **replayed** so diagnostics stay stable without re-executing the query body.

### `Snapshot`

Frozen dataclass:

- **`revision: int`** — Global revision pinned for reads using **`snapshot=`** on inputs and queries.

### `TraceEvent`

Frozen dataclass (see **`traces()`**):

- **`event: str`**, **`key: str`**, **`revision: int`**, **`detail: str`**, **`timestamp: float`**.

### `QueryKey`

Typing-only alias (same shape the engine uses internally): **`("query" \| "input", function_id, args_tuple)`**. Example:

```python
from cascade import Engine, QueryKey

engine = Engine()

@engine.query
def work(x: int) -> int:
    return x + 1

def roots_for(x: int) -> list[QueryKey]:
    return [("query", work.id, (x,))]
```

### Exceptions

- **`CycleError`** — Dynamic dependency graph formed a cycle; no fixed-point solver is applied.
- **`QueryCancelled`** — Scheduled or running work was superseded; typical when reading **`Future.result()`** from **`submit`** after inputs changed.
- **`CancellationError`** — Base type for **`QueryCancelled`**; you can catch it if you treat all cancellation the same.

### Persistence and inspection (quick usage)

```python
engine.save("state.db")
engine.load("state.db")
print(engine.inspect_graph())
for event in engine.traces():
    print(event.event, event.key, event.detail)
```

### Subgraph (dependency closure)

Default **`direction="deps"`** is **backward** along edges as reported by **`inspect_graph()`** (from a memoized query toward its recorded dependencies). **`direction="dependents"`** walks “upward” to transitive dependents.

```python
from cascade import Engine

engine = Engine()

@engine.input
def src(x: int) -> int:
    return 0

@engine.query
def mid(x: int) -> int:
    return src(x) + 1

@engine.query
def top(x: int) -> int:
    return mid(x) * 2

src.set(0, 3)
assert top(0) == 8
full = engine.inspect_graph()
leaf = next(n for n in full["nodes"] if n.startswith("query:") and "top" in n)
deps_only = engine.subgraph([leaf], direction="deps")
assert set(deps_only["nodes"]) <= set(full["nodes"])
print(deps_only["memo_count"], deps_only["edges"])
```

### Query stats (timing and eviction)

**`by_key`** totals **wall time** in seconds for **successful** recomputes only (cache hits are not timed). Each value is **inclusive** for that callable: if the body calls other queries, their execution time is included in the parent’s row as well as their own (do not sum **`by_key`** across the graph to get “total work”). **`stats_clock`** is optional for deterministic tests. Counters are **in-memory only** (**`save`** / **`load`** do not persist them).

```python
from cascade import Engine

engine = Engine(max_entries=2, stats=True)

@engine.input
def i() -> int:
    return 0

@engine.query
def slow() -> int:
    return i() + 1

@engine.query
def fast() -> int:
    return i() + 2

i.set(1)
slow()
fast()
extra = Engine(max_entries=1, stats=True)

@extra.input
def j() -> int:
    return 0

@extra.query
def a() -> int:
    return j()

@extra.query
def b() -> int:
    return j() + 1

j.set(0)
a()
b()
print(engine.stats_summary()["by_key"])
print(extra.stats_summary()["evictions_total"], extra.stats_summary()["evictions_recent"])
```

### Graph export (DOT and Mermaid)

Pure functions: pass any **`inspect_graph()`**-shaped dict (including **`subgraph(...)`**). Labels are escaped; large graphs are fine for **`dot`** layout (very large Mermaid diagrams may be slow in viewers).

```python
from cascade import Engine, export_dot, export_mermaid

engine = Engine()
# ... define inputs/queries, run something ...
g = engine.inspect_graph()
open("memo.dot", "w", encoding="utf-8").write(export_dot(g))
open("memo.mmd", "w", encoding="utf-8").write(export_mermaid(g))
```

**Mermaid:** newline characters inside keys are replaced with spaces so node syntax stays valid.

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
| `compute_many_with_accumulators.py` | `compute_many(..., effects=...)` accumulator collection |
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
