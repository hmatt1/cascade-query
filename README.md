# Cascade Query

<p align="center">
  <a href="https://pypi.org/project/query-cascade/"><img alt="PyPI version" src="https://img.shields.io/pypi/v/query-cascade?color=3775A9&label=PyPI"></a>
  <a href="https://pypi.org/project/query-cascade/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/query-cascade?color=5A9"></a>
  <a href="https://pypi.org/project/query-cascade/"><img alt="Distribution format" src="https://img.shields.io/pypi/wheel/query-cascade?color=4CAF50"></a>
</p>

**Releases:** https://pypi.org/project/query-cascade/

`cascade-query` is a minimal, demand-driven incremental computation framework for Python.

## Windows setup (free-threaded Python + PyPI install)

`query-cascade` targets **free-threaded CPython**. On Windows, install the free-threaded build first, then install from PyPI.

1. Install free-threaded Python 3.14 from https://www.python.org/downloads/windows/
   - Use the installer entry that includes **free-threaded** (`python3.14t` / launcher target `-3.14t`).
   - If using the standard installer, ensure the free-threaded binaries option is enabled.
2. Install the latest `query-cascade` globally:

```powershell
py -3.14t -m pip install -U query-cascade
```

3. Verify the setup (import works + free-threaded runtime with GIL disabled):

```powershell
py -3.14t -X gil=0 -c "import cascade, sys, sysconfig; print('cascade import ok from', cascade.__file__); print('Py_GIL_DISABLED=', sysconfig.get_config_var('Py_GIL_DISABLED')); print('GIL enabled?', sys._is_gil_enabled())"
```

Expected verification output includes:
- `Py_GIL_DISABLED= 1`
- `GIL enabled? False`

It is designed for compiler-like workloads where you want:

- lazy pull-based evaluation
- precise dependency tracking
- red-green early bailout (backdating)
- query dedup across concurrent callers
- snapshot isolation for concurrent reads
- safe cancellation of obsolete background work
- side-effect replay on cache hits
- persistence + graph inspection

## Minimal API

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

### Primitives

- `engine.input(fn)`  
  Wraps mutable roots. Use `.set(...)` to create new revisions.
- `engine.query(fn)`  
  Wraps pure demand-driven queries with memoization and dependency capture.
- `engine.accumulator(name)`  
  Creates thread-safe side-effect channels replayed on cache hits.
- `engine.snapshot()`  
  Captures an immutable read view (`Snapshot`) for MVCC-like isolation.
- `engine.submit(query, *args, snapshot=...)`  
  Runs a query in the background with cancellation if inputs mutate.
- `engine.compute_many([(query, args), ...], workers=N)`  
  Multi-threaded execution with a work-stealing scheduler.
- `engine.inspect_graph()` / `engine.traces()`  
  Introspection hooks for diagnostics.
- `engine.save(path)` / `engine.load(path)`  
  Persist or recover graph/cached state from SQLite.
- `engine.prune(roots)`  
  Garbage-collect memoized subgraphs not reachable from roots.

## Design Notes

### What this framework guarantees

- **Smart recalculation**: only stale demand paths recompute.
- **Selective updates**: unchanged parents remain green after child backdating.
- **Query deduplication**: one in-flight compute serves all identical concurrent requests.
- **Cycle detection**: recursive query cycles raise `CycleError`.
- **Cancellation**: stale background queries raise `QueryCancelled`.

### Limitations and enforceability boundaries

- **Free-threaded runtime requirement**: this project targets free-threaded CPython (`3.14t`) with runtime GIL disabled. If a non-free-threaded interpreter is used, or if imported extensions force GIL re-enable, CPU-bound parallel scaling will degrade.
  - **Publish/build environment vs runtime compatibility**: publishing wheels/sdists from a non-free-threaded interpreter does not, by itself, prevent installation or execution on free-threaded CPython. Compatibility is determined at install/runtime by the interpreter and dependency stack. This package is pure Python, so there is no extension ABI lock to a specific GIL mode.
- **Process-level durability model**: persistence is an explicit point-in-time snapshot (`save`/`load`), not a transactional WAL-backed MVCC store shared by multiple live processes.
- **Boundary of side-effect replay guarantees**: replay is guaranteed only for effects emitted through `Accumulator`; out-of-band side effects in query bodies (printing, network calls, filesystem writes) are intentionally not replayed.
- **Cycle handling scope**: direct and long-chain dynamic query cycles are detected and raised as `CycleError`; this engine does not implement fixed-point solvers for cyclic dataflow.

### What this framework intentionally does not include

To keep API surface minimal, this version does not include:

- nominal interning APIs (`@interned`) and tracked structs (`@tracked`)
- fixed-point cycle solvers
- distributed/shared cache protocols

Those can be layered on top without changing the core query model.

## Use-case fit checklist

Use this checklist to decide if a problem is a strong fit for `query-cascade`.
The best candidates are graph-shaped, mostly pure computations that run often on
mostly stable state.

Mark each item Yes/No:

1. **Graph-shaped derivation:** Can the logic be represented as inputs feeding
   derived queries (`A -> B -> C`)?
2. **Mostly pure compute:** Are derived results mostly pure functions of
   tracked inputs (with minimal out-of-band side effects)?
3. **High read repetition:** Are the same queries asked repeatedly?
4. **Small incremental writes:** Do inputs usually change in small increments
   instead of full replacement each run?
5. **Expensive recompute cost:** Is full recomputation meaningfully expensive?
6. **Need precise invalidation:** Do you need to recompute only affected
   downstream nodes?
7. **Concurrency overlap:** Do concurrent callers often request identical keys?
8. **Snapshot consistency need:** Do some callers need a stable point-in-time
   view while writes continue?
9. **Stale background work risk:** Do background computations become obsolete
   when new writes arrive?
10. **Debug/explainability need:** Is it useful to inspect why recomputation,
    cache hits, or invalidation happened?

Interpretation:

- **9-10 Yes:** excellent fit
- **7-8 Yes:** strong fit
- **5-6 Yes:** possible fit; validate with a small prototype
- **0-4 Yes:** likely not the right abstraction

### Specific problems that pass this checklist

The following concrete problems are typically excellent fits (9-10 Yes):

1. **Incremental IDE analysis pipeline**
   - Parse, symbol index, type diagnostics, code actions per edited file.
2. **Monorepo impacted-test planner**
   - Compute the minimal set of tests/checks affected by a PR diff.
3. **Incremental static analyzer/security scanner**
   - Recompute findings only for changed code and affected dependents.
4. **Compiler or DSL transpiler with live diagnostics**
   - Reuse parse/lowering/type stages and replay warnings on cache hits.
5. **Policy-as-code evaluator for IaC changes**
   - Re-evaluate only impacted resources/rules after config edits.
6. **Feature-flag or entitlement resolution engine**
   - Compute effective access from plans, flags, and org/user overrides.
7. **Schema compatibility and migration impact checker**
   - Track which downstream contracts break when one schema evolves.
8. **Derived analytics metric compiler**
   - Incrementally compile metric definitions and dependency expansions.
9. **Asset/build graph incremental planner**
   - Rebuild only outputs impacted by source, config, or toolchain changes.
10. **Large rules engine with concurrent duplicate requests**
    - Deduplicate in-flight computation for identical rule evaluations.

## Quickstart

```python
from cascade import Engine

engine = Engine()

@engine.input
def text() -> str:
    return ""

@engine.query
def lint_count() -> int:
    value = text()
    return value.count("TODO")

text.set("TODO: one\nTODO: two")
assert lint_count() == 2

# No recompute needed if input did not semantically change.
text.set("TODO: one\nTODO: two")
assert lint_count() == 2
```

## Examples

- `examples/compiler_pipeline.py`  
  Tiny compiler pipeline (`source -> parse -> symbol_names -> typecheck`) with warnings accumulator and cache-hit narration.
- `examples/dynamic_macro_expansion.py`  
  Runtime macro-expansion query that dynamically changes downstream graph dependencies.
- `examples/snapshot_isolation.py`  
  Demonstrates immutable snapshot reads while live inputs continue to change.
- `examples/concurrent_background_work.py`  
  Shows concurrent deduplicated query execution plus stale background cancellation after input mutation.
- `examples/persistence_and_inspection.py`  
  Saves engine state, loads it into a new engine, and inspects memo/input graph summaries.
- `examples/gil_parallel_speedup.py`  
  Benchmarks CPU-bound threaded work so you can compare `python3.14` (GIL) vs `python3.14t` (free-threaded).

### Running examples

Install package + test dependencies once:

```bash
python3.14t -m pip install -e ".[dev]"
```

Run a single example:

```bash
python3.14t examples/compiler_pipeline.py
```

Run all examples:

```bash
for example in examples/*.py; do
  echo "Running $example"
  python3.14t "$example"
done
```

Examples print step-by-step narration while they run so you can follow each behavior they demonstrate.

### Compare `python3.14` vs free-threaded performance

Install both interpreters so you can run the same benchmark twice:

- **CPython 3.14 (with GIL):** `python3.14`
- **Free-threaded CPython 3.14:** `python3.14t`

Example install on Ubuntu (deadsnakes):

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.14 python3.14-venv python3.14t python3.14t-venv
```

Install this project for both interpreters:

```bash
python3.14 -m pip install -e .
python3.14t -m pip install -e .
```

Quick run (same free-threaded interpreter, toggle runtime GIL mode):

```bash
python3.14t -c "import sys, sysconfig; print('Py_GIL_DISABLED=', sysconfig.get_config_var('Py_GIL_DISABLED')); print('GIL enabled?', sys._is_gil_enabled())"
PYTHON_GIL=1 python3.14t examples/gil_parallel_speedup.py --workers 8 --tasks 96 --rounds 300000 --repeats 5
PYTHON_GIL=0 python3.14t examples/gil_parallel_speedup.py --workers 8 --tasks 96 --rounds 300000 --repeats 5
```

Alternative (compare separate interpreter builds directly):

```bash
python3.14 examples/gil_parallel_speedup.py --workers 8 --tasks 96 --rounds 300000 --repeats 5
PYTHON_GIL=0 python3.14t examples/gil_parallel_speedup.py --workers 8 --tasks 96 --rounds 300000 --repeats 5
```

Compare these lines from each run:

- `median parallel seconds`
- `threaded speedup in this runtime`

Interpretation:

- lower `median parallel seconds` is better
- higher `threaded speedup in this runtime` is better

Benchmark hygiene tips:

- keep arguments identical between runs
- run with minimal background CPU load
- use `--repeats` (for example `5`) to reduce noise

On multi-core hardware, free-threaded CPython with `PYTHON_GIL=0` should usually show substantially better threaded speedup for this CPU-bound workload.

## Persistence and inspection

```python
engine.save("state.db")
engine.load("state.db")
print(engine.inspect_graph())
for event in engine.traces():
    print(event.event, event.key, event.detail)
```

## Analysis review of the uploaded notes

This project now assumes a free-threaded CPython baseline and aligns with state-of-the-art incremental systems:

- Correct: pull-based demand, red-green early bailout, dependency graph capture, dedup, MVCC snapshots, cancellation, side-effect replay, tracing, and persistence.
- CPU-bound parallelism is expected on multi-core hardware when running with free-threaded CPython and GIL disabled.
- Overreach for this minimal implementation: unsafe-pointer lifetime tricks, red/green syntax tree internals, and fixed-point cycle solving are advanced optimizations that are not required for a practical minimal API.

## Running tests

```bash
python3.14t -m pip install -e ".[dev]"
python -c "import sys, sysconfig; print('Py_GIL_DISABLED=', sysconfig.get_config_var('Py_GIL_DISABLED')); print('GIL enabled?', sys._is_gil_enabled())"
pytest -q
```

Run the stateful invariant fuzz test directly:

```bash
PYTHON_GIL=0 python3.14t -m pytest -q tests/test_stateful_engine_invariants.py
```

Run mutation testing (test-strength check):

```bash
PATH="$HOME/.local/bin:$PATH" PYTHON_GIL=0 mutmut run
PATH="$HOME/.local/bin:$PATH" PYTHON_GIL=0 mutmut results
```

Use the `mutmut` CLI entrypoint (`mutmut run`) instead of `python -m mutmut run`.

## Performance checks and report

Performance-sensitive behavior in this project is concentrated around:

- cache-hit verification (green-path checks) versus full recomputation cost
- concurrent query deduplication under contention
- scheduler throughput for `compute_many` on free-threaded CPU workloads
- giant-graph targeted mutation latency versus full rebuild latency
- mark-green verification overhead as dependency depth grows
- prune runtime scaling from small to large graphs

Run the performance suite locally:

```bash
python -m benchmarks.performance_suite --report-dir artifacts/performance --assert-thresholds
```

This writes:

- `artifacts/performance/performance-report.json`
- `artifacts/performance/performance-report.md`

CI executes the same suite on each build and uploads the report as an artifact named `performance-report`.

### Nightly long-running performance workflow

A separate GitHub Actions workflow (`.github/workflows/nightly-performance.yml`) runs a longer perf sweep on a nightly schedule (and on demand via `workflow_dispatch`):

- executes the performance suite repeatedly (currently 8 runs) to improve signal quality
- emits an aggregated summary and artifact bundle (`nightly-performance-report`)

## Scale and stress test categories

The test suite now includes scale-focused correctness tests in `tests/test_scale_behavior.py`:

- giant graph selective invalidation with recompute-count assertions
- dynamic dependency churn (stale edge cleanup and consistency checks)
- prune stress (large memo graphs with narrow retention roots)
- persistence round-trip at scale (post-load cache-hit behavior)
- eviction policy behavior under heavy churn
- mixed concurrency stress (`submit` + `compute_many` + frequent writes)

Some of the heaviest graph and concurrency scenarios are marked `@pytest.mark.slow` to keep default CI deterministic and fast while still running a representative giant-graph test by default.

Internal-invariant checks are intentionally centralized in
`tests/test_internal_invariants.py`. This keeps private-engine coupling minimal
while preserving a focused safety net for internal consistency.
Invariant-oriented internal probes now flow through a single private object
(`engine._internals`), while older private attributes remain compatibility
aliases during migration.

Run default CI-equivalent tests:

```bash
pytest -q
```

Run only slow scale/stress tests locally:

```bash
pytest -q -m slow
```

Run all tests including slow:

```bash
pytest -q -m "slow or not slow"
```

## CI best practices included

- GitHub Actions workflow at `.github/workflows/ci.yml`.
- Runs on both pushes and pull requests.
- Linting with `ruff` before tests.
- Separate package-build job (`python -m build`) to catch packaging regressions early.
