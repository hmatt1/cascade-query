# Cascade Query

Cascade Query is a Python library for incremental dependency tracking. It caches function results and re-executes them only when their specific inputs or upstream dependencies change.

[![PyPI version](https://img.shields.io/pypi/v/query-cascade?color=3775A9&label=PyPI)](https://pypi.org/project/query-cascade/)
[![Python versions](https://img.shields.io/pypi/pyversions/query-cascade?color=5A9)](https://pypi.org/project/query-cascade/)

---

## Core Principles

1. **Automatic Caching:** Results are stored. If dependencies are unchanged, the function body does not execute.
2. **Dependency Tracking:** Cascade records every `@engine.input` or `@engine.query` accessed during execution.
3. **Targeted Updates:** When an input changes, Cascade identifies and invalidates only the affected downstream functions.
4. **Early Bail-out:** If a function's output remains identical after its dependencies change, re-computation stops for that branch.

---

## Quickstart

```python
import time
from cascade import Engine

engine = Engine()

@engine.input
def user_id():
    return "user_1"

@engine.query
def fetch_data():
    time.sleep(2) 
    return {"id": user_id(), "data": "value"}

@engine.query
def get_result():
    data = fetch_data()
    return f"Result for {data['id']}"

# First run: Executes for 2 seconds.
print(get_result())

# Second run: Returns immediately from cache.
print(get_result())

# Update input:
user_id.set("user_2")

# Third run: Executes for 2 seconds to refresh.
print(get_result())
```

---

## Engine API

### Core Methods
*   **`Engine(max_entries=10000, stats=False, cache_dir=None, cache_map_size=2**30)`**: Initializes the engine. `max_entries` sets the limit for the Least Recently Used (LRU) cache. Passing `cache_dir` enables persistent disk caching (see below).
*   **`@engine.input`**: Decorator for mutable data roots.
    *   `input.set(value)`: Updates the value and increments the global revision.
    *   `input.set(*args, value=value)`: Updates a keyed input.
*   **`@engine.query`**: Decorator for cached computations.
*   **`engine.snapshot()`**: Returns a `Snapshot` object pinning the current global revision. Use `query(snapshot=s)` to read data as it existed at that revision.
*   **`engine.save(path)` / `engine.load(path)`**: Persists all inputs and cached results to a SQLite database.
*   **`engine.clear_disk_cache()`**: Deletes every entry in the persistent disk cache. Raises if the engine was created without `cache_dir`.

### Parallel & Background Execution
*   **`engine.compute_many(calls, workers=None)`**: Executes a list of queries in parallel using a thread pool.
*   **`engine.submit(query, *args, executor=None)`**: Schedules a query for background execution. Returns a `concurrent.futures.Future`.
*   **`QueryCancelled`**: Exception raised if a background query's dependencies change before it completes.

### Graph Utilities
*   **`engine.inspect_graph()`**: Returns a dictionary of all nodes and edges in the dependency graph.
*   **`engine.subgraph(roots, direction="deps")`**: Filters the graph to the dependency chain of the specified root nodes.
*   **`engine.prune(roots, vacuum_disk=False)`**: Removes cached query results from the in-memory LRU cache that are not reachable from the specified roots. Set `vacuum_disk=True` to also do a deep vacuum of the persistent LMDB disk cache, deleting all orphaned blobs and metadata.

---

## Persistent Disk Caching

Passing `cache_dir` to the `Engine` turns on zero-config persistence. Cascade provisions an embedded LMDB store in that directory, serializes query results with a deterministic msgpack encoding, and fingerprints every input value by hashing its serialized bytes with blake2b. Nothing else changes: queries and inputs are written exactly as before.

```python
from cascade import Engine

engine = Engine(max_entries=10_000, cache_dir=".cascade_cache")

@engine.input
def package_source_text(pkg: str) -> str:
    with open(pkg, "r") as f:
        return f.read()

@engine.query
def parsed_package_ast(pkg: str):
    return parse(package_source_text(pkg))
```

The first run executes normally and writes each result to disk. A later run in a new process starts with an empty in-memory cache, finds the entry on disk, and verifies it top-down: leaf inputs are re-executed and re-hashed (for the input above, that means re-reading the file), and the current hashes are compared against the fingerprints saved with the entry. If everything matches, the stored value is deserialized and returned without running any query body. If a file changed, its hash mismatches, and exactly the queries downstream of that file recompute. Early bail-out works across sessions too, since dependency fingerprints are content hashes: a whitespace-only edit that leaves an intermediate result unchanged will not recompute anything past it.

Accumulator effects are stored with each entry and replayed on disk hits, so a warning emitted in run 1 still appears in run 2 even when the query is served from disk.

`lmdb` and `msgpack` are required once `cache_dir` is set; there is no fallback, and the engine raises `PersistentCacheError` with install instructions if either is missing:

```bash
pip install query-cascade[disk]
```

A few things to know:

*   Values and arguments must be serializable: primitives, bytes, `list`/`tuple`/`set`/`frozenset`/`dict`, `@dataclass` instances, and `typing.NamedTuple` instances. A query that returns anything else raises at compute time when persistence is on. A query called with an unserializable argument still computes and memoizes in memory, it just skips the disk.
*   Cache addresses are derived from the function id (`module:qualname`) and the hashed arguments, so renaming or moving a function starts it from a cold cache. Editing a function body does not invalidate its entries; bump the cache with `engine.clear_disk_cache()` or delete the directory when query logic changes.
*   The store supports concurrent access from multiple processes through LMDB's own locking. Within one process, engines sharing a `cache_dir` share one LMDB environment; the first opener's `cache_map_size` wins.
*   The default `cache_map_size` is 1 GiB. LMDB allocates this lazily, so the file only grows as entries are written. If the cache fills up, `PersistentCacheError` explains the options.
*   The on-disk data is a cache: clearing it is always safe and only costs recomputation. Cascade wipes it automatically when its own storage format version changes.

---

## Advanced Features

### Side-Effect Accumulators
Queries must be pure functions. Use `Accumulator` to record side-effects (like logs or warnings) that must be replayed when a result is served from the cache.

```python
warnings = engine.accumulator("warnings")

@engine.query
def validate_data():
    data = fetch_data()
    if not data:
        warnings.push("No data found")
    return data

# On cache hit, 'warnings' are re-populated into the effects dictionary.
effects = {}
validate_data(effects=effects)
print(effects["warnings"])
```

### Performance Metrics
Set `stats=True` in the `Engine` constructor to track execution timing.
*   **`engine.stats_summary()`**: Returns wall-clock time spent in function bodies and cache eviction counts.
*   **`engine.reset_stats()`**: Clears accumulated timing data.

---

## Visualization
Cascade provides renderers for the dependency graph.

```python
from cascade import export_dot, export_mermaid

graph = engine.inspect_graph()
# Generate Graphviz DOT format
dot_text = export_dot(graph)
# Generate Mermaid flowchart format
mermaid_text = export_mermaid(graph)
```

---

## Limitations

1. **Cycle Detection:** Cascade detects and rejects recursive function calls (cycles) with a `CycleError`.
2. **Thread Safety:** While Cascade supports parallel query execution, the `Engine` object itself should be modified (`.set()`, `@engine.query`) from a single thread or with external synchronization.
3. **Persistence Security:** `engine.load()` and the persistent disk cache resolve `@dataclass` and `NamedTuple` types via `importlib`. Only load databases or open cache directories from trusted sources.
4. **Python Version:** Optimization for parallel CPU-bound work requires **CPython 3.14+ free-threaded** builds with `PYTHON_GIL=0`.

---

## Installation

```bash
pip install query-cascade
```

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
