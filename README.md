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
*   **`engine.prune(roots)`**: Removes cached query results that are not reachable from the specified roots.

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
