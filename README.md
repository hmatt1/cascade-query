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
*   **`Engine(max_entries=10000, stats=False)`**: Initializes the engine. `max_entries` sets the limit for the Least Recently Used (LRU) cache.
*   **`@engine.input`**: Decorator for mutable data roots.
    *   `input.set(value)`: Updates the value and increments the global revision.
    *   `input.set(*args, value=value)`: Updates a keyed input.
*   **`@engine.query`**: Decorator for cached computations.
*   **`engine.snapshot()`**: Returns a `Snapshot` object pinning the current global revision. Use `query(snapshot=s)` to read data as it existed at that revision.
*   **`engine.save(path)` / `engine.load(path)`**: Persists all inputs and cached results to a SQLite database.

### Parallel & Background Execution
*   **`engine.compute_many(calls, workers=None)`**: Executes a list of queries in parallel using a thread pool.
*   **`engine.submit(query, *args, executor=None)`**: Schedules a query for background execution. Returns a `concurrent.futures.Future`.
*   **`QueryCancelled`**: Exception raised if a background query's dependencies change before it completes.

### Graph Utilities
*   **`engine.inspect_graph()`**: Returns a dictionary of all nodes and edges in the dependency graph.
*   **`engine.subgraph(roots, direction="deps")`**: Filters the graph to the dependency chain of the specified root nodes.
*   **`engine.prune(roots)`**: Removes cached query results that are not reachable from the specified roots.

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
3. **Persistence Security:** `engine.load()` uses JSON to resolve types via `importlib`. Only load databases from trusted sources.
4. **Python Version:** Optimization for parallel CPU-bound work requires **CPython 3.14+ free-threaded** builds with `PYTHON_GIL=0`.

---

## Installation

```bash
pip install query-cascade
```
