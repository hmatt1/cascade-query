# Cascade Query

Cascade Query is a Python library for incremental dependency tracking. It caches function results and re-executes them only when their specific inputs or upstream dependencies change.

[![PyPI version](https://img.shields.io/pypi/v/query-cascade?color=3775A9&label=PyPI)](https://pypi.org/project/query-cascade/)
[![Python versions](https://img.shields.io/pypi/pyversions/query-cascade?color=5A9)](https://pypi.org/project/query-cascade/)

---

## 1. Introduction & Quickstart

### Core Principles

1. **Automatic Caching:** Results are stored. If dependencies are unchanged, the function body does not execute.
2. **Dependency Tracking:** Cascade records every `@engine.input` or `@engine.query` accessed during execution.
3. **Targeted Updates:** When an input changes, Cascade identifies and invalidates only the affected downstream functions.
4. **Early Bail-out:** If a function's output remains identical after its dependencies change, re-computation stops for that branch.

### Installation

```bash
pip install query-cascade
# Or to enable persistent disk caching:
pip install query-cascade[disk]
```

### Quickstart

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

## 2. Core Concepts

### Engine Initialization & Configuration

The `Engine` manages caching, execution, and state tracking.

*   **`Engine(*, max_entries=10000, trace_limit=50000, stats=False, cache_dir=None, cache_map_size=2**30, cache_backend="mdbx", incremental=True)`**: Initializes the engine. 
    *   `max_entries` sets the limit for the Least Recently Used (LRU) in-memory cache. 
    *   Passing `cache_dir` enables persistent disk caching.
    *   `cache_backend` allows you to select the storage engine for persistence (`"mdbx"`, `"sqlite"`, or `"lmdb"`).

### Defining Nodes: `@engine.input` and `@engine.query`

*   **`@engine.input`**: Decorator for mutable data roots.
    *   `input.set(value)`: Updates the value and increments the global revision.
    *   `input.set(*args, value=value)`: Updates a keyed input.
*   **`@engine.query`**: Decorator for cached computations. Re-evaluates only if upstream data changes.

### Snapshots & Transactions (Input Debouncing)

*   **`engine.snapshot()`**: Returns a `Snapshot` object pinning the current global revision. Use `query(snapshot=s)` to read data as it existed at that revision.
*   **`engine.transaction()`**: When updating multiple inputs, intermediate read states ("flapping") can cause inconsistent evaluations. Use a transaction block to batch updates so they are committed atomically.

```python
with engine.transaction():
    theme.set("dark")
    layout.set("grid")
```

---

## 3. Execution Modes

### Synchronous Execution
Standard functions evaluate eagerly when called.

### Asynchronous Execution
Cascade natively supports asynchronous queries via `async def`. This is extremely useful for I/O-bound workflows (such as making database or network calls). When an asynchronous query evaluates, it runs cooperatively on the active `asyncio` event loop.

```python
import asyncio
from cascade import Engine

engine = Engine()

@engine.query
async def fetch_user(uid: str):
    await asyncio.sleep(0.5) # Network I/O
    return {"id": uid, "name": "Alice"}
    
@engine.query
async def process_user(uid: str):
    # Await downstream async queries seamlessly
    user = await fetch_user(uid)
    return user["name"].upper()

async def main():
    print(await process_user("123"))

asyncio.run(main())
```

Synchronous queries can also be called directly from inside asynchronous nodes (and vice versa). Cascade maintains type stability and performance segregation, ensuring pure-Python CPU-bound tasks suffer no context-switching overhead while I/O-bound tasks evaluate concurrently.

### Parallel & Background Execution
*   **`engine.submit(query, *args, executor=None)`**: Schedules a query for background execution. Returns a `concurrent.futures.Future`.
*   **`engine.compute_many(calls, workers=None)`**: Executes a list of queries in parallel using a thread pool. Blocks until all are complete and returns a list of results.
*   **`engine.compute_many_stream(calls, workers=None)`**: Yields completed results as each call finishes. Yields `(index, value, call_effects)` in completion order.
*   **`engine.shutdown(*, wait=True, cancel_futures=False)`**: Shuts down the default thread pool executor used by `submit`.
*   **`QueryCancelled`**: Exception raised if a background query's dependencies change before it completes.

---

## 4. Cache Management & Invalidations

### Targeted Updates & Early Bail-out
If a function's logic evaluates to a result identical to its previous run, early bail-out prevents re-computation of downstream queries.

### Code-Aware Auto-Invalidation
Cascade automatically inspects the Python bytecode of your `@engine.query` and `@engine.input` functions. If you edit a function's logic and the module is hot-reloaded (or you restart your script), Cascade compares the new function's bytecode hash against the previously cached logic. If the logic has changed, Cascade immediately invalidates that function's memory and persistent disk caches.

### Time-To-Live (TTL) Caching
Cascade supports expiring cache entries automatically based on wall-clock time. By passing the `ttl` flag to `@engine.query`, you can guarantee that a node will recompute its value if it is accessed after the specified number of seconds has elapsed.

```python
@engine.query(ttl=5.0)
def fetch_external_data():
    return request.get("https://api.example.com/data")
```

### Error Caching
By default, queries intercept and cache exceptions (excluding control flow exceptions like `QueryCancelled`).

```python
@engine.query(cache_exceptions=(ValueError, TypeError))
def parse(source: str):
    if not source:
        raise ValueError("Empty source")
    return {"ast": source}
```

### Pass-Through Queries (`memoize=False`)
For intermediate queries generating large outputs, set `memoize=False`. This saves significant memory. Downstream nodes will still accurately detect when the query's inputs change, and the unmemoized query will recompute its output on-demand when an active caller needs it.

```python
@engine.query(memoize=False)
def mapped_data() -> list[int]:
    data = raw_data()
    return [x * 2 for x in data]
```

### Garbage Collection & Pruning
*   **`engine.prune(roots, vacuum_disk=False)`**: Removes cached query results from the in-memory LRU cache that are not reachable from the specified roots. Set `vacuum_disk=True` to deep vacuum the persistent MDBX disk cache.
*   **`engine.access_id`**: Property returning a monotonically increasing sequence number for memo accesses.
*   **`engine.sweep_unaccessed(since_access_id)`**: Evicts all memos that haven't been accessed since `since_access_id`. Useful for generational garbage collection.

---

## 5. State, Side-Effects & Persistence

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

### Persistent Disk Caching
Passing `cache_dir` to the `Engine` turns on zero-config persistence. Cascade provisions an embedded store (MDBX by default), serializes query results with a deterministic msgpack encoding, and fingerprints every input value by hashing its serialized bytes with blake2b.

```python
from cascade import Engine
# Uses the default "mdbx" backend:
engine = Engine(max_entries=10_000, cache_dir=".cascade_cache")

# Or opt into an alternative backend, like SQLite:
sqlite_engine = Engine(max_entries=10_000, cache_dir=".cascade_cache", cache_backend="sqlite")
```
*   `libmdbx` and `msgpack` are required for the default backend (`pip install query-cascade[disk]`). You can also use `sqlite` which has no external dependencies.
*   Values and arguments must be serializable (primitives, bytes, lists, dicts, dataclasses, etc.).
*   **`engine.clear_disk_cache()`**: Deletes every entry in the persistent disk cache.

### Export / Import
*   **`engine.save(path)` / `engine.load(path)`**: Persists all inputs and cached results to a SQLite database. Note: Collection event logs are not saved here, use persistent collections instead.

---

## 6. Observability & Debugging

### Graph Inspection & Visualization
*   **`engine.inspect_graph(condense=False)`**: Returns a dictionary of all nodes and edges in the dependency graph.
*   **`engine.subgraph(roots, direction="deps")`**: Filters the graph to the dependency chain of the specified root nodes.

Cascade provides renderers for the dependency graph:
```python
from cascade import export_dot, export_mermaid

graph = engine.inspect_graph()
# Generate Graphviz DOT format
dot_text = export_dot(graph)
# Generate Mermaid flowchart format
mermaid_text = export_mermaid(graph)
```

### Performance Metrics
Set `stats=True` or use `engine.enable_stats()` to track execution timing.
*   **`engine.enable_stats(enabled=True)`**: Dynamically enables or disables performance tracking.
*   **`engine.stats_summary()`**: Returns wall-clock time spent in function bodies and cache eviction counts.
*   **`engine.reset_stats()`**: Clears accumulated timing data.

### Event Tracing
*   **`engine.traces()`**: Returns a list of `TraceEvent` objects detailing internal operations (like recomputes, backdates).
*   **`engine.clear_traces()`**: Clears the internal trace buffer.

---

## 7. Incremental Collections & Native Map/Reduce

`CascadeList`, `CascadeSet`, and `CascadeDict` behave like their builtin counterparts, but every mutation appends a diff to an event log. Queries written with ordinary comprehensions and reducers are rewritten at registration time into incremental pipelines that consume only new diffs. Appending one element to a list of a million reruns your mapping function once.

```python
from cascade import Engine, CascadeList

engine = Engine()
docs = CascadeList(engine, ["alpha", "beta", "gamma"], name="docs")

@engine.query
def long_doc_count():
    # Standard Python. Rewritten into filter -> len over the diff stream.
    return len([d for d in docs if len(d) > 4])

long_doc_count()      # walks all elements once
docs.append("delta")  # one diff
long_doc_count()      # processes only "delta"
```

### The collections

*   **`CascadeList`** emits insert/update/remove diffs with hidden monotonic uids.
*   **`CascadeSet`** emits add/remove diffs.
*   **`CascadeDict`** emits upsert/remove diffs.

### What gets rewritten

Rewriting covers comprehensions (`[...]`, `{...}`, `{k: v ...}`), `map`, `filter`, `reversed`, and reducers: `sum`, `len`, `min`, `max`, `any`, `all`, `sorted`, `list`, `set`, `dict`, and string-literal `.join`.

| Reducer | Ingest per diff | Finalize |
|---------|-----------------|----------|
| `sum`, `len`, `any`, `all` | O(1) | O(1) |
| `min`, `max` | O(log N) | O(1) |
| `sorted` | O(log N) | O(output) |
| `list`, `set`, `dict`, `.join` | O(1) | O(output) |
| `reversed` | O(1) positional flip | inherited |

*   Arbitrary `for` loops are never rewritten, but invalidate through read tracking.
*   `enumerate()` and `zip()` sources are explicitly excluded.
*   Opt out globally via `Engine(incremental=False)` or per-query `@engine.query(incremental=False)`.

### Batching Mutations for Disk Performance

By design, Cascade collections use an immediate write-ahead pattern for crash-safety. Every time you mutate a collection (`__setitem__`, `append()`, `add()`), Cascade immediately opens a disk transaction, writes the diff to the MDBX event log, and forces an OS sync (`fsync`). 

If you iterate through a loop inserting thousands of records one by one, you will trigger thousands of physical disk syncs, resulting in extremely slow performance (e.g., waiting on disk I/O).

To avoid this, always align disk syncs with your **logical units of work** by batching bulk-inserts using `.update()` or `extend()`. This allows Cascade to group all changes into a single disk transaction:

```python
# SLOW (10,000 separate disk syncs)
for i in range(10000):
    docs[f"doc_{i}"] = i

# FAST (1 single disk sync)
docs.update({f"doc_{i}": i for i in range(10000)})
```

*(Note: `.update()` behaves exactly like a standard Python dictionary. If a key already exists, it will seamlessly overwrite the old value by batching an `"upsert"` event to the underlying log, ensuring downstream queries receive the correct diffs.)*

### Persistence & Inspection

A **named** collection on an engine with `cache_dir` event-sources its log to disk automatically.

```python
engine = Engine(cache_dir="./cache")
items = CascadeList(engine, name="items", compact_every=1024)
```

*   **`engine.inspect_pipelines()`**: Lists every observed pipeline with its source, logical stages, fused stage count, checkpoint revision, and consuming queries.

---

## 8. Limitations & Development

### Limitations

1. **Cycle Detection:** Cascade detects and rejects recursive function calls (cycles) with a `CycleError`.
2. **Thread Safety:** While Cascade supports parallel query execution, the `Engine` object itself should be modified (`.set()`, `@engine.query`) from a single thread or with external synchronization.
3. **Persistence Security:** `engine.load()` and the persistent disk cache resolve `@dataclass` and `NamedTuple` types via `importlib`. Only load databases or open cache directories from trusted sources.
4. **Python Version:** Optimization for parallel CPU-bound work requires **CPython 3.14+ free-threaded** builds with `PYTHON_GIL=0`.
5. **Collections & `engine.save()`:** State snapshots via `engine.save()`/`engine.load()` do not carry collection event logs; use a named collection with `cache_dir` for durable collection state.

### Examples (in `examples/`)

| Script | What it shows |
|--------|----------------|
| `compiler_pipeline.py` | `source → parse → symbols → typecheck`, warnings accumulator, cache-hit narration |
| `incremental_collections.py` | CascadeList/Set/Dict diffs, comprehension rewriting, O(1) ingest, pipeline inspection |
| `async_execution.py` | Asynchronous query evaluation and asyncio event loop integration for IO-bound work |
| `ttl_invalidation.py` | Expiring stale query caches automatically based on wall-clock time |
| `error_caching.py` | Basic exception caching to prevent repeated re-evaluation on failure |
| `error_caching_persistence.py` | Disk cache hydration of exceptions across process runs |
| `code_versioning.py` | Automatic cache invalidation when a function's bytecode logic changes |
| `pass_through_queries.py` | `memoize=False` tracking inputs without keeping large outputs in the LRU cache |
| `dynamic_macro_expansion.py` | Query that **changes downstream dependencies** at runtime |
| `snapshot_isolation.py` | Snapshot reads while live inputs change |
| `concurrent_background_work.py` | Dedup under concurrency + cancellation after input changes |
| `compute_many_with_accumulators.py` | `compute_many(..., effects=...)` accumulator collection |
| `persistence_and_inspection.py` | Save/load and graph summaries |
| `gil_parallel_speedup.py` | Threaded CPU benchmark: GIL vs free-threaded |

### Development & Tests

```bash
export PYTHON_GIL=0
python3.14t -m pip install -e ".[dev]"
python3.14t -m pytest -q --cov=src/cascade --cov-branch --cov-report=term-missing
```

*   **Stateful fuzz**: `PYTHON_GIL=0 python3.14t -m pytest -q tests/test_stateful_engine_invariants.py`
*   **Formal model**: TLA+ specs live under `docs/formal/`.
*   **Performance suite**: `python -m benchmarks.performance_suite --report-dir artifacts/performance --assert-thresholds`
