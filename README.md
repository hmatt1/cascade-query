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
    *   `cache_backend` allows you to select the storage engine for persistence (`"mdbx"` or `"sqlite"`).

### Defining Nodes: `@engine.input` and `@engine.query`

*   **`@engine.input`**: Decorator for mutable data roots.
    *   `input.set(value)`: Updates the value and increments the global revision. Setting the value the input already holds is a no-op: the revision does not change and nothing is invalidated or cancelled.
    *   `input.set(*args, value=value)`: Updates a keyed input.
*   **`@engine.query`**: Decorator for cached computations. Re-evaluates only if upstream data changes.

A few rules apply to every engine, with or without `cache_dir`:

*   **Values must be encodable.** Results and input values are fingerprinted with the same encoder the disk cache uses, so they must be primitives, bytes, builtin containers, `datetime` types, dataclasses, `NamedTuple`s or exceptions. Anything else (`Enum`, `Path`, `Decimal`, arbitrary class instances) raises `UnencodableValueError`, a `TypeError` whose message names the query or input, the path to the offending part of the value (`result[2]['k'].field`) and its type. Such a result is not cached.
*   **Results are returned by reference.** The caller gets the cached object itself; mutating it changes the cache.
*   **A query's or input's identity is a name: its `module:qualname` by default.** One name holds one function. Registering a different function under a name in use replaces the earlier one: its cached results are dropped, and its handle raises `StaleHandleError` when called. That is what you want from a hot reload (pick up the new handle by importing it again), but it also happens when several functions are made from one body, as in a factory, a loop, or lambdas in one module. Give those their own identities with `name=`:

    ```python
    def make_scaler(k: int):
        @engine.query(name=f"scale_by_{k}")
        def scale(x: int) -> int:
            return k * x
        return scale

    double, triple = make_scaler(2), make_scaler(3)   # two queries, two caches
    threshold = engine.input(lambda: 10, name="threshold")
    ```

    The name is also the identity in the disk cache, so keep it stable across runs.
*   **Arguments are positional and hashable.** Arguments that compare equal share one cached result (`q(1)` and `q(1.0)`); a dict cannot be an argument.

### Snapshots & Transactions (Input Debouncing)

*   **`engine.snapshot()`**: Returns a `Snapshot` object pinning the current global revision. Use `query(snapshot=s)` to read data as it existed at that revision.
*   **`engine.transaction()`**: When updating multiple inputs, intermediate read states ("flapping") can cause inconsistent evaluations. Use a transaction block to batch updates so they are committed atomically. Only the last value set for each input counts, inputs that end at their current value are skipped, and a transaction that changes nothing leaves the revision alone. Collection changes made inside the block show up on the collection objects right away, and are undone if the block raises. Queries run inside the block still see the engine as it was before the block, so a collection created in the block reads as empty to them until it commits. A transaction object is single-use: call `engine.transaction()` for each block; entering the same object a second time raises `RuntimeError`. A transaction belongs to the asyncio task that entered it (or the thread, when entered outside any task): threads and tasks started inside the block are not part of it, so a `set()` they make applies at once, and a change they make to a collection the transaction has already changed raises `RuntimeError` until the transaction ends.

The engine keeps old input values only while something can still read them: a snapshot that has not been released, or a call that is still running. Release a snapshot when you are done with it, either with a `with` block or `release()`; dropping every reference to it also releases it. Reading through a released snapshot raises `SnapshotExpiredError` once its history has been trimmed.

```python
with engine.snapshot() as before:
    price.set(12)
    assert total(snapshot=before) != total()
# History older than the current revision can now be dropped.
```

Collections trim their in-memory diff logs the same way: diffs no live snapshot, open transaction or pipeline still needs are folded into the collection's base, so long-running ingest keeps memory proportional to the collection's size.

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

One exception: synchronous code running on the event loop's own thread (a sync query called from a coroutine) cannot wait for an async query, because the wait would block the loop the async query needs. It gets an async query's cached result as usual; when the async query would have to run, or is running as a task on that loop, the call raises `EventLoopBlockedError` at once instead of hanging. Await the async query from async code, or move the synchronous call off the loop with `await asyncio.to_thread(sync_query)`.

### Parallel & Background Execution
*   **`engine.submit(query, *args, executor=None)`**: Schedules a query for background execution. Returns a `concurrent.futures.Future`. Calling it from inside a running query raises `RuntimeError`, because the future could outlive the query and its read could not be tracked; call the query directly or use `compute_many` there.
*   **`engine.compute_many(calls, workers=None)`**: Executes a list of queries in parallel using a thread pool. Blocks until all are complete and returns a list of results. Inside a running query, each requested query becomes a dependency of the calling query, just as if it had been called directly (`memoize=False` queries then run on the calling thread).
*   **`engine.compute_many_stream(calls, workers=None)`**: Yields completed results as each call finishes. Yields `(index, value, call_effects)` in completion order. Tracks dependencies inside a query the same way `compute_many` does.
*   **`engine.shutdown(*, wait=True, cancel_futures=False)`**: Shuts down the default thread pool executor used by `submit`. With `cache_dir`, it also writes any results still queued for the disk cache and closes it; like `flush_disk()`, it raises `PersistentCacheError` if a result could not be written and that was not reported yet.
*   **`QueryCancelled`**: Exception raised if a background query's dependencies change before it completes. Only queries started with `submit` are cancelled; direct calls, `compute_many` and `compute_many_stream` always run to completion. A direct call that joins a submitted run of the same query is not cancelled with it: it runs the query itself at its own revision.

---

## 4. Cache Management & Invalidations

### Targeted Updates & Early Bail-out
If a function's logic evaluates to a result identical to its previous run, early bail-out prevents re-computation of downstream queries.

"Identical" is stricter than `==`: `1`, `1.0` and `True` are different results, and so are two dicts with the same items in a different insertion order, because a query that iterates the dict can tell them apart. Sets have no order and compare by their elements. The same rule decides whether `input.set(value)` is a no-op. If a query builds a dict whose order carries no meaning (for example from iterating a set), return `dict(sorted(d.items()))` so that equal contents stop propagation and are reused across runs.

### Code-Aware Auto-Invalidation
Cascade automatically inspects the Python bytecode of your `@engine.query` and `@engine.input` functions. If you edit a function's logic and the module is hot-reloaded (or you restart your script), Cascade compares the new function's bytecode hash against the previously cached logic when the function is registered again. If the logic has changed, Cascade invalidates that function's memory and persistent disk caches at that point, and results that other queries computed from the old function are re-run the next time they are used, whether or not an input changed in between. Handles to the old function become stale: calling one raises `StaleHandleError` rather than running the old code, so modules that imported the handle need to import it again after the reload.

The hash covers only the function's own bytecode, constants and names. It does not cover helper functions the query calls, closure values, default arguments or globals, so editing those leaves cached results in place; clear the cache (`engine.clear_disk_cache()`) after such an edit.

### Time-To-Live (TTL) Caching
Cascade supports expiring cache entries automatically based on wall-clock time. By passing the `ttl` flag to `@engine.query`, you ask for a node to recompute its value if it is needed after the specified number of seconds has elapsed. That covers queries that depend on it too: calling one after the TTL has elapsed re-runs the expired query and then whatever its new value changes, with no input change needed. If the new value equals the old one, the dependents are not re-run. Queries with no TTL query beneath them are unaffected.

```python
@engine.query(ttl=5.0)
def fetch_external_data():
    return request.get("https://api.example.com/data")
```

### Fixed-Point Cycles
A query that calls itself, directly or through other queries, raises `CycleError`. Passing `fixed_point=<initial guess>` to `@engine.query` turns it into the head of the cycle instead: the call that reaches it again gets the current guess, and the query is re-run until a run returns the value it was given as the guess.

```python
@engine.query(fixed_point=0.0)
def bonus_pool() -> float:
    return round(net_profit() * 0.10, 2)

@engine.query
def net_profit() -> float:
    return base_profit() - bonus_pool()
```

The iteration happens at the caller's revision: `bonus_pool(snapshot=s)` is the fixed point for the inputs as of `s`, and evaluating a fixed point never moves `engine.revision`. A cycle that has not converged after 1000 runs raises `CycleError`; pass `max_iterations=` to change the limit. Enter a cycle through the query that has `fixed_point=`, and from one thread at a time (see G7 in `docs/guarantees.md`).

### Error Caching
By default, queries intercept and cache exceptions (excluding control flow exceptions like `QueryCancelled`). The caller always gets the exception the query raised: one whose `args` cannot be encoded is raised as it is and not cached.

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
*   **`engine.prune(roots, vacuum_disk=False)`**: Removes cached query results from the in-memory LRU cache that are not reachable from the specified roots. Set `vacuum_disk=True` to also delete unreachable entries from the persistent disk cache (`docs/disk_cache_vacuuming.md` describes the limits, written for the MDBX backend).
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
*   Values must be serializable (primitives, bytes, lists, dicts, dataclasses, etc.); this is required for every engine, not only with `cache_dir`. A query whose arguments cannot be serialized is computed but not persisted.
*   **`engine.clear_disk_cache()`**: Deletes every cached query result in the persistent disk cache. Named collections are kept: their contents are data, not cache, and are restored as usual on the next run. To empty a collection, call its `clear()`.
*   **Upgrades:** when a new version of Cascade changes how results are stored, the cached results in an existing `cache_dir` are dropped on open and recomputed (this version does: dicts are now stored in insertion order instead of sorted). Named collections are kept across such a change. If a directory holds collection data the running version cannot read, `Engine(cache_dir=...)` raises `PersistentCacheError` and leaves the directory untouched.
*   **Reusing results across runs:** results are queued when their query finishes and written to the cache in batches by a background thread. They are on disk once `engine.flush_disk()` or `engine.shutdown()` returns; if a write failed (another process holds the database locked, the map is full), the call raises `PersistentCacheError` with the original error as its cause instead, the results stay correct in memory, and the writer carries on with the next ones. A normal interpreter exit (end of script, `sys.exit()`, an unhandled exception) writes whatever is still queued. A process that is killed (`os._exit`, SIGKILL, a crash) loses the results still in the queue, so call `flush_disk()` at the points where that matters. A new `Engine` with the same `cache_dir` reuses them once you set the same input values again, and recomputes only the queries whose inputs changed.

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

`CascadeList`, `CascadeSet`, and `CascadeDict` behave like their builtin counterparts, but every mutation appends a diff to an event log. Queries written with ordinary single-`for` comprehensions and reducers are rewritten at registration time into incremental pipelines that consume only new diffs. Appending one element to a list of a million reruns your mapping function once.

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

Rewriting covers comprehensions with a single `for` clause (`[...]`, `{...}`, `{k: v ...}`), `map`, `filter`, `reversed`, and reducers: `sum`, `len`, `min`, `max`, `any`, `all`, `sorted`, `list`, `set`, `dict`, and string-literal `.join`.

A rewritten expression returns exactly what the same Python returns: the same value, with the same type, the same choice between equal elements, and the same exception when Python raises one. Where an incremental update cannot guarantee that, the reducer does more work instead of returning something else.

| Reducer | Ingest per diff | Finalize |
|---------|-----------------|----------|
| `sum` of ints, `len`, `any`, `all` | O(1) | O(1) |
| `sum` with floats (or anything but ints) | O(1) | O(N): the builtin `sum` over the stored values |
| `min`, `max` | O(log N) | O(1) |
| `sorted` | O(log N) | O(output) |
| `list`, `set`, `dict`, `.join` | O(1) | O(output) |
| `reversed` | O(1) positional flip | inherited |

*   A float `sum` is re-added from the stored values on every run, because float addition rounds and only the builtin's own pass gives the builtin's result. Your mapping function still runs once per changed element.
*   `min`, `max` and `sorted` pick between equal elements by position, as the builtins do (`sorted` is stable, also with `reverse=True`). Keys that Python cannot order totally (NaN, mixed types, sets, arbitrary objects) are handed to the builtin on every run.
*   `any` and `all` evaluate every element, where Python stops at the first that decides the result. If an element raises, Cascade evaluates the expression the plain way instead, so `any(1 / z for z in zs)` over `[1, 0]` is `True` as in Python. The same goes for any exception inside a rewritten expression: the plain evaluation decides what is returned or raised, and it is what runs on each call while the offending element is in the collection.
*   `len(...)` is rewritten for a list comprehension or a collection. `len(map(...))` and `len(filter(...))` raise `TypeError`, as in Python.

*   Arbitrary `for` loops and comprehensions with more than one `for` are never rewritten, but invalidate through read tracking.
*   A rewritten comprehension or reducer visits a `CascadeSet` in insertion order. A `for` loop, a helper function and `list(s)` see the builtin set's order instead; until that is settled (D29 in `docs/guarantees.md`), do not rely on the order of a `CascadeSet` outside rewritten expressions.
*   Two more open gaps in rewriting: an assignment expression (`:=`) inside a rewritten comprehension does not reach the enclosing function (D30), and `Accumulator.push` calls made per element are not replayed for elements that did not change (D31). Use `@engine.query(incremental=False)` for such a query.
*   `enumerate()` and `zip()` sources are explicitly excluded.
*   Reads that are not rewritten are still tracked: indexing, slicing, `in`, `len`, comparisons, `xs + ys`, `s | t` and the other set operators and methods, `d.keys() & other`, `repr`, `list(xs)`, `dict(d)`, `json.dumps(xs)` and so on. Each returns a plain builtin and re-runs the query when the collection changes.
*   **One limit:** CPython reads a `set` subclass's storage directly when a `CascadeSet` is handed to another set's code, and Cascade cannot see that. Inside a query, `frozenset(s)`, `{*s}`, `plain.union(s)`, `plain |= s`, `frozenset_a | s`, and `set(s)` in a helper function (in the query body itself `set(s)` is rewritten and fine) do not record the dependency. Write `s.copy()`, or put the collection on the left: `s.union(plain)`, `s | frozenset_a`.
*   `copy.copy(xs)` and `xs.copy()` give plain builtins; collections cannot be pickled.
*   A comprehension whose element expression, `if` clause or `key=` function itself reads an input, a query or another collection is recomputed in full each time its query re-runs. Read the value once before the comprehension (`f = factor()`) to keep it incremental.
*   Opt out globally via `Engine(incremental=False)` or per-query `@engine.query(incremental=False)`.

### Batching Mutations for Disk Performance

By design, Cascade collections use an immediate write-ahead pattern for crash-safety. Every time you mutate a collection (`__setitem__`, `append()`, `add()`), Cascade immediately opens a disk transaction and commits the diff to the collection's event log on disk. The commit is not followed by an OS sync (`fsync`) on either backend, so a mutation survives a process crash but not necessarily a power loss.

If you iterate through a loop inserting thousands of records one by one, you will trigger thousands of separate disk transactions, resulting in slow performance (e.g., waiting on disk I/O).

To avoid this, always align disk transactions with your **logical units of work** by batching bulk-inserts using `.update()` or `extend()`. This allows Cascade to group all changes into a single disk transaction:

```python
# SLOW (10,000 separate disk transactions)
for i in range(10000):
    docs[f"doc_{i}"] = i

# FAST (1 single disk transaction)
docs.update({f"doc_{i}": i for i in range(10000)})
```

Inside `engine.transaction()`, named collections hold their disk writes until the block finishes and then write them in one batch per collection; if the block raises, nothing reaches disk. A value that cannot be persisted still raises at the mutating call.

*(Note: `.update()` behaves exactly like a standard Python dictionary. If a key already exists, it will seamlessly overwrite the old value by batching an `"upsert"` event to the underlying log, ensuring downstream queries receive the correct diffs.)*

A mutating call that raises changes nothing. In particular `CascadeSet.update()` and `difference_update()` are all-or-nothing: `s.update([1, [2]])` raises `TypeError` and adds neither element, where a builtin `set` would keep the `1`.

### Persistence & Inspection

A **named** collection on an engine with `cache_dir` event-sources its log to disk automatically.

```python
engine = Engine(cache_dir="./cache")
items = CascadeList(engine, name="items", compact_every=1024)
```

One engine at a time may change a named collection. The first change takes a lock on it in the cache directory, held until `engine.shutdown()` or the end of the process (a crashed process leaves no lock behind). Another engine, in the same process or another one, can open and read the collection and shares every cached query result, but changing the collection there raises `PersistentCacheError` and changes nothing. So does changing a copy that is older than what another engine has written since; open it again from a new engine.

*   **`engine.inspect_pipelines()`**: Lists every observed pipeline with its source, logical stages, fused stage count, checkpoint revision, and consuming queries.

---

## 8. Limitations & Development

### Limitations

1. **Cycle Detection:** Cascade detects and rejects recursive function calls (cycles) with a `CycleError`, unless the query reached again has `fixed_point=`. A fixed-point cycle must be entered through that query, and stops with a `CycleError` if it has not converged after `max_iterations` runs (default 1000).
2. **Thread Safety:** Calling queries, reading inputs, `.set()`, transactions and registering queries or inputs are safe from any thread. Several engines and processes may share a `cache_dir` for cached results, but only one at a time may change a given named collection (see Persistence & Inspection above); with sqlite a busy database can make a write fail, which `flush_disk()` reports. A transaction belongs to the task or thread that opened it; mutating a collection from elsewhere while a transaction that changed it is open raises `RuntimeError` (see G25 in `docs/guarantees.md`).
3. **Persistence Security:** The persistent disk cache resolves `@dataclass`, `NamedTuple` and exception types via `importlib`. Only open cache directories from trusted sources.
4. **Python Version:** Optimization for parallel CPU-bound work requires **CPython 3.14+ free-threaded** builds with `PYTHON_GIL=0`.

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
| `disk_cache_and_inspection.py` | Reusing results across engine sessions via `cache_dir`, plus graph summaries and pruning |
| `gil_parallel_speedup.py` | Threaded CPU benchmark: GIL vs free-threaded |

### Development & Tests

```bash
export PYTHON_GIL=0
python3.14t -m pip install -e ".[dev]"
python3.14t -m pytest -q --cov=src/cascade --cov-branch --cov-report=term-missing
```

*   **Stateful fuzz**: `PYTHON_GIL=0 python3.14t -m pytest -q tests/test_stateful_engine_invariants.py`
*   **Guarantees**: `docs/guarantees.md` lists what the engine promises, the tests behind each promise, and known defects.
*   **Formal model**: TLA+ specs live under `docs/formal/`.
*   **Performance suite**: `python -m benchmarks.performance_suite --report-dir artifacts/performance --assert-thresholds`
