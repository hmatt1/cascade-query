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

### The rules your code must follow

Cascade returns correct results only if queries follow these rules. Breaking one usually gives **a plausible wrong answer with no error**, so read them before you write your first query.

1. **A query reads only its arguments, inputs, other queries and Cascade collections.** Anything else is invisible to Cascade: a file, an environment variable, the clock, randomness, a global, an attribute of an object. If it changes, the old result is still served.

    ```python
    CONFIG = {"factor": 1}

    @engine.query
    def scaled():
        return n() * CONFIG["factor"]   # wrong: CONFIG is not tracked

    scaled()                # 2
    CONFIG["factor"] = 10
    scaled()                # still 2; a fresh run gives 20
    ```

    Put outside data in an `@engine.input` and change it with `.set()`. For data you cannot push in (a web request), give the query a `ttl=`.

2. **Never change a value you got from a query or an input.** You get the cached object itself, not a copy. This applies to callers and to other queries alike.

    ```python
    rows = items()
    rows.append(99)         # wrong: items() now returns the extra row too
    rows = [*items(), 99]   # right: build a new list
    ```

    The same goes for input values: `cfg()["k"] = 1` changes nothing Cascade can see. Call `cfg.set(new_value)`.

    Cascade can check for this, in tests and during development. `engine.check_mutations()` hashes every cached result and input value again and raises `MutatedValueError` naming any that changed in place; a changed result is dropped and computed again on its next use. `Engine(check_mutations=True)` also makes each query check the values it read when its body finishes. Both are off in normal runs, because they cost one digest of each value checked (about 0.75 ms for a list of 10,000 ints).

3. **Return values, not iterators.** A generator, `map` or `zip` can be read once; the second caller gets it empty. Return `list(...)` or a tuple. Cascade raises `UnencodableValueError` when a query returns one (a `memoize=False` query may, since it keeps no value).

4. **Do not rely on side effects.** On a cache hit the body does not run, so a `print`, a log line or a write does not happen again. Use an `Accumulator` for effects that must be replayed ([Side-Effect Accumulators](#side-effect-accumulators)).

5. **With `cache_dir`, bump `version=` when a library or a method changes what a query computes.** Cascade notices an edit to the query's body, to the helper functions and classes of your program it names, and to the constants, closure values and defaults it uses. It does not notice an upgraded library, or a method reached through an object (`obj.method()`), and keeps serving the stored result across runs ([What the code hash sees](#what-the-code-hash-sees-and-version)).

    ```python
    @engine.query(version=2)    # bumped: the tokenizer library was upgraded
    def tokens(path: str) -> list[str]:
        return tokenizer.split(read_file(path))
    ```

6. **Change and read Cascade collections through their own methods.** `frozenset(s)`, `{*s}` and `plain | s` on a `CascadeSet` are not tracked; write `s.copy()` or `s | plain`. `heapq.heapify(xs)` and `list.sort(xs)` on a `CascadeList` are not seen; write `xs.sort()` ([What gets rewritten](#what-gets-rewritten)).

7. **Arguments are values, `self` included.** Pass a frozen dataclass, a `NamedTuple` or plain data. An ordinary object (no `__eq__` and `__hash__` of its own) is refused with `TypeError`, because changing one of its attributes would not invalidate results computed from it. A class that defines its own `__eq__` and `__hash__` is accepted: keep the state they cover immutable.

If a result looks stale, compare it with a run on an engine that has no cache: a new `Engine()` without `cache_dir`, or after `engine.clear_disk_cache()`.

### How well each part is tested

Not every feature is tested to the same depth. `docs/guarantees.md` has the details, the limits of each guarantee and the list of known test gaps.

| Area | Test strength | What that means for you |
|------|---------------|-------------------------|
| Synchronous queries, inputs, snapshots, eviction | Strong: a formal model, a randomized state machine and differential tests | The best-tested path |
| Disk cache, one process | Strong on the sync path, good for async | Rule 5 is the main risk |
| Collections and rewritten comprehensions | Strong for reducers; generated whole-query tests use small numbers only | Two wrong-result bugs were fixed in November 2026 (D36, D37). If such a query gives a result you do not expect, `incremental_per_element=False` on it runs the same code as plain Python; please report any difference |
| `async def` queries | Medium: fewer assertions than the sync path | Test your own async graphs |
| Threads on free-threaded Python | Stress-tested, and a randomized test that picks thread interleavings itself for some scenarios; not exhaustive | One race took about ten CI runs to show; that test now finds it on every build |
| Several processes writing one `cache_dir` | Tested around named collections only, not under load | Give parallel jobs their own directories |

---

## 2. Core Concepts

### Engine Initialization & Configuration

The `Engine` manages caching, execution, and state tracking.

*   **`Engine(*, max_entries=10000, trace_limit=50000, stats=False, cache_dir=None, cache_map_size=2**30, cache_backend="mdbx", incremental_per_element=True, cache_version=None, check_mutations=False)`**: Initializes the engine. 
    *   `max_entries` sets the limit for the Least Recently Used (LRU) in-memory cache. 
    *   Passing `cache_dir` enables persistent disk caching.
    *   `cache_backend` allows you to select the storage engine for persistence (`"mdbx"` or `"sqlite"`).
    *   `incremental_per_element` decides how much work a query over a Cascade collection does when the collection changes: only the changed elements (the default), or every element as plain Python. The result is the same; see [What gets rewritten](#what-gets-rewritten).
    *   `check_mutations=True` makes each query check, when its body finishes, that the values it read were not changed in place (rule 2 above). For tests and development: it costs one digest of each value read, per run.
    *   `cache_version` (a `str` or `int`) salts every result this engine stores; results stored under another salt are not used. Change it to throw away the whole result cache, for example after upgrading a library your queries use.

### Defining Nodes: `@engine.input` and `@engine.query`

*   **`@engine.input`**: Decorator for mutable data roots.
    *   `input.set(value)`: Updates the value and increments the global revision. Setting the value the input already holds is a no-op: the revision does not change and nothing is invalidated or cancelled.
    *   `input.set(*args, value=value)`: Updates a keyed input.
*   **`@engine.query`**: Decorator for cached computations. Re-evaluates only if upstream data changes.

A few rules apply to every engine, with or without `cache_dir`:

*   **Values are fingerprinted.** Results and input values are encoded to tell whether they changed (and, with `cache_dir`, to store them). The encoder knows the standard value types: primitives, `bytes`/`bytearray`, `complex`, builtin containers, `deque`, `range`, `Decimal`, `Fraction`, `UUID`, `Enum` members, `pathlib` paths, the `datetime` types, dataclasses, `NamedTuple`s, exceptions, and numpy arrays and scalars if your program uses numpy. Each comes back from disk as the same type. Other classes can be taught with `register_type` (below). A result the encoder does not know is fine on an engine without `cache_dir`: it is cached as it is, but never counts as equal to an earlier result, so its dependents always re-run when it is recomputed. With `cache_dir`, and for input values and `fixed_point=` values on any engine, it raises `UnencodableValueError`, a `TypeError` whose message names the query or input, the path to the offending part of the value (`result[2]['k'].field`) and its type. Such a result is not cached.
*   **Your own types: `register_type`.** Give Cascade a pair of functions for a class, and its instances can be results, input values, arguments and collection items, also on disk:

    ```python
    from cascade import register_type

    class Money:
        def __init__(self, amount: Decimal, currency: str): ...

    register_type(
        Money,
        encode=lambda m: (m.amount, m.currency),   # anything Cascade can encode
        decode=lambda fields: Money(*fields),
        name="billing.Money/1",                   # optional; the identity on disk
    )
    ```

    Two values whose `encode` results are equal are treated as the same value (no re-run of dependents, a no-op `set`), so `encode` must capture everything a query can observe. Bump `name` when the encoding changes. The registry is process-wide (`Engine.register_type` is the same function): register next to the class, before an engine reads its values. Cascade never pickles; a cache directory can only produce values through the registered `decode`.
*   **Results are returned by reference.** The caller gets the cached object itself; mutating it changes the cache (rule 2 above). A query that returns a generator or another one-shot iterator raises `UnencodableValueError`, since every later caller would find it used up.
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
*   **Methods can be queries.** Decorate a method in the class body; calling it on an instance passes the instance as `self`, which becomes part of the cache key like any argument:

    ```python
    @dataclass(frozen=True)
    class Document:
        path: str

        @engine.query
        def word_count(self) -> int:
            return len(read_file(self.path).split())   # read_file: an input

    Document("a.txt").word_count()   # equal documents share one result
    ```

    `self` must be hashable; two instances share results when they are equal and of the same class. A frozen dataclass, a `NamedTuple` or a type registered with `register_type` is also stored on disk; an ordinary object, which Python hashes by identity, is refused with `TypeError`, as `self` and as any other argument: changing one of its attributes would not invalidate the results computed from it. Put `@staticmethod` or `@classmethod` above `@engine.query`.
*   **Arguments are hashable, and keyed by type.** Calls are bound to the function's parameters, so keyword arguments work and `q(1)`, `q(x=1)` and `q(1, y=2)` (when `y=2` is the default) share one cached result. Arguments are compared by value and type, so `q(1)`, `q(True)` and `q(1.0)` each get their own, as do `q((1, 2))` and `q((1, 2.0))`. A dict or list cannot be an argument; pass a tuple or a frozen dataclass. `snapshot=` and `effects=` are the handle's own keywords: pass a parameter with one of those names positionally.

### Snapshots & Transactions (Input Debouncing)

*   **`engine.snapshot()`**: Returns a `Snapshot` object pinning the current global revision. Use `query(snapshot=s)` to read data as it existed at that revision.
*   **`engine.transaction()`**: When updating multiple inputs, intermediate read states ("flapping") can cause inconsistent evaluations. Use a transaction block to batch updates so they are committed atomically. Only the last value set for each input counts, inputs that end at their current value are skipped, and a transaction that changes nothing leaves the revision alone. Collection changes made inside the block show up on the collection objects right away, and are undone if the block raises. The commit checks every input value before it applies anything: a value that cannot be encoded fails the whole commit with `UnencodableValueError`, and the collection changes are undone as if the block had raised. Queries run inside the block still see the engine as it was before the block, so a collection created in the block reads as empty to them until it commits. A transaction object is single-use: call `engine.transaction()` for each block; entering the same object a second time raises `RuntimeError`. A transaction belongs to the asyncio task that entered it (or the thread, when entered outside any task): threads and tasks started inside the block are not part of it, so a `set()` they make applies at once, and a change they make to a collection the transaction has already changed raises `RuntimeError` until the transaction ends.

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
*   **`engine.submit(query, *args, executor=None, **kwargs)`**: Schedules a query for background execution. Returns a `concurrent.futures.Future`. Calling it from inside a running query raises `RuntimeError`, because the future could outlive the query and its read could not be tracked; call the query directly or use `compute_many` there.
*   **`engine.compute_many(calls, workers=None)`**: Executes a list of `(query, args)` or `(query, args, kwargs)` calls in parallel using a thread pool. Blocks until all are complete and returns a list of results. Inside a running query, each requested query becomes a dependency of the calling query, just as if it had been called directly (`memoize=False` queries then run on the calling thread).
*   **`engine.compute_many_stream(calls, workers=None)`**: Yields completed results as each call finishes. Yields `(index, value, call_effects)` in completion order. Tracks dependencies inside a query the same way `compute_many` does.
*   **`engine.shutdown(*, wait=True, cancel_futures=False)`**: Shuts down the default thread pool executor used by `submit`. With `cache_dir`, it also writes any results still queued for the disk cache and closes it; like `flush_disk()`, it raises `PersistentCacheError` if a result could not be written and that was not reported yet.
*   **`with Engine(...) as engine:`** calls `shutdown()` when the block ends. If the block raised, its exception propagates and a write failure is reported as a `RuntimeWarning` instead. An engine you simply drop is garbage-collected too, `cache_dir` or not: its disk writer finishes the queued writes, closes the cache and releases its collection locks.
*   **`QueryCancelled`**: Exception raised if a background query's dependencies change before it completes. Only queries started with `submit` are cancelled; direct calls, `compute_many` and `compute_many_stream` always run to completion. A direct call that joins a submitted run of the same query is not cancelled with it: it runs the query itself at its own revision.

---

## 4. Cache Management & Invalidations

### Targeted Updates & Early Bail-out
If a function's logic evaluates to a result identical to its previous run, early bail-out prevents re-computation of downstream queries.

"Identical" is stricter than `==`: `1`, `1.0` and `True` are different results, and so are two dicts with the same items in a different insertion order, because a query that iterates the dict can tell them apart. Sets have no order and compare by their elements. The same rule decides whether `input.set(value)` is a no-op. If a query builds a dict whose order carries no meaning (for example from iterating a set), return `dict(sorted(d.items()))` so that equal contents stop propagation and are reused across runs.

### Code-Aware Auto-Invalidation
Cascade automatically inspects the Python bytecode of your `@engine.query` and `@engine.input` functions. If you edit a function's logic and the module is hot-reloaded (or you restart your script), Cascade compares the new function's bytecode hash against the previously cached logic when the function is registered again. If the logic has changed, Cascade invalidates that function's memory and persistent disk caches at that point, and results that other queries computed from the old function are re-run the next time they are used, whether or not an input changed in between. Handles to the old function become stale: calling one raises `StaleHandleError` rather than running the old code, so modules that imported the handle need to import it again after the reload.

#### What the code hash sees, and `version=`

The hash a stored result is kept under covers:

*   the query function's own code: its bytecode, the constants and names in it, its local variable names, and functions and comprehensions written inside it;
*   the functions and classes of your program it names, and what those name in turn: a helper, a helper of that helper, a class's methods and those of its base classes, a function reached as `module.name` in a module of your program;
*   the immutable values it names: module constants such as `LIMIT = 10` or `UNITS = ("m", "cm")`, frozen dataclasses, enum members;
*   its closure values and default arguments, when they are immutable values or functions.

So editing a helper or a constant between runs recomputes the stored results that used it, with nothing to declare. The hash does **not** cover:

*   the standard library and installed packages: upgrading one does not change the hash;
*   a method called on an object (`obj.method()`), since the class is only known when the query runs;
*   mutable objects a query reads (a module-level `dict` or `list`, an object's attributes), and names the query assigns itself (a `global` or `nonlocal` counter);
*   files, environment variables, or anything else read without an `@engine.input`.

For those, declare the change with `version=` on the query:

```python
@engine.query(version=2)          # bumped: the tokenizer library was upgraded
def tokens(path: str) -> list[str]:
    return tokenizer.split(read_file(path))
```

Any new value works (`str` or `int`). Results computed under another version are not used: the query runs again, and queries that depend on it run again if its result changed. Queries that only call `tokens()` need no bump. To discard every stored result at once, change `Engine(cache_version=...)`, or call `engine.clear_disk_cache()`. Old results are replaced as their queries are recomputed; `engine.prune(roots, vacuum_disk=True)` removes the ones nothing reaches.

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

The iteration happens at the caller's revision: `bonus_pool(snapshot=s)` is the fixed point for the inputs as of `s`, and evaluating a fixed point never moves `engine.revision`. A cycle that has not converged after 1000 runs raises `CycleError`; pass `max_iterations=` to change the limit. Enter a cycle through the query that has `fixed_point=`. Several threads or tasks may enter one cycle at once, also through different queries: the one that entered through the head gets the fixed point, and one that entered through another member gets it too or a `CycleError`, as it would on one thread (see G7 in `docs/guarantees.md`).

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
Queries must be pure functions (see [the rules](#the-rules-your-code-must-follow)). Use `Accumulator` to record side-effects (like logs or warnings) that must be replayed when a result is served from the cache.

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

A `fixed_point=` query re-runs until it converges; only the run that converged counts, so its effects (and those of the queries in its cycle, from that run) are what callers see, once.

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
*   Results and input values must be encodable (see "Values are fingerprinted" above). A query whose arguments cannot be encoded is computed but not persisted.
*   **`engine.clear_disk_cache()`**: Deletes every cached query result in the persistent disk cache. Named collections are kept: their contents are data, not cache, and are restored as usual on the next run. To empty a collection, call its `clear()`.
*   **Upgrades:** when a new version of Cascade changes how results are stored, the cached results in an existing `cache_dir` are dropped on open and recomputed (this version does: it adds value types, stores a `bytearray` as itself rather than as `bytes`, and keys arguments by type). Named collections are kept across such a change. If a directory holds collection data the running version cannot read, `Engine(cache_dir=...)` raises `PersistentCacheError` and leaves the directory untouched.
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
*   A `CascadeSet` iterates in insertion order (like a dict's keys), everywhere: rewritten comprehensions and reducers, `for` loops, helper functions, `list(s)`, `repr`, snapshots and restarts. A plain `set` built from it (`set(s)`, `s.copy()`, `s | t`) iterates in the builtin's own order.
*   A comprehension with an assignment expression (`:=`) in its element or `if` clauses is left as written, so the name is bound in the query as in Python. So is a comprehension or reducer nested inside a comprehension that is left as written (for example one with two `for` clauses) when it uses that comprehension's loop variable.
*   If an element, filter or `key=` function pushes to an `Accumulator` (or calls a query that does), the expression is evaluated as plain Python from the first run on which that happens, so the effects are exactly Python's, in Python's order; such an expression is no longer incremental.
*   `enumerate()` and `zip()` sources are explicitly excluded.
*   Reads that are not rewritten are still tracked: indexing, slicing, `in`, `len`, comparisons, `xs + ys`, `s | t` and the other set operators and methods, `d.keys() & other`, `repr`, `list(xs)`, `dict(d)`, `json.dumps(xs)` and so on. Each returns a plain builtin and re-runs the query when the collection changes.
*   **One limit:** CPython reads a `set` subclass's storage directly when a `CascadeSet` is handed to another set's code, and Cascade cannot see that. Inside a query, `frozenset(s)`, `{*s}`, `plain.union(s)`, `plain |= s`, `frozenset_a | s`, and `set(s)` in a helper function (in the query body itself `set(s)` is rewritten and fine) do not record the dependency. Write `s.copy()`, or put the collection on the left: `s.union(plain)`, `s | frozenset_a`.
*   `copy.copy(xs)` and `xs.copy()` give plain builtins; collections cannot be pickled.
*   C code that writes a collection's storage directly, such as `heapq.heappush(xs, v)`, `heapq.heappop(xs)` or `list.append(xs, v)` called by hand, cannot be intercepted. The next use of the collection notices that its size changed without a recorded change, undoes it and raises `CollectionBypassedError`. Changes that keep the size (`heapq.heapify`, `heapq.heapreplace`, `list.sort(xs)`) are not noticed and leave queries on the old contents: use the collection's own methods (`xs.sort()`), or a plain copy for heap work. `bisect.insort` goes through `insert` and is fine.
*   A comprehension whose element expression, `if` clause or `key=` function itself reads an input, a query or another collection is recomputed in full each time its query re-runs. Read the value once before the comprehension (`f = factor()`) to keep it incremental.
*   The per-element results are kept for the values the element, `if` or `key=` function captures (closure variables and defaults) as they were when the query last ran. If one of those has other contents when the query runs again, every element is recomputed. The common way to trip over this is a log or counter in a helper that keeps growing (`seen.append(x)`): every run then recomputes everything, and the growing list is hashed each time. The result stays correct; only the saving is lost. Keep such counters where they are not captured, for example as an attribute of the function (`f.calls += 1`) or a module-level variable.
*   **Turning it off: `incremental_per_element=False`.** Every query is incremental: it re-runs only when something it read changes. This option is about what such a re-run costs for a query over a collection. In the example above, after `docs.append("delta")` the default runs `len(d) > 4` once, for the new element; with `@engine.query(incremental_per_element=False)` the comprehension runs as plain Python over all four. Both return the same value, and both are cached and invalidated the same way. Turn it off for one query when its result surprises you (to tell your code from Cascade's rewriting), or for every query with `Engine(incremental_per_element=False)`.

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

One engine at a time may change a named collection. The first change takes a lock on it in the cache directory, held until `engine.shutdown()` (or the end of a `with Engine(...)` block), until the engine is garbage-collected, or until the end of the process (a crashed process leaves no lock behind). Another engine, in the same process or another one, can open and read the collection and shares every cached query result, but changing the collection there raises `PersistentCacheError` and changes nothing. So does changing a copy that is older than what another engine has written since; open it again from a new engine.

*   **`engine.inspect_pipelines()`**: Lists every observed pipeline with its source, logical stages, fused stage count, checkpoint revision, and consuming queries.

---

## 8. Limitations & Development

### Limitations

1. **Cycle Detection:** Cascade detects and rejects recursive function calls (cycles) with a `CycleError`, unless the query reached again has `fixed_point=`. A fixed-point cycle must first be entered through that query; once it has a result (in memory or in `cache_dir`), any member of the cycle can be called directly, also after the member's own result was evicted or pruned. The cycle stops with a `CycleError` if it has not converged after `max_iterations` runs (default 1000).
2. **Thread Safety:** Calling queries, reading inputs, `.set()`, transactions and registering queries or inputs are safe from any thread. Several engines and processes may share a `cache_dir` for cached results, but only one at a time may change a given named collection (see Persistence & Inspection above); with sqlite a busy database can make a write fail, which `flush_disk()` reports. A transaction belongs to the task or thread that opened it; mutating a collection from elsewhere while a transaction that changed it is open raises `RuntimeError` (see G25 in `docs/guarantees.md`).
3. **Persistence Security:** The persistent disk cache resolves `@dataclass`, `NamedTuple`, `Enum` and exception types via `importlib`, and calls the `decode` function of registered types. It never unpickles. Only open cache directories from trusted sources.
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
*   **Formal model**: a TLA+ model of lazy re-verification with concurrent callers lives under `docs/formal/`. CI checks its quick configuration on every push; the exhaustive one (26 million states) is run locally or by starting `.github/workflows/model-check.yml` by hand.
*   **Mutation testing**: `python scripts/mutation.py run`, then `collect`, then `summary` print the share of mutants the tests do not notice, per module. A full run is hours of test runs, so it is done on demand: on a machine with many cores, or by starting `.github/workflows/mutation.yml` by hand. mutmut needs `fork`; on Windows `run` uses `scripts/mutation_windows.py` instead, which checks the same mutants against the same tests with one process per mutant. `python scripts/mutation.py run "cascade._state.*"` does one module. `docs/verification-status.md` has the results so far (20.2% of 11,282 mutants survive) and what is still open.
*   **Performance suite**: `python -m benchmarks.performance_suite --report-dir artifacts/performance --assert-thresholds`
