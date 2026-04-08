# Cascade Query

`cascade-query` is a minimal, demand-driven incremental computation framework for Python.

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

### What this framework intentionally does not include

To keep API surface minimal, this version does not include:

- nominal interning APIs (`@interned`) and tracked structs (`@tracked`)
- fixed-point cycle solvers
- distributed/shared cache protocols

Those can be layered on top without changing the core query model.

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
  Tiny compiler pipeline (`source -> parse -> symbol_names -> typecheck`) with warnings accumulator.
- `examples/dynamic_macro_expansion.py`  
  Runtime macro-expansion query that dynamically changes downstream graph dependencies.

## Persistence and inspection

```python
engine.save("state.db")
engine.load("state.db")
print(engine.inspect_graph())
for event in engine.traces():
    print(event.event, event.key, event.detail)
```

## Analysis review of the uploaded notes

Your friend’s notes are largely directionally correct and align with state-of-the-art incremental systems:

- Correct: pull-based demand, red-green early bailout, dependency graph capture, dedup, MVCC snapshots, cancellation, side-effect replay, tracing, and persistence.
- Needs qualification in Python: true CPU-bound parallelism is constrained by the GIL unless query bodies release it (I/O/native extensions).
- Overreach for this minimal implementation: unsafe-pointer lifetime tricks, red/green syntax tree internals, and fixed-point cycle solving are advanced optimizations that are not required for a practical minimal API.

## Running tests

```bash
python -m pip install -e . pytest
pytest -q
```

## CI best practices included

- GitHub Actions workflow at `.github/workflows/ci.yml`.
- Matrix test runs on Python `3.10`, `3.11`, and `3.12`.
- Runs on both pushes and pull requests.
- Linting with `ruff` before tests.
- Separate package-build job (`python -m build`) to catch packaging regressions early.
