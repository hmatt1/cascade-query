# Internal architecture (Engine phase 2)

This note documents the internal module boundaries behind `Engine` and the
invariants each layer owns. Public API remains `cascade.Engine` and related
handles/types.

## Module responsibilities

- `engine.py` (facade/composition)
  - Public API surface (`Engine`, input/query/accumulator handles).
  - Delegates state and evaluation logic to internal modules.
  - Keeps compatibility shims for existing private test introspection.

- `_state.py` (data model)
  - Core immutable/mutable record types:
    - `Snapshot`, `InputVersion`, `MemoEntry`, `Dependency`, `TraceEvent`.
  - Shared key aliases (`QueryKey`, `InputKey`) used across modules.

- `_errors.py` (error taxonomy)
  - Internal/public exceptions (`CycleError`, `CancellationError`,
    `QueryCancelled`).

- `_store.py` (state ownership + graph persistence in memory)
  - Owns mutable engine state under `RLock`.
  - Provides stable hash, input version timelines, memo/dependent indexes,
    in-flight dedup registry, trace ring buffer.
  - Owns query registration/lookup to keep query function identity centralized.

- `_runtime.py` (execution context model)
  - Thread/context-local runtime records (`RuntimeState`, `RuntimeFrame`) used
    during query execution.

- `_evaluator.py` (evaluation + incremental semantics)
  - Query/input evaluation, dependency capture, red/green verification,
    recomputation/backdating, dedup wait/owner behavior, effect replay, and
    cancellation checks.
  - Uses `_store` as the single mutable state boundary.

- `_scheduler.py` (batch execution primitive)
  - Work-stealing task scheduler used by `Engine.compute_many`.

- `_persistence.py` (serialization boundary)
  - Save/load payload blobs to SQLite (`cascade_state` table).

## Key invariants

- All mutable graph state (`inputs`, `memos`, `dependents`, `in_flight`,
  revision counters) is owned by `GraphStore`.
- Query dedup identity is `(query key, snapshot revision)` so stale and live
  snapshots do not share in-flight owners.
- Snapshot reads are read-only unless reading current head revision with an
  unset input default, in which case a single materialized version may be
  created without bumping cancel epoch.
- Dependency verification (`dependency_changed_at`) runs in a shadow runtime so
  it does not mutate dependency edges of the currently executing frame.
- Accumulator replay mirrors push propagation to all active frames so cached
  child queries preserve transitive effects for parents.
- `dependents` must be consistent with `memo.deps` for every cached memo.
  (This is intentionally covered by a single internal-invariant test helper.)
