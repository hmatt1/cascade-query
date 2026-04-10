# Internal architecture (Engine phase 2)

This note documents the internal module boundaries behind `Engine` and the
invariants each layer owns. Public API remains `cascade.Engine` and related
handles/types.

## Module responsibilities

- `engine.py` (facade/composition)
  - Public API surface (`Engine`, input/query/accumulator handles).
  - Delegates state and evaluation logic to internal modules.
  - Exposes a single invariant-oriented probe (`_internals`) for tests; no other
    private `Engine` attributes are part of the supported coupling surface.

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

## Private internal API hygiene policy

`Engine` declares `_INTERNAL_TEST_API` (currently `("_internals",)`) so tests
that need graph or epoch state do so through one explicit probe object.

`engine._internals` is the ownership boundary for invariant-oriented probing:
`latest_input_version`, `input_version_at`, `dependency_changed_at`, `memos`,
`dependents`, `cancel_epoch`, `next_access_id`, `in_flight`, `inputs`, `queries`,
`lock`, `max_entries`.

Policy:

1. New tests should default to public API behavior. Prefer public diagnostics
   (`revision`, `inspect_graph()`, `traces()`) over widening the internal
   surface.
2. If a test needs internals, use `engine._internals` (or the centralized
   invariant helper in `tests/scale_helpers.py`) rather than new `Engine`
   attributes.
3. Keep `_INTERNAL_TEST_API` minimal (today: only `_internals`). Add entries
   only when a long-lived invariant truly cannot be asserted through the public
   API or `_internals`.

## Testing strategy split

- **Black-box behavior tests** stay in broad scenario suites
  (`tests/test_engine.py`, `tests/test_engine_concurrency.py`,
  `tests/test_engine_compute_many.py`, `tests/test_engine_persistence_and_tracing.py`,
  `tests/test_scale_behavior.py`, `tests/test_dependency_parallelism.py`) and
  exercise only public API.
- **Internal invariant tests** stay centralized in
  `tests/test_internal_invariants.py` and are intentionally minimal. This keeps
  private coupling explicit and easy to audit.

## Adding new internals safely

When adding or exposing a new internal hook:

1. Confirm the need cannot be covered by public API assertions.
2. Add the internal to `Engine._INTERNAL_TEST_API` only if it is required for a
   stable invariant assertion.
3. Add/adjust one focused invariant test in
   `tests/test_internal_invariants.py`.
4. Document the invariant ownership in this file so future refactors preserve
   the same boundary.
