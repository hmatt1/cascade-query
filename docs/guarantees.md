# Cascade guarantees

This page states what Cascade promises, where each promise stops, and which
tests enforce it. It is the reference the tests, the formal model and the
README are checked against.

The second half is an audit dated 2026-11-23 (commit `03c7ed7`): defects where
the code does not keep a promise, and guarantees with no test behind them.
Every defect has a regression test in `tests/test_known_defects.py`. All of
them have been fixed since the audit and are listed under "Fixed since the
audit"; no defect is open.

**Status key**

| Status | Meaning |
|---|---|
| Holds | No counterexample known; tests enforce it |
| Holds with limits | True inside the stated limits; the limits are by design |
| Broken | A reproduced counterexample exists; see the defect ID |

## Summary

| # | Guarantee | Status | Test strength |
|---|---|---|---|
| G1 | A query returns what a fresh run at that revision would return | Holds with limits | Strong for pure integer graphs |
| G2 | Unchanged dependencies mean the body does not run; equal results stop propagation | Holds with limits | Strong for run counts |
| G3 | Setting an input to its current value is a no-op | Holds with limits | Strong |
| G4 | A snapshot read sees inputs as of the snapshot's revision | Holds with limits | Strong |
| G5 | Concurrent requests for one query at one revision run the body once | Holds with limits | Strong |
| G6 | A submitted query whose inputs change raises `QueryCancelled` and stores nothing | Holds with limits | Good |
| G7 | Cycles raise `CycleError`; `fixed_point=` iterates to a stable value at the caller's revision | Holds with limits | Good, threads and tasks included |
| G8 | Cached errors are re-raised without re-running; control-flow errors are never cached | Holds with limits | Strong |
| G9 | A TTL query recomputes once its TTL has elapsed, also when reached through dependents | Holds with limits | Good |
| G10 | `memoize=False` queries track dependencies but keep no value | Holds | Medium |
| G11 | Re-registering a query with changed code drops its cached results and reaches its dependents | Holds with limits | Good inside a process |
| G12 | Accumulator effects are replayed on cache hits, in a fixed order | Holds with limits | Good |
| G13 | Eviction, `prune` and `sweep_unaccessed` change cost, never results | Holds | Strong for eviction and prune |
| G14 | Async queries behave like sync queries | Holds with limits | Medium |
| G15 | A new engine on the same `cache_dir` reuses results whose inputs match | Holds with limits | Strong on the sync path, good for async |
| G16 | A stale persisted result is never served | Holds with limits (G11's blind spots) | Medium |
| G17 | A finished result is on disk once the process exits normally | Holds with limits | Good for normal exits |
| G18 | Values round-trip through the disk codec with their types | Holds with limits | Strong (Hypothesis, nested values of every supported type) |
| G19 | Only values no code can tell apart encode to the same bytes; dict order is part of the value | Holds | Strong (Hypothesis, both codecs) |
| G20 | A corrupt or old-format cache entry is recomputed, not served; a format change keeps collections | Holds | Good |
| G21 | Collections behave like `list`, `set` and `dict`, and every read inside a query is tracked | Holds with limits | Strong |
| G22 | A rewritten query returns what the plain Python would | Holds | Strong for reducers (Hypothesis, against the builtins) |
| G23 | One mutation costs one diff of work, where the result can be exact | Holds with limits | Adequate |
| G24 | A snapshot read sees collection contents as of the snapshot | Holds with limits | Strong |
| G25 | A transaction applies completely or not at all | Holds with limits | Good, threads and tasks included |
| G26 | A named collection is restored with the same contents after a restart | Holds with limits | Good, two engines and processes included |

## Core evaluation

### G1. From-scratch equivalence

Calling a query returns the value its function would return if run fresh
against the inputs and collections at the caller's revision.

How: each cached result records its dependencies and a marker of the value
each had: for an input the revision that set it, for a query a stamp that is
new whenever the query's value changes. A result is reused only after every
recorded dependency is confirmed unchanged at the caller's revision. The
query stamp is not a revision, because a query's value can change while the
revision stands still (an expired TTL, G9).

Dependencies are recorded for direct calls and for `engine.compute_many` and
`engine.compute_many_stream` called inside a query: worker threads compute
the requested queries in parallel at the calling query's revision, then the
calling query reads each result with an ordinary nested call, which is what
records the dependency, replays effects and raises errors.

Limits, by design:

- **No `engine.submit` inside a query.** It raises `RuntimeError`: the future
  can outlive the query, so what the query reads through it cannot be
  recorded.
- **Fan-out inside a query is parallel only for memoized queries.**
  `memoize=False` queries keep no value to hand over and run on the calling
  thread. An explicit `snapshot=` is ignored there, as for a nested call.

- **Queries must be pure.** Anything read that is not an input, a query or a
  tracked collection read is invisible to the engine.
- **Returned values are shared.** The caller gets the cached object itself, so
  mutating a returned list changes the cache.
- **Identity is a name: `module:qualname`, or `name=`.** One name holds one
  function. Registering a different function under a name in use replaces
  it: the old function's cached results are dropped and its handles go
  stale, so calling one raises `StaleHandleError` instead of running old
  code into the new function's cache. This is what a reload with changed
  code does, and also what a second function made from the same body does
  (a factory, a loop, lambdas in one module). Functions like that need
  `engine.query(name=...)` / `engine.input(name=...)` to coexist. Registering
  the same function again, or one with equal bytecode that captures the same
  objects (a module reloaded unchanged), keeps results and handles.
- **Methods are queries whose first argument is `self`.** `@engine.query` on
  a function written in a class body gives a handle that, read from an
  instance, binds it as `self`, as a function would; read from the class
  it is the plain handle (`Cls.method(obj, x)` is the same call as
  `obj.method(x)`). `self` is part of the key like any argument: it must
  be hashable (otherwise the call raises `TypeError` saying so, before the
  body runs), two instances share results when they are equal and of one
  class, and the memo holds a reference to it. Instances with the default
  identity hash get results of their own, kept in memory only, since they
  cannot be encoded; a frozen dataclass, a `NamedTuple` or a registered
  type is stored on disk like any argument. `@staticmethod` and
  `@classmethod` go above `@engine.query` (the class is then the first
  argument); below it they are refused when the class is defined. A query
  defined outside a class and assigned to a class attribute is not bound.
- **Values are encoded to tell whether they changed.** G18 lists the
  types the codecs know; any other class can be added with
  `cascade.register_type` (G18). What happens to a value of a type neither
  knows depends on where it is:
  - a query result, or the `args` of an exception a query raises, on an
    engine **without** `cache_dir`: kept as it is, under a digest that is
    new every time. Such a result never counts as equal to an earlier one,
    so a query that returns one and runs again always re-runs its
    dependents: no early bail-out (G2), never a wrong answer. An exception
    is cached as usual (G8);
  - the same on an engine **with** `cache_dir`, where results are written
    to disk: `UnencodableValueError`, a `TypeError` naming the query (with
    its arguments), the path to the part that cannot be encoded
    (`result[2]['k'].payload`, `value key 'x'`) and its type. Such a result
    is not cached, so each call runs the body and raises again; a caller
    that catches the error depends on what the query read (G8);
  - an input value, or a `fixed_point=` guess or head result, on any
    engine: `UnencodableValueError`. A same-value `set` must be detected
    (G3), and a fixed point is reached when a result equals its guess
    (G7), so these need an encoding.
- **Arguments are hashable, bound to parameters, and keyed by type.**
  Keyword arguments are bound to the function's parameters as Python binds
  them, defaults filled in, so `q(1)`, `q(x=1)` and `q(1, y=2)` (where
  `y=2` is the default) share one result. Two arguments are one key when
  they are `==` and of the same type, recursively through tuples,
  frozensets, `NamedTuple`s and dataclass fields; a float also by the sign
  of a zero, and `Decimal`, `datetime`, `time`, `timezone`, `range` and
  paths also by their `repr`. So `q(1)`, `q(True)`, `q(1.0)` and
  `q(Decimal(1))` have a result each, as do `q((1, 2))` and `q((1, 2.0))`.
  A default that is not hashable or cannot be encoded (`[]`, a sentinel
  `object()`) is left out of the key when the call does not pass another
  value. A call that does not bind (a missing or unexpected argument)
  raises `TypeError` before the body runs, uncached. The handles keep
  `snapshot=` and `effects=` (an input's: `snapshot=`, and `value=` on
  `set`); a parameter with one of those names is passed positionally, and
  a keyword-only one is refused when the function is registered. A dict is
  not hashable and cannot be an argument. A hashable object that holds a
  dict (a frozen dataclass with the field left out of its hash) shares one
  result with an equal one, the first call's, whatever the order of their
  dicts; differences that a class's own `==` hides are not seen either.
  Results and input values are compared more strictly (G2).

Enforced by: `test_dirty_propagation.py::test_random_operations_match_plain_recomputation`
(seeded differential test against an independent model, 16 parametrisations),
`test_stateful_engine_invariants.py` (Hypothesis state machine against a
plain-Python model, in memory and on both backends; see test gap 3),
`test_dirty_propagation.py::test_concurrent_writers_and_readers_only_see_real_states`,
`test_fan_out_inside_queries.py` (dependencies, snapshots, effects, errors and
cycles through `compute_many` and `compute_many_stream` inside a query;
`submit` refused), `test_query_identity.py` (replaced functions, stale
handles, `name=`), `test_query_methods.py` (binding, keyword arguments, an unhashable `self`,
static and class methods, `compute_many`, `submit`, async, reuse from disk
on both backends), `test_argument_keys.py` (look-alike arguments in memory
and on both backends; keyword, positional-only, keyword-only, `*args` and
`**kwargs` binding; defaults that cannot be keys; binding errors; inputs,
`compute_many` and `submit`; Hypothesis: equal keys only for arguments no
body can tell apart, keys unchanged by the trip through the disk codec).

### G2. Minimal re-execution and early bail-out

If no dependency changed, the body does not run. If a body re-runs and
produces an equal result, its dependents do not re-run.

"Equal" means equal content digest, not `==`. Two values have the same
digest when a query could not tell them apart by their contents. The digest
is type-sensitive (`1`, `1.0` and `True` differ) and includes the insertion
order of dicts, at any depth: `{"a": 1, "b": 1}` and `{"b": 1, "a": 1}` are
`==` but iterate differently, so a result that changes from one to the other
re-runs its dependents. Sets have no order; two equal sets have the same
digest however they were built.

Cost of the stricter comparison: a query that returns a dict whose order
varies without meaning (keys taken from iterating a set of strings, for
example, whose order changes from one process to the next) no longer stops
propagation and is not reused across runs when the order differs. Return
`dict(sorted(d.items()))` where the order means nothing.

Enforced by: `test_dirty_propagation.py::test_backdated_middle_result_stops_recompute_but_stays_correct`,
`::test_unrelated_input_change_needs_no_walk_or_recompute`,
`test_engine.py::test_smart_recalculation_and_selective_updates`,
`test_scale_behavior.py::test_giant_graph_selective_invalidation_counts`,
`test_iterative_verification.py::test_verification_stops_at_first_changed_dependency`,
`test_known_defects.py::test_d6_dict_order_change_reaches_dependents`,
`test_dict_order.py` (both digests, nested and non-string-keyed dicts, sets
unaffected; in memory and on both backends).

### G3. Same-value `set` is a no-op

`input.set(v)` where `v` has the same digest as the current value leaves the
revision alone, invalidates nothing and cancels nothing. Inside a transaction
the same rule applies per input after last-write-wins.

Reading an input that was never set is not a change either. The first read
at the newest revision (directly, in a query, or through a snapshot of the
newest revision) keeps the default as the input's value, so the default
function is not called again and every reader at that revision sees the same
value, but the revision does not move and nothing is invalidated or
cancelled. A read at an older revision keeps nothing.

Limits: "same" is digest equality (G2), so `set(1)` then `set(1.0)` is a
change, and so is setting a dict with the same items in another order.

Enforced by: `test_history_retention.py::test_same_value_set_is_a_full_noop`,
`::test_transaction_with_only_unchanged_values_does_not_bump`,
`::test_transaction_bumps_once_and_skips_unchanged_inputs`,
`::test_transaction_that_returns_to_the_original_value_is_a_noop`,
`test_dirty_propagation.py::test_same_value_set_keeps_everything_clean`,
`test_input_default_reads.py` (first reads in and out of queries, async,
through a snapshot, from eight threads at once; reuse from disk on both
backends).

### G4. Snapshot isolation for inputs

`query(snapshot=s)` sees every input as of `s.revision`, whatever is written
afterwards. Input history is kept while any snapshot or running call needs it.
Reading through a released snapshot raises `SnapshotExpiredError` once the
history has been trimmed.

This includes `fixed_point=` queries: the cycle is iterated at the snapshot's
revision and gives the fixed point for the snapshot's inputs (G7).

Limits: a `Snapshot` built by hand from a revision number pins nothing. An old
snapshot read replaces the head's cached row, which is correct but costs a
recompute on the next head read. TTL results are not part of a snapshot (G9).

Enforced by: `test_engine.py::test_snapshot_isolation_mvcc`,
`test_history_retention.py::test_held_snapshot_keeps_history_until_released`,
`::test_hand_built_snapshot_below_the_floor_raises`,
`::test_concurrent_snapshots_and_writers_never_expire_live_reads`,
`test_dirty_propagation.py::test_pinned_snapshot_and_head_reads_interleave_correctly`,
`test_snapshot_verification_race.py::test_old_snapshot_read_ignores_rows_replaced_during_verification`,
`test_engine_concurrency.py::test_snapshot_reads_stable_during_concurrent_writes`,
`test_fixed_point.py::test_fixed_point_read_through_a_snapshot_uses_the_snapshot_inputs`,
`::test_threads_reading_a_fixed_point_at_different_revisions`.

### G5. Concurrent demand runs the body once

When several threads request the same query with equal arguments at the same
revision, one runs the body and the others wait for its result, effects and
exception.

The body can run more than once when callers are at different revisions, when
the query is `memoize=False`, when the first run raised an error that is not
cached, or when the cached row was evicted in between.

A caller that stops waiting for a run (a cancelled task, a timeout around
the call) leaves the run, its owner and its other waiters alone.

Enforced by: `test_engine_concurrency.py::test_query_dedup_concurrent_requests`,
`::test_query_dedup_heavy_contention_stress`,
`::test_query_dedup_failure_propagates_and_recovers`,
`::test_in_flight_dedup_does_not_cross_snapshot_boundaries`,
`test_dependency_parallelism.py::test_compute_many_layered_dependencies_run_once_per_key`,
`::test_result_stored_between_cache_miss_and_claim_is_reused`,
`test_known_defects.py::test_d33_caller_the_owner_just_stopped_waiting_for_does_not_run_the_query_again`,
`::test_d34_cancelling_a_waiting_task_leaves_the_owner_and_other_waiters_alone`,
`::test_d34_sync_query_keeps_its_value_when_a_task_waiting_for_its_run_is_cancelled`.

### G6. Cancellation

A query started with `engine.submit` raises `QueryCancelled` if an input
changes value before it finishes. Its result is discarded and never cached.
Dependencies that already finished keep their results.

Only the submitted call is cancelled. Callers that joined its run (G5) are
not: when the run they wait for ends in `QueryCancelled`, a caller that
cannot be cancelled (a direct call, `compute_many`, a query calling it) or a
submitted call that started after the change claims the query again and
runs it at its own revision. A joined submitted call that is stale itself
raises its own `QueryCancelled`.

Limits: only `submit` cancels. Direct calls, `compute_many` and
`compute_many_stream` always run to completion. A same-value `set` cancels
nothing.

Enforced by: `test_engine_concurrency.py::test_safe_task_cancellation_after_input_mutation`,
`::test_cancellation_race_stress`, `test_async_coverage.py::test_async_query_cancelled`,
`test_scale_behavior.py::test_concurrency_stress_submit_compute_many_and_writes`,
`test_known_defects.py::test_d19_direct_caller_is_not_cancelled_by_a_submitted_run`,
`test_cancellation_joins.py` (direct, `compute_many`, nested and async
callers joining a cancelled submit; a later and a stale submit on one
snapshot; a stress run of direct callers and submits under changing inputs).

### G7. Cycles and fixed points

A query that reaches itself raises `CycleError` naming the path. With
`fixed_point=v` the query is a cycle head instead: the call that reaches it
again returns the current guess (`v` at first, then the result of the run
before), and the head re-runs until a run returns a result equal to the guess
it was computed from. That result is the fixed point for the inputs at the
caller's revision; read through a snapshot, for the snapshot's inputs.

Evaluating a fixed point changes nothing another reader can see.
`engine.revision` does not move, nothing is cancelled, and no input history
is pinned or trimmed. Results computed from a guess stay private to the call
that is iterating; the head and the queries in its cycle are stored together
once the head has converged, and a thread that asks for the head in the
meantime waits for that. The head's cached result depends on everything any
query in the cycle read from outside the cycle, so a change to an input that
only another member reads re-runs the cycle.

Limits, by design:

- **Iteration cap.** A head that has not converged after 1000 runs raises
  `CycleError` saying so. `engine.query(fixed_point=..., max_iterations=n)`
  sets another limit. The error is not cached, like every `CycleError`.
  "Converged" means equal digests (G2), so a cycle over floats that keep
  changing in the last digit runs into the cap.
- **Enter through the head.** The query that is reached again must be the one
  with `fixed_point=`. Calling another member of a cycle whose head has no
  current result reaches that member again first and raises `CycleError`.
  Once the head has a result, in memory or on disk, members can be called
  directly, also when their own results were dropped or never stored (a
  head loaded from disk stores no members): a member whose run reaches
  itself through such a head, or through the head it was last stored
  with, starts again by bringing that head up to date, which is what its
  cached result would have done (D25).
- **A cached error ends the iteration.** If a run of the head raises an error
  that is cached, that error is the result and the other members are not
  stored.
- **Every run re-runs the whole cycle**, and a head nested in another head's
  cycle starts again from its initial guess on each run of the outer one.
- **Threads and tasks.** Any number of threads or asyncio tasks may enter one
  cycle at once, through the head or through other members. A caller that
  would wait for a query another thread or task is running, while that one
  waits (directly or through others) for a query this caller is running,
  does not wait: the engine keeps a record of which run each blocked thread
  or awaiting task waits for, finds the loop in it, and runs the query on
  the caller's own stack, where the cycle is handled as on one thread. A
  caller whose joined run ended in a `CycleError` runs the query itself
  too, since whether a cycle is an error depends on where it was entered.
  So whoever entered through the head gets the fixed point, and a caller
  that entered through another member gets it as well or a `CycleError`;
  without `fixed_point=` every caller gets `CycleError`. A loop closed
  through a wait the engine cannot see (a query body that blocks on a
  thread of its own, or awaits `asyncio.to_thread`, which then calls back
  into the cycle) is not detected and still hangs.

Enforced by: `test_engine.py::test_cycle_detection`,
`::test_long_chain_cycle_detection_behavior`, `test_async_coverage.py::test_async_cycle`,
`::test_sync_cycle_fixed_point`, and `test_fixed_point.py`:
`::test_fixed_point_basic`, `::test_fixed_point_no_default_raises`,
`::test_fixed_point_read_through_a_snapshot_uses_the_snapshot_inputs`,
`::test_fixed_point_member_first_read_through_a_snapshot`,
`::test_evaluating_a_fixed_point_leaves_the_revision_alone`,
`::test_fixed_point_does_not_cancel_or_disturb_submitted_work`,
`::test_fixed_point_follows_an_input_read_only_by_another_member`,
`::test_fixed_point_with_a_stale_result_read_before_the_cycle_is_entered`,
`::test_nested_fixed_points_match_a_fresh_engine_after_every_change`,
`::test_intermediate_results_never_reach_the_store`,
`::test_fixed_point_that_does_not_converge_raises_instead_of_hanging`,
`::test_fixed_point_that_oscillates_between_members_raises`,
`::test_max_iterations_bounds_the_number_of_runs`,
`::test_fixed_point_head_that_raises_a_cached_error`,
`::test_fixed_point_recovers_after_an_uncached_error`,
`::test_caller_that_catches_a_failed_fixed_point_is_cached_normally`,
`::test_cold_entry_through_a_member_without_fixed_point_raises`,
`::test_threads_requesting_one_fixed_point_get_one_result`,
`::test_threads_reading_a_fixed_point_at_different_revisions`,
`::test_async_fixed_point_honours_snapshots_and_the_revision`,
`::test_fixed_point_with_a_disk_cache` (both backends),
`test_known_defects.py::test_d2_fixed_point_query_honours_snapshot`,
`::test_d24_threads_entering_a_cycle_from_both_ends_do_not_deadlock`,
`test_cross_thread_cycles.py` (two threads in both orders with and without
`fixed_point=`, three threads in a three-query cycle, tasks under
`asyncio.gather`, a task and a thread, a stress run of six threads entering
at random members while the input changes, a thread and a task that joined
a run which ended on a guess, a task that iterates a fixed point whose owner
was cancelled, a wait that stays on record when another run is released).

### G8. Error caching

With the default `cache_exceptions=True`, an `Exception` raised by a query is
cached and re-raised on later calls without re-running the body. A tuple
restricts caching to those types. `QueryCancelled`, `CycleError`,
`PersistentCacheError` and `SnapshotExpiredError` are never cached. A caller
that catches a dependency's error still depends on it.

The caller always sees the exception the query raised. With `cache_dir`,
one that cannot be encoded (its `args` hold an unsupported type) is not
cached: it propagates as itself, like an error excluded by
`cache_exceptions`, and the next call runs the body again. Without
`cache_dir` it is cached like any other (G1). `UnencodableValueError` is
never cached.

Limits: the same exception object is re-raised each time, so its traceback
grows. `BaseException` subclasses outside `Exception` are not cached.

Enforced by: `test_error_caching.py::test_error_is_cached`,
`::test_error_cache_invalidation`, `::test_selective_exception_caching`,
`::test_caught_cached_error_does_not_escape_on_reverification`,
`::test_caught_uncached_error_still_tracks_dependencies`,
`::test_uncached_error_two_levels_down_reaches_the_handler`,
`test_known_defects.py::test_d20_unencodable_result_is_returned_or_reported_clearly`,
`::test_d20_exception_with_unencodable_args_reaches_the_caller`,
`test_unencodable_values.py` (on both backends: paths through
lists, dicts, keys, dataclasses, NamedTuples and exception args; nothing
cached; a caller that catches the error; async queries; inputs and input
defaults, also in memory; fixed-point guesses and results; `compute_many`),
`test_opaque_values.py` (in memory: unencodable results and exceptions
cached, dependents re-run, `memoize=False`, async, snapshots,
`compute_many`; inputs and fixed points still refused).

### G9. TTL

A `ttl=` query re-runs when it is needed after its TTL has elapsed: called
directly, or reached through any query that depends on it, however many
levels up, at the newest revision or through a snapshot. No revision change
is needed. If the new result is equal, dependents do not re-run.

How: every cached result carries the earliest time at which a TTL result
beneath it expires. Until then it is served as before; after it, the result is
verified again, which re-runs the expired query and then whatever its new
value changes. A result with no TTL query beneath it carries no expiry and
pays one attribute check per cache hit.

Limits, by design:

- **The clock is `time.time()`.** A TTL bounds the age of a result when a
  call starts; a call that is already running keeps what it read.
- **A TTL result is shared by all revisions.** It is not part of a snapshot:
  a snapshot read after expiry sees the snapshot's inputs and a fresh TTL
  result.
- **A dependent can lag by up to one TTL.** If a TTL result is dropped early
  (eviction, `prune`) and recomputed with a new value, queries that used the
  old one keep it until the time the old one would have expired.

Enforced by: `test_ttl.py::test_ttl_invalidation_basic`,
`::test_ttl_invalidation_async`,
`::test_expired_ttl_refreshes_a_chain_of_dependents`,
`::test_expired_ttl_with_an_equal_result_does_not_rerun_dependents`,
`::test_expired_ttl_refreshes_a_dependent_read_through_a_snapshot`,
`::test_dependent_expires_with_the_shortest_ttl_beneath_it`,
`::test_expired_ttl_reaches_a_caller_that_caught_a_dependency_error`,
`::test_expired_ttl_refreshes_async_dependents`,
`::test_expired_ttl_refreshes_dependents_loaded_from_disk` and
`::test_expired_ttl_refreshes_async_dependents_loaded_from_disk` (both backends),
`::test_queries_without_a_ttl_beneath_them_carry_no_expiry`,
`::test_ttl_result_is_still_good_at_exactly_its_ttl`,
`::test_ttl_query_stays_cached_above_a_query_without_an_expiry`,
`::test_verified_dependent_keeps_the_shortest_expiry_beneath_it`,
`::test_result_loaded_from_disk_expires_when_the_stored_one_would`,
`::test_ttl_result_loaded_from_disk_expires_with_a_shorter_ttl_beneath_it` and
`::test_dependent_loaded_from_disk_expires_with_the_shortest_ttl_beneath_it`
(these six for sync queries and for async ones awaited on a loop, the last
three on both backends),
`test_known_defects.py::test_d3_expired_ttl_dependency_refreshes_through_its_dependent`,
`test_dirty_propagation.py::test_ttl_dependency_keeps_dependents_on_the_slow_path`.

### G10. `memoize=False`

The query keeps its digest, dependencies and effects but no value. Every call
that needs the value re-runs the body; verification alone does not. Nothing is
written to disk for it.

Enforced by: `test_memoize_false.py::test_memoize_false`,
`::test_memoize_false_multiple_calls_same_revision`.

### G11. Code-aware invalidation

Registering a query or input whose bytecode hash differs from the one cached
drops its results, in memory and on disk, and makes every other cached result
verify its dependencies again before it is used. A result computed from the
old code is therefore re-run on its next use, at the newest revision or
through a snapshot, whether or not an input changed in between; if it comes
out equal, the results above it are kept. This happens at registration, not
while the old function is still registered. Handles to the replaced function
go stale and raise `StaleHandleError` when used (see G1, identity).

Registering a query again with a different `ttl=` does the same, because
cached results carry an expiry worked out from the old TTL (G9).

Cost: after such a registration every cached result pays one dependency walk
on its next use, as it does after an input change it does not depend on when
the walk cannot be skipped. Results read directly from a replaced input are
dropped and re-run.

The hash covers the function's bytecode, constants, names and local variable
names. It does not cover closure values, default arguments, globals, or any
helper function the query calls. Editing those leaves stale results in place.
One exception inside a process: a function registered over one with equal
bytecode but different captured objects is treated as different code. Its
results are dropped, and for the rest of the process its persisted results
are stored under a salted hash, so it cannot load what the earlier function
wrote. Across runs the blind spot remains: two closures of one body
registered under one name in different runs can serve each other's persisted
results. `name=` avoids that.

**Manual versions.** What the hash cannot see, the program can declare:

- `engine.query(version=v)` (a `str` or an `int`) is part of the query's
  code hash. Registering the query with another `version` is a code change:
  its results are dropped in memory, and persisted results stored under
  another version are not served (G16) but recomputed, and replaced when
  the new result is stored. Dependents follow the new result as for any
  code change: they re-run if its value differs and keep their results if
  it is equal.
- `Engine(cache_version=v)` salts the hash of every persisted result of
  that engine. An engine with another salt, or none, does not serve them.
  It is the switch for a change that touches everything (a library
  upgrade, a new data format); it does not touch named collections, which
  are data.

Results stored under an old version are not deleted when a new version is
registered; each is replaced when its query is next computed and stored,
and `prune(vacuum_disk=True)` or `clear_disk_cache()` removes the rest.

Enforced by: `test_cache_versions.py` (both backends: a helper edit served
stale until the version changes, a new version with a different and with
an equal result, the engine salt, a new version inside one process, argument
checks),
`test_code_versioning.py::test_memory_cache_invalidation_on_code_change`,
`::test_disk_cache_invalidation_on_code_change`,
`::test_input_cache_invalidation_on_code_change`,
`test_dirty_propagation.py::test_reregistering_a_query_dirties_its_dependents`,
`test_known_defects.py::test_d22_reregistered_query_reaches_dependents_at_the_same_revision`,
`test_code_versioning.py::test_reregistered_query_reaches_a_chain_of_dependents_at_the_same_revision`,
`::test_reregistered_query_with_an_equal_result_stops_at_its_first_dependent`,
`::test_reregistered_query_reaches_dependents_read_through_a_snapshot`,
`::test_reregistered_query_reaches_dependents_after_a_result_was_evicted`,
`::test_reregistered_input_default_reaches_dependents_at_the_same_revision`,
`::test_reregistered_input_default_reaches_dependents_that_never_stored_it`,
`::test_reregistered_query_reaches_async_dependents`,
`::test_reregistering_with_another_ttl_applies_it_to_cached_results`,
`::test_reregistered_query_reaches_dependents_with_a_disk_cache` and
`::test_reregistered_query_reaches_in_memory_dependents_with_a_disk_cache`
(both backends),
`test_query_identity.py::test_reload_with_changed_code_makes_old_handles_stale`,
`::test_reload_with_unchanged_code_keeps_results_and_handles`,
`::test_replacing_function_does_not_load_the_old_ones_results_from_disk`.

### G12. Accumulator effects

Effects pushed during a query are stored with its result and replayed on every
cache hit, to every waiting caller, in execution order. A query that fails
leaks no partial effects. `compute_many(effects=)` merges per-call effects in
call order.

A fixed-point head runs until a run returns the guess it was computed from
(G7). Only that last run counts: its effects (the head's own pushes and
those of the members it ran) are the head's effects, and each member keeps
what it pushed in that run. The pushes of the abandoned runs before it are
taken back from every caller above the head, including the `effects=`
dict, so a caller sees the converged run once, whether the cycle was
computed for it, replayed from the cache or loaded from disk. A member
whose run is abandoned to enter its cycle through the head (G7) is taken
back the same way. A head whose last run raised a cached error keeps that
run's effects with the error.

Inside a rewritten expression (G22) the effects are those plain Python
pushes, in its order: a pipeline runs its element, filter and `key=`
functions only for the elements that changed, in the order the changes
arrived, so it cannot push them itself. It notices at run time when any of
them pushes an effect, or calls a query whose effects are replayed: that
run's pushes are taken back, the expression is evaluated plainly, and so it
is on every later run of that expression in this engine. Detection is at
run time because the push may sit in a helper function or depend on the
element: a pipeline whose functions have never pushed stays incremental,
which gives the same effects (none) as plain evaluation of the elements it
holds.

Limits: an expression whose functions push effects costs a plain
evaluation on every run (G23).

Enforced by: `test_engine.py::test_side_effect_replay_on_cache_hits`,
`::test_nested_side_effect_replay_propagates_to_parent_effects`,
`::test_failed_query_does_not_leak_partial_accumulator_effects`,
`test_engine_compute_many.py::test_compute_many_can_collect_accumulator_effects`,
`test_engine_concurrency.py::test_query_dedup_replays_effects_to_all_concurrent_callers`,
`test_known_defects.py::test_d26_fixed_point_effects_are_those_of_the_final_run`,
`test_fixed_point_effects.py` (callers above the head, the head and a member
called directly, an input change, a cached error in the last run, nested
heads, `compute_many`, threads, async queries, replay from disk on both
backends),
`test_known_defects.py::test_d31_effects_pushed_per_element_survive_an_incremental_update`,
`test_rewrite_effects.py` (fourteen bodies over a list compared with plain
evaluation after each of eight kinds of change: map, filter, both,
short-circuiting `any` and `all`, `sorted` with `key=`, `max`, `len`,
`join`, a push that depends on the element, builtin `map` and `filter`, an
element that raises after pushing; a stage calling a query with effects, a
stage closing over the accumulator, a dict source, a snapshot, pushes beside
the pipeline, a pure pipeline staying incremental).

### G13. Eviction, prune and sweep

Dropping a cached result (LRU at `max_entries`, `prune`, `sweep_unaccessed`)
never changes what a later call returns; it only makes that call recompute.

This includes the members of a fixed-point cycle: a member whose result was
dropped is brought back by entering the cycle through its head, as a cached
result would have been (G7).

Enforced by: `test_dirty_propagation.py::test_lru_eviction_never_serves_stale_results`,
`::test_dropping_a_middle_memo_dirties_its_dependents`,
`test_scale_behavior.py::test_prune_stress_keeps_consistent_reachable_subgraph`,
`test_iterative_verification.py::test_deep_chain_with_evicted_middle_memo`,
`test_known_defects.py::test_d25_evicting_a_cycle_member_does_not_change_the_outcome`,
`test_fixed_point_dropped_members.py` (each way of dropping a member,
members and head dropped together, input values where the head raises or
leaves the cycle, a snapshot, six threads, async queries, a three-member
cycle, and a head loaded from disk with no members, on both backends).

### G14. Async parity

`async def` queries follow G1–G13. Sync queries may call async queries and the
reverse.

**Synchronous code on the loop's thread.** A sync query called on the
thread that runs an event loop (from a coroutine, say) cannot wait for an
async query: running one needs that loop, and so does finishing one that runs
as a task on it, while the synchronous wait blocks it. So there:

- calling an async query from the sync query's body takes the synchronous
  path: a cached result is returned as usual (before, the body got a
  coroutine back and failed with a `TypeError` that was cached);
- an async query or async input default that has to run raises
  `EventLoopBlockedError` at once, naming it;
- waiting for a run owned by a task on the same loop raises
  `EventLoopBlockedError` for an async query, and for a sync query runs it
  on this thread instead of waiting;
- `EventLoopBlockedError` is never cached and is raised out of dependency
  verification too, so the same call works once the async result is cached,
  and from another thread (`await asyncio.to_thread(sync_query)`) at once.

Waiting for a run on another thread or another loop is unaffected.

Limits: the async verification path is recursive where the sync path is
iterative, so very deep stale chains may hit the recursion limit. Almost all
async internals are excluded from coverage with `# pragma: no cover`.

Enforced by: `test_async.py::test_async_query_basic`, `::test_mixed_graph_sync_async`,
`::test_dedup_yields_loop`, `test_async_dependency_tracking.py`,
`test_history_retention.py::test_no_pins_leak_from_async_calls_and_cancellation`,
`test_known_defects.py::test_d21_sync_query_on_the_loop_does_not_deadlock_on_an_async_run`,
`test_event_loop_blocking.py` (a task on the same loop, the `to_thread`
workaround, uncached and cached async queries and async input defaults from
a sync body on the loop, a sync query owned by a task, a task on another
loop, async queries awaiting each other).

### Thread safety

Safe from any thread: calling queries, reading inputs, `input.set`,
transactions, registering queries and inputs, `prune`, `sweep_unaccessed`,
traces and stats. All of these take the store lock. Mutating a collection is
safe from any thread; while a transaction that changed it is open, a
mutation from outside that transaction raises `RuntimeError` (G25).

Two threads or tasks entering one dependency cycle through different queries
get what one thread would (G7). Two engines changing the same named
collection is refused with `PersistentCacheError` (G26).

## Persistence

### G15. Reuse across runs

A new `Engine` on the same `cache_dir` returns a stored result without running
the body when every recorded dependency matches: an input by the digest of its
current value, a dependency query by the digest of its up-to-date result. An
input never set in the new session is matched by the digest of its default.

Arguments are stored by type, as they are keyed (G1): `q(True)` never
loads what `q(1)` stored.

Limits: a query whose arguments cannot be encoded is computed but not
persisted. When a fixed-point cycle converges only the head's result is
written; the other members are recomputed from it in a new session. Query identity on disk is `module:qualname` unless `name=` is
given, so two scripts run as `__main__` with same-named queries share entries.

Enforced by: `test_disk_cache.py::test_cross_session_hit_skips_all_query_bodies`,
`::test_file_edit_invalidates_only_its_chain`,
`::test_early_cutoff_survives_across_sessions`,
`::test_deep_dependency_chain_cross_session`,
`::test_loaded_result_is_verified_like_a_computed_one`,
`::test_record_without_an_error_flag_is_loaded_as_a_value`,
`test_persistence_permutations.py::test_permutation_incremental_mdbx`,
`test_known_defects.py::test_d23_async_query_result_is_reused_by_a_new_engine`,
`test_ttl.py::test_expired_ttl_refreshes_async_dependents_loaded_from_disk` and
`::test_async_result_loaded_from_disk_keeps_its_computed_time` (both backends).

### G16. No stale persisted result

A persisted result is not served if an input value differs, a dependency's
result differs, the set of dependencies changed, its TTL elapsed, the
function's bytecode hash changed, its `version=` changed, or the engine's
`cache_version=` differs from the one it was stored under. G11's blind
spots apply here too, for code whose change no version declares.

Enforced by: `test_cache_versions.py`,
`test_code_versioning.py::test_disk_cache_invalidation_on_code_change`,
`test_disk_cache.py::test_dynamic_graph_topology_cross_session`,
`::test_in_session_invalidation_still_works_after_hydration`,
`::test_dependent_notices_a_result_loaded_again_with_another_value`,
`test_ttl.py::test_result_loaded_from_disk_expires_when_the_stored_one_would`,
`::test_ttl_result_loaded_from_disk_expires_with_a_shorter_ttl_beneath_it` and
`::test_dependent_loaded_from_disk_expires_with_the_shortest_ttl_beneath_it`
(a stored result whose TTL, or one beneath it, has elapsed; both backends).

### G17. Durability

Results are queued when their query finishes and written in batches by a
background thread. They are on disk once `flush_disk()` or `shutdown()`
returns, and also when the interpreter exits normally without either: an
`atexit` hook writes whatever is still queued. "Normally" covers falling off
the end of the script, `sys.exit()` and an unhandled exception.

A write that fails never stops the writer and never hangs a caller. The
batch it hit is dropped and the writer goes on with the next one. The next
`flush_disk()` or `shutdown()` raises `PersistentCacheError` naming the
directory and the number of results lost, with the original error (sqlite
"database is locked", a full map) as its `__cause__`; it is reported once.
`shutdown()` closes the cache before raising. `clear_disk_cache()` forgets
such failures, since it deletes the results anyway. A failure still
unreported at a normal exit is printed as a `RuntimeWarning`. Results in
memory are unaffected, and the `disk_stores` counter and `disk_store` trace
count only committed writes (a failed one traces `disk_store_failed`).

Limits: a process that is killed (`os._exit`, SIGKILL, a crash in native
code) runs no exit hook and loses the results still queued; the cache stays
usable. Neither backend fsyncs each commit, so a power loss can lose recent
commits even after a flush. If the writer thread is not running (it stops
at `shutdown()`, or after its engine was dropped), `flush_disk()` raises
`PersistentCacheError` and the exit hook gives up, so neither waits for
ever.

**Engines dropped without `shutdown()`.** The writer thread holds the cache
but not the engine, so an engine the program no longer references is
garbage-collected like any object (it has reference cycles, so by the cycle
collector). Its writer then writes every result still queued, closes the
cache, which releases the engine's collection locks (G26), and ends; a write
that failed and was never reported is a `RuntimeWarning`. Until the thread
has done so, a normal exit still waits for it. `with Engine(...) as engine:`
calls `shutdown()` at the end of the block; if the block raised, a write
failure `shutdown()` would report is a `RuntimeWarning` instead, so the
block's own exception propagates.

Enforced by: `test_known_defects.py::test_d15_results_survive_a_normal_exit_without_shutdown`,
`test_durability_at_exit.py::test_normal_exit_without_shutdown_keeps_every_result`
(both backends; plain exit, `sys.exit`, unhandled exception, engine dropped),
`::test_exit_does_not_hang_when_the_writer_thread_is_dead`,
`::test_hard_kill_is_not_covered_but_leaves_a_usable_cache`,
`test_known_defects.py::test_d18_flush_disk_returns_after_a_writer_error`,
`test_disk_writer_errors.py` (both backends: the error and its cause, the
writer going on after it, `shutdown()` reporting and closing, counters,
`clear_disk_cache()`, a stopped writer, the exit warning, threads writing
through intermittent errors),
`test_engine_lifetime.py` (both backends: a dropped engine collected, its
queued results written and its collection lock released; no threads left
behind; `with Engine(...)`, with and without an error in the block; a failed
write of a dropped engine).

### G18. Round-trip fidelity

Supported: `None`, `bool`, `int` of any size, `float`, `complex`, `str`,
`bytes`, `bytearray`, `list`, `tuple`, `set`, `frozenset`, `dict`,
`collections.deque` (with its `maxlen`), `range`, `decimal.Decimal`
(digits, exponent and sign as written, NaN included), `fractions.Fraction`,
`uuid.UUID`, `Enum` members (`IntEnum`, `StrEnum` and `Flag` combinations
included), `pathlib` paths of the four concrete classes (`PurePosixPath`,
`PureWindowsPath`, `PosixPath`, `WindowsPath`), `datetime`, `date`,
`time`, `timedelta`, `timezone`, dataclasses, `NamedTuple`s, exceptions,
numpy arrays and scalars when numpy is installed, and classes registered
with `cascade.register_type`. These decode to a value of the same type that
no query could tell from the original: equal, and also the same where `==`
looks away (a `Decimal`'s trailing zeros, a Windows path's case, the sign
of a zero, a `datetime`'s `fold`, `range(0)` against `range(1, 1)`).

A dict comes back in the insertion order it was stored with.

**Registered types.** `cascade.register_type(cls, encode=..., decode=...,
name=None)` (also `Engine.register_type`) makes exact instances of `cls`
encodable: `encode(obj)` returns a value Cascade can already encode, and
`decode(that)` rebuilds the object. Values whose `encode` results are equal
are treated as the same value, so `encode` must capture everything a query
can observe. `name` (default `module:qualname`) identifies the type on
disk; change it when the encoding changes. The registry is process-wide,
registering a native type or one already registered with other functions is
refused, and a value stored under a name that is not registered when it is
read is not decoded: a cached result is recomputed, a named collection
raises. Pickle is never used.

**numpy.** Arrays of exact type `numpy.ndarray` whose dtype holds no Python
objects and no fields, and numpy scalars, are encoded as dtype, shape and
bytes. numpy is never imported to encode (a value can only be an array if
the program imported numpy) and is not a dependency. A Fortran-ordered
array comes back C-ordered. Other arrays (object, structured, subclasses
such as masked arrays) need `register_type`.

Known losses: subclasses of containers become the base type (an
`OrderedDict` a `dict`); `zoneinfo` zones and named `timezone`s become
fixed offsets; exceptions keep only type and `args`. Dataclasses with
`init=False` fields and classes defined inside functions cannot be decoded,
so their results recompute every session. A `PosixPath` cannot be built on
Windows, nor a `WindowsPath` elsewhere, so such a result recomputes there.

Unsupported, and handled as G1 says: anything else, for example plain class
instances, `ast` nodes and pandas objects, unless registered.

Enforced by: `test_codec_properties.py` (Hypothesis: nested values of every
supported type decode to the same value and re-encode to the same bytes,
and the digest of a decoded copy is the digest of the original),
`test_value_types.py` (each type as a query argument, an input value and a
result, in memory and on both backends, and read back by a new engine with
its type; registered types and their errors; numpy),
`test_disk_cache.py::test_round_trip`,
`::test_round_trip_preserves_container_types`,
`::test_bytearray_round_trips_and_nan_round_trips`,
`test_disk_format2.py::test_round_trip_preserves_value_and_type`,
`::test_container_subclasses_encode_as_their_base_type`,
`test_dict_order.py::test_decoded_dict_keeps_its_order`,
`::test_stored_dict_comes_back_in_the_order_it_was_returned` and
`::test_compacted_named_dict_restores_its_insertion_order` (both backends).

### G19. Deterministic encoding

Two values of the same type with the same contents encode to the same bytes,
and so have the same digest. For sets "the same contents" ignores the order
in which elements were added. For dicts it includes the insertion order,
because code can observe it (G2): the same items in another order are other
bytes. This changed with disk format 3; format 2 sorted dict keys, so its
results held dicts in an order the query had not returned, and they are
dropped on open (G20). Collection logs written under format 2 are read
unchanged. The converse holds as well: two values a query could tell apart
never encode to the same bytes, in the disk codec and in the in-memory
digest alike (`1`, `1.0`, `True`, `Decimal(1)` and `Level.LOW` are five
values).

Enforced by: `test_codec_properties.py` (Hypothesis: equal bytes or equal
digests only for values no query can tell apart; sets in any insertion
order; look-alikes such as `Decimal("1.0")` and `Decimal("1.00")`),
`test_disk_cache.py::test_encode_is_deterministic_for_dict_and_set_ordering`,
`test_disk_format2.py::test_encoding_is_independent_of_insertion_order`,
`test_dict_order.py::test_digest_tells_dict_orders_apart_at_any_depth`,
`::test_digest_of_sets_ignores_how_they_were_built`,
`::test_results_stored_with_sorted_dicts_are_dropped_and_collections_kept`
(both backends).

### G20. Corruption and format changes

An undecodable query entry, a missing blob or a digest mismatch is treated as a
miss and recomputed. Opening a directory with the other backend sees an empty
cache, because each backend uses its own files.

A cache directory carries two format numbers. `DISK_FORMAT` (now 5) covers
cached query results: results written under another number are deleted on
open and recomputed. `COLLECTION_FORMAT` (now 2) covers the logs of named
collections, which are data and are never deleted: a change of `DISK_FORMAT`
leaves them as they are, in both directions. Collection format 2 added the
value types of result format 4; format 1 logs are read unchanged, and the
directory is restamped 2, so an older release refuses it instead of failing
on a value type it does not know. A directory whose collection data this version
cannot read (a newer `COLLECTION_FORMAT`, or collections left by a release
older than the ones known) is refused with `PersistentCacheError` at
`Engine(cache_dir=...)`, and nothing in it is changed. A directory written
before collections had a number of their own (result format 2) is read as
collection format 1.

Limits: a release from before this split still wipes collections when it
opens a directory with a result format it does not know, so do not open a
directory with an older release than the one that wrote it.

Enforced by: `test_disk_cache.py::test_corrupt_blob_falls_back_to_recompute`,
`::test_malformed_dependency_row_falls_back_to_recompute`,
`::test_format_bump_wipes_stale_cache`, `::test_format_bump_sqlite`,
`test_format_bump.py` (both backends: results recomputed and collections
kept across a bump and back, a compacted log with its tail, a directory
without a collection stamp, unreadable collection formats refused with
nothing dropped),
`test_disk_format2.py::test_format_change_keeps_collection_logs`,
`test_known_defects.py::test_d16_format_change_does_not_silently_drop_collections`,
`test_disk_format2.py::test_corrupt_inline_value_falls_back_to_recompute`,
`::test_foreign_extension_payload_is_rejected`.

### Trust boundary

A cache directory is trusted input. Loading one can import any importable
module named in it and construct any dataclass, `NamedTuple` or exception class
with arguments it chooses, look up any `Enum` member, import numpy, and call
the `decode` function of any registered type with a value it chooses. Result
values are not authenticated. Nothing is unpickled, and registering a type
never adds pickle. Only open cache directories you wrote.

## Collections, rewriting and transactions

### G21. Builtin equivalence

`CascadeList`, `CascadeSet` and `CascadeDict` subclass the builtins. Every
mutating method changes storage and logs a diff.

Inside a query, every read made through the collection's own methods and
operators records a dependency and sees the contents as of the query's
revision:

- all three: iteration, `len`, truth value, `in`, `reversed` (not sets),
  `copy()`, `copy.copy` and `copy.deepcopy`, `==` and `!=`, `repr`, `str`
  and formatting;
- lists: indexing and slicing, `index`, `count`, `<` `<=` `>` `>=`, `+` and
  `*` in either operand order;
- sets: `<` `<=` `>` `>=`, `|` `&` `-` `^` with a `set` in either operand
  order, `union`, `intersection`, `difference`, `symmetric_difference`,
  `issubset`, `issuperset`, `isdisjoint`;
- dicts: indexing, `get`, `|` in either operand order, and the `keys()`,
  `values()` and `items()` views with their iteration, `len`, `in`,
  comparisons and set algebra.

Whatever the builtins build on those is tracked with them: `list(xs)`,
`tuple(xs)`, `sorted(xs)`, `sum`, `min`, `max`, `[*xs]`, `f(*xs)`,
`plain.extend(xs)`, `dict(d)`, `{**d}`, `f(**d)`, `plain.update(d)`,
`json.dumps`, `str.join`, `set(xs)` of a list or dict. When the other operand
is a collection too (`a | b`, `xs == ys`), both are tracked.

A `CascadeSet` iterates in insertion order, as a dict's keys do: the order
in which the elements now present were added, so removing one and adding it
again moves it to the end. A plain `set` iterates in an order that depends
on hashes and on its history; Cascade chooses insertion order so that the
order is one thing everywhere: a `for` loop, a helper function, `list(s)`,
`sorted` ties, `repr`, a rewritten comprehension, a snapshot read and a
collection restored from disk all see it (D29). Equal elements keep the
first one added, and its place. `pop()` still removes whichever element the
storage gives, like the builtin.

Every read gives a plain `list`, `set`, `dict` or scalar, never a second
collection object. An operand the builtin refuses raises the builtin's
`TypeError`, after the other operand's reflected method has had its turn.

A mutator that raises changes nothing: storage and log stay in agreement, so
queries see exactly what the collection holds. This makes
`CascadeSet.update` and `difference_update` atomic, where the builtin keeps
the elements handled before the failing one.

Limits:

- **C code that reads a `CascadeSet`'s storage itself is not tracked.** For
  any `set` subclass, CPython's set and frozenset code reads the other
  operand's hash table directly, and no method of the subclass runs. Inside
  a query these record no dependency and read the live contents: `set(s)`
  and `frozenset(s)` (except `set(s)` written in a rewritten query body,
  which is tracked), `{*s}`, a plain set's or frozenset's methods given the
  collection (`plain.union(s)`, `plain.update(s)`, `plain.issubset(s)`,
  `plain |= s` and the like), and operators with a `frozenset` on the left
  (`fs | s`, `fs == s`). Tracked spellings: `s.copy()`, the collection's own
  method or operator (`s.union(plain)`, `s | fs`, `s == fs`), or `iter(s)`.
  Lists and dicts have no such path: CPython takes its shortcuts for exact
  `list` and `dict` only, or for subclasses that do not override `__iter__`.
- **Calling a builtin's method on a collection by hand bypasses it**:
  `list.__len__(xs)`, `set.union(s, t)`, `dict.items(d)`.
- **C code that changes a collection's storage itself logs nothing.**
  CPython calls no method of the subclass there, so it cannot be
  intercepted. Cascade notices a change of size: every collection records
  its size at each change it makes, and the next use of the collection (any
  read, inside a query or not, any mutation, a rewritten pipeline over it)
  compares. If the size differs, the change is undone (the storage is put
  back to what the collection recorded, which is what queries see) and
  `CollectionBypassedError` is raised, naming the collection. It is never
  cached: a query that met it reads the repaired collection on its next
  run. This catches `heapq.heappush` and `heapq.heappop`, and a builtin's
  method called by hand that adds or removes (`list.append(xs, v)`,
  `list.pop(xs)`, `list.extend(xs, ...)`, `set.add(s, v)`,
  `set.discard(s, v)`, `dict.__setitem__(d, new_key, v)`,
  `dict.pop(d, k)`). `bisect.insort` calls the list's own `insert` and is
  tracked. Not noticed, because the size stays the same:
  `heapq.heapify`, `heapq.heapreplace`, `heapq.heappushpop` (when it
  replaces the smallest element), `list.sort(xs)`, `list.reverse(xs)`,
  `list.__setitem__(xs, i, v)`, `dict.__setitem__(d, present_key, v)`, and
  a by-hand removal followed by an addition. After one of those, queries
  keep seeing the old contents, and a list's element ids no longer match
  its positions, so later diffs can name the wrong element: use the
  collection's own methods (`xs.sort()`, `xs.reverse()`, `xs[i] = v`), or
  work on a copy and write it back. Until the collection is next used, a
  result cached before the change is served as it was.
- Dict views are wrappers, not `dict_keys` objects: `isinstance(d.keys(),
  collections.abc.KeysView)` is false and there is no `.mapping`.
- `repr` of a `CascadeSet` is `CascadeSet({...})`, in iteration order; lists
  and dicts print as the builtins do.
- A plain set built from a `CascadeSet` by C code (`set(s)`, `s.copy()`,
  `s | t`) is a builtin set and iterates in the builtin's order.
- Collections cannot be pickled: `pickle.dumps` raises `TypeError`. Pickle
  `xs.copy()`.

Enforced by: `test_collection_read_tracking.py` (some 180 read expressions,
each compared with the builtin after creation, after mutations and through a
snapshot, with the result type; operand errors; two collections in one
expression; `::test_limit_set_read_by_c_code_is_not_tracked` pins the first
limit), `test_known_defects.py::test_d5_set_union_inside_a_query_sees_later_mutations`,
`::test_d28_heapq_on_a_cascade_list_reaches_queries`,
`test_collection_bypass.py` (nine size-changing bypasses of all three kinds,
each met by six kinds of read, a mutation and a query that runs again; a
rewritten pipeline; `bisect.insort`; a transaction; named collections on both
backends; writers and readers on six threads with no false alarm; the
same-size limit pinned for eight operations, and the cached result served
until the next use),
the example tests in `test_incremental_collections.py`
(`::test_failed_list_mutator_changes_nothing`, `::test_failed_set_mutator_changes_nothing`
and `::test_failed_dict_mutator_changes_nothing` cover failing calls of every
mutator) and its Hypothesis tests `::test_list_log_replay_matches_contents`,
`::test_dict_log_replay_matches_contents`, `::test_set_log_replay_matches_contents`.

### G22. Rewrite equivalence

A query rewritten into an incremental pipeline returns exactly what the same
Python returns without rewriting, after any sequence of mutations: the same
value down to its type (`1`, `True` and `1.0` are told apart, and so are
`0.0` and `-0.0`), or the same exception with the same message.

Rewritten: single-`for` comprehensions, `map`, `filter`, `reversed`, and the
reducers `sum`, `len`, `min`, `max`, `any`, `all`, `sorted`, `list`, `set`,
`dict` and `str.join`.

Not rewritten, running as plain Python: comprehensions with more than one
`for`, `enumerate`, `zip`, `min`/`max` with `key=` or `default=`, `sum` with a
start value, `len` of anything but a list comprehension or a bare collection
(`len(map(...))` is the `TypeError` it is in Python), lambdas, async queries,
and any query for which one of those builtin names is rebound: in the query,
in a function enclosing it, or at module level.

Also left as written, because moving code into a helper function would
change what a name means:

- a comprehension with an assignment expression (`:=`) anywhere in its
  element or `if` clauses, nested comprehensions included. Python binds the
  name in the enclosing function; in the helper that holds the element it
  would bind there and be lost (D30);
- a pipeline inside a comprehension that is itself left as written (two
  `for` clauses, a `:=`, ...) whose element or `if` clause uses that
  comprehension's loop variable. Its helper is placed before the statement,
  outside the comprehension, where the loop variable does not exist (D32).
  A pipeline inside a comprehension that is rewritten is placed in that
  comprehension's own helper and sees its loop variable.

How each reducer matches its builtin:

- **`sum`** keeps a running total while every value is an `int` or `bool`.
  With any other value present (a `float`, a `Fraction`, a string) it calls
  the builtin `sum` on the stored values in order, so the result is the
  builtin's to the last bit, and so is the error for values that cannot be
  added.
- **`min`, `max`, `sorted`** keep keys in a sorted structure while all keys
  have one shape that Python orders totally: numbers (no NaN), `str`,
  `bytes`, or tuples of those with matching positions. Equal keys are
  ordered by position, as the builtins order them: `min` and `max` return
  the first of several equal elements, and `sorted` is stable, with
  `reverse=True` too. With any other key present (NaN, mixed types, sets,
  objects) the builtin itself is applied to the stored keys and values.
- **`any`, `all`** count truthy elements.
- **`list`, `set`, `dict`, `join`** hand the stored values to the builtin in
  the collection's order, so a `set` or `dict` keeps the first of two equal
  values and malformed elements raise the builtin's error.
- **`len`** counts the elements that pass the filters.

Exceptions and laziness. Python evaluates a pipeline lazily and in order:
`any` and `all` stop at the first element that decides them, and the first
element that raises ends the call. A pipeline evaluates every element, and in
the order the changes arrived. So while the collection holds an element for
which a stage, key function or reducer step raised, the expression is
evaluated plainly over the collection's contents; that run returns or raises
exactly as Python does. `any(1 / z for z in [1, 0])` is `True`, and of two
elements that raise, the first in the collection is the one reported. The
pipeline remembers which elements raised and keeps the rest of its state, so
it answers incrementally again once they are removed or replaced. Until then
each run costs what Python's own evaluation costs: up to the element where it
stops.

Function expressions are evaluated in Python's order: for
`sorted(filter(f, map(g, xs)), key=k)` that is `f`, `g`, `xs`, `k`.

A `CascadeSet` is visited in its insertion order on both paths (G21).

Enforced by: `test_rewrite_differential.py` (Hypothesis: random mutation
sequences over a list, a dict and a set, about 100 query bodies, each compared
after every mutation and through a snapshot with the same body over a plain
`list` or `dict`; values are ints, bools, floats with NaN, infinities and
`-0.0`, strings, tuples with ties, and mixtures that raise; one run uses lists
of 20 to 45 elements that are mostly ties),
`test_rewrite_reducers_exact.py` (one example per former difference, and which
updates stay incremental), `test_known_defects.py::test_d7_rewritten_float_sum_matches_builtin`,
`::test_d7_rewritten_sorted_reverse_is_stable`, `::test_d7_rewritten_any_short_circuits`,
`test_incremental_pipeline.py::test_list_reducers_match_recompute_through_mutations`,
`::test_property_list_pipeline_equals_recompute` (Hypothesis),
`::test_property_dict_pipeline_equals_recompute` (Hypothesis),
`::test_min_max_handle_removal_of_current_extreme`, `::test_empty_collection_reducers`,
`test_ast_rewrite.py::test_shadowed_builtin_disables_rewriting`,
`::test_builtin_shadowed_in_an_enclosing_function_disables_rewriting`,
`::test_enumerate_and_zip_sources_untouched`,
`test_known_defects.py::test_d30_walrus_in_a_comprehension_binds_in_the_query`,
`::test_d32_nested_pipeline_sees_the_loop_variable_of_a_kept_comprehension`,
`::test_d29_set_order_is_the_same_rewritten_or_not`,
`test_set_order.py` (eleven ways of iterating, rewritten and not, after
thirteen kinds of change; snapshots, equal elements, a rolled-back
transaction, a restart and compaction on both backends, four writer
threads, effects pushed per element),
`test_rewrite_scoping.py` (`:=` in the element, the filter, a reducer's
generator, nested comprehensions and their filters, set and dict
comprehensions, beside a comprehension that is still rewritten; pipelines
nested in two-`for` comprehensions, in a `:=` comprehension, in a generator
left as written and in a rewritten comprehension, with a same-named variable
in the query; each compared with plain evaluation after eight changes).

### G23. Incrementality

After an append, insert, update or remove, a rewritten query runs its mapping,
filter and key functions once for the changed element. What the reducer then
does to produce the result:

| Reducer | Per change | Result |
|---|---|---|
| `sum` of ints, `len`, `any`, `all` | O(1) | O(1) |
| `sum` with a non-int value present | O(1) | one pass of the builtin `sum` over the stored values |
| `min`, `max` | O(log N) | O(1) |
| `sorted` | O(log N) | O(output) |
| `list`, `set`, `dict`, `join` | O(1) | O(output) |

Where exactness costs more (G22):

- equal keys are ordered by position, which is known for free only for
  elements appended at the end or replaced in place. After an insert
  elsewhere, a group of equal keys that gains a member has to be put in
  order by looking up its members' positions: one scan of the collection
  per member for up to eight, one pass over the collection beyond that, and
  now and then an O(N log N) renumbering of everything. `sorted` does this
  for every such group, `min` and `max` only for the group that holds the
  result;
- keys outside the totally ordered shapes make `min`, `max` and `sorted` run
  the builtin over the stored keys on every run: no stage function is called
  again, but the scan or sort is;
- an element that raises makes every run a plain evaluation while it is in
  the collection: as far as Python itself gets, so to the first truthy
  element for `any`, or to the element that raises;
- an element, filter or `key=` function that pushes accumulator effects (or
  calls a query that has some) makes the expression a plain evaluation on
  that run and every later one (G12).

A full rebuild happens after `sort()` or `reverse()`, when a captured closure
value changes, and when the same query is called with alternating arguments.

A pipeline whose element function, filter or `key=` function reads an input,
a query or another collection is rebuilt on every run of its query: what it
computed per element can change without its own collection changing. This
holds whether or not the query body read the same thing first; every tracked
read is counted, not only the first of each dependency. Reads made elsewhere
in the body cost the pipeline nothing.

Enforced by: `test_incremental_pipeline.py::test_single_append_runs_map_and_filter_once`,
`::test_lagging_consumer_requests_only_newer_diffs`,
`::test_closure_value_change_rebuilds_state`,
`test_known_defects.py::test_d1_stage_reading_an_already_read_input_is_invalidated`,
`test_rewrite_stage_reads.py` (reads in the element expression, the filter
clause, nested comprehensions and `key=`, of an input, a query and a TTL
query; a pure stage beside a body that reads stays incremental).

### G24. Snapshot semantics for collections

A query run with `snapshot=s` sees each collection's contents as of
`s.revision`. A collection created after the snapshot reads as empty.

A collection keeps its diffs in memory and folds old ones into a base from
time to time. It never folds a diff that a live snapshot, a running call or
an open transaction can still ask for: the log is kept from the oldest head
any pinned revision saw (for a pinned revision from before the collection
existed, from the collection's first head) and from the head an open
transaction started at. Pipelines that lag far behind are rebuilt instead.

Limits: a `Snapshot` built by hand, or one already released, pins nothing.
Reading a collection through it raises `SnapshotExpiredError` once the diffs
it needs are folded, as for inputs (G4).

Enforced by: `test_collection_snapshot_direct_reads.py`,
`test_collection_snapshot_before_creation.py`,
`test_collection_compaction_pins.py` (several snapshots under an older pin,
all three kinds, the public error for an unpinned reader, snapshot readers
against a writer thread, a compaction that falls due inside a mutation
while a pipeline reads the collection),
`test_known_defects.py::test_d12_snapshot_read_survives_compaction_with_an_older_pin`,
`test_history_retention.py::test_collection_snapshot_reads_survive_compaction`,
`::test_collection_log_stays_bounded_under_churn`.

### G25. Transactions

Inside `with engine.transaction():`

- input updates are queued and applied as one revision at exit;
- the last value set for each input wins, inputs ending at their current value
  are skipped, and a block that changes nothing leaves the revision alone;
- collection changes are visible on the collection object at once and are
  undone if the block raises, however many there were and also for a
  collection created in the block;
- queries run inside the block see the engine as it was before the block;
- named collections write to disk once per collection at commit, and not at
  all if the block raises;
- opening a second transaction inside an open one raises `RuntimeError`, even
  for a different engine;
- a transaction object is single-use: entering it a second time raises
  `RuntimeError` and changes nothing;
- a commit checks every input value (the last one set for each input) before
  it applies or writes anything. A value that cannot be encoded, or an
  argument that cannot be hashed, fails the whole commit: the collection
  changes are rolled back as for a block that raised, no input changes, the
  revision stays put, and the original error (`UnencodableValueError`, a
  `TypeError`) propagates.

**Who is inside.** A transaction belongs to whoever entered it: the asyncio
task, or the thread when it was entered outside any task. Only work done
there while the block is open is part of it; a coroutine awaited directly is,
and so is an event loop the block itself runs (`asyncio.run` inside a block
entered outside any task). Everything else is outside, on every Python build
and whether or not it inherited a copy of the block's context: a thread or
task started in the block, `asyncio.to_thread`, `asyncio.gather`, anything
that runs after the block has ended. For code outside:

- `input.set` applies at once, as with no transaction. It is not queued, and
  never dropped;
- it can open a transaction of its own;
- changing a collection that an open transaction has changed raises
  `RuntimeError` and changes nothing. The collection is reserved from the
  transaction's first change to it until the transaction has committed or
  rolled back. Two writers' diffs in one log could not be told apart on
  rollback, and the outside write would publish the uncommitted ones to
  queries. Collections the transaction has not changed are free.

Limits: if the disk write for a later collection fails at commit, earlier
collections are already on disk. Reads of a collection object from outside a query
see uncommitted changes from any thread. `asyncio.gather` inside a block does
not batch the updates its coroutines make: they run as separate tasks.

Enforced by: `test_engine.py::test_engine_transaction`, `test_transaction_collections.py`
(including `::test_transaction_object_cannot_be_entered_twice`),
`test_history_retention.py::test_collection_mutations_in_one_transaction_store_one_version`,
`test_incremental_collections.py::test_mutations_inside_transaction_batch_invalidation`,
`test_known_defects.py::test_d17_rollback_keeps_another_threads_collection_change`,
`::test_d17_task_created_in_a_transaction_can_set_after_it_commits`,
`test_transaction_ownership.py` (plain threads and threads that inherit the
context, tasks during and after the block, `asyncio.to_thread`, two
transactions on one collection, a four-thread run of transactions over a
shared collection),
`test_known_defects.py::test_d12_rollback_undoes_everything_after_compaction_in_the_block`,
`test_collection_compaction_pins.py::test_rollback_of_a_collection_created_in_the_block`,
`::test_rollback_restores_an_existing_collection_under_an_older_pin`,
`::test_rolled_back_named_collection_is_unchanged_in_memory_and_on_disk` (both
backends),
`test_known_defects.py::test_d27_commit_that_fails_on_an_input_applies_nothing`,
`test_transaction_commit_failure.py` (every kind of collection, an unhashable
argument, the error raised, queries inside the block, a collection created in
the block, the collection free for other threads afterwards; named
collections on both backends).

### G26. Named collection persistence

A named collection on an engine with `cache_dir` writes each mutation to disk
before applying it in memory. A new engine restores the same contents, and
compaction preserves them. A value that cannot be encoded raises
`PersistentCacheError` at the mutating call.

`clear_disk_cache()` deletes cached query results only. Collection logs are
data that nothing can recompute, so they stay, and a restart after a clear
restores exactly what each collection held at shutdown. To empty a
collection, call its `clear()`. A change of the result cache's format
keeps them too (G20).

**One writer per collection.** Engines in one process or several may share a
`cache_dir`: they share cached query results, and any of them may open and
read a named collection. The first change an engine makes to a named
collection (any mutating call, `compact()`, or a transaction that changes
it) takes an exclusive OS-level lock on that collection in the directory
(`locks/collection-<hash>.lock`, `fcntl.flock` on POSIX, `msvcrt.locking` on
Windows). The engine holds it until `shutdown()` (or the end of a `with
Engine(...)` block), until the engine is garbage-collected and its writer
has written what was queued (G17), or until its process ends, also by a
crash: the lock belongs to the open file, so a lock file left behind blocks
nobody. A change from another engine while it is held raises
`PersistentCacheError` naming the collection and the directory, and changes
nothing in memory or on disk. An engine whose copy of the collection is
older than what is on disk (another engine changed it after this one loaded
it) is refused the same way when it first changes it, saying the copy is out
of date. Without this, both engines numbered their changes from their own
copy of the log and overwrote each other's (D10).

Limits: writes are committed but not fsynced, so they survive a process kill
but not necessarily a power loss. An engine dropped without `shutdown()`
keeps its locks until it is collected: while anything still references it
(a collection object does), it is not. An engine that only reads a
collection keeps the contents it loaded and does not see later changes
another engine makes. Two collection objects with the same name in one
engine are not told apart by the lock.

Enforced by: `test_incremental_persistence.py::test_named_collections_restore_across_engines`,
`::test_restored_list_keeps_uid_continuity`, `::test_compaction_squashes_log_tail`,
`::test_unserializable_value_raises_persistent_cache_error`,
`::test_clear_disk_cache_keeps_collections`,
`::test_clear_disk_cache_drops_results_but_not_collections`,
`test_transaction_collections.py::test_named_collection_writes_to_disk_only_on_commit`,
`test_known_defects.py::test_d10_two_engines_on_one_named_collection`,
`test_collection_write_lock.py` (both backends: every kind of change refused
and undone, in a transaction too, release at shutdown, an out-of-date copy,
also after compaction, different collections and shared results, queries in
two engines, two threads racing, another process holding the lock, a
crashed holder).

---

# Audit, 2026-11-23

## Defects

"Reproduced" means re-run independently for this audit on Python 3.12.9, and
pinned by a regression test in `tests/test_known_defects.py` (an
expected failure until the defect was fixed). D22 was
found after the audit, while fixing D9; D23 to D26 while fixing D3 and D2;
D27 while fixing D17; D28 while fixing D5; D29 to D31 while fixing D7; D32
while fixing D30. All
of those were already present at the audit commit. D33 and D34 were found
on 2026-11-24, while writing tests for what mutation testing showed; D33
came with the fix for D24.
"Reported" meant found and probed during the audit but not re-run a second
time; nothing is in that state now.

There are no open defects: every defect found by the audit or since is
fixed and listed below. The tables of open wrong results, data loss and
hangs are empty, and every test in `tests/test_known_defects.py` passes
without an expected-failure marker.

### Fixed since the audit

| ID | Defect | Fixed by |
|---|---|---|
| D15 | Results were lost when a process ended without `shutdown()` or `flush_disk()` (1224 of 2000 on mdbx after a normal exit). | `fix: results were lost when a script ended without shutdown()` |
| D13 | Reusing an `EngineTransaction` object replayed its old state and could roll back committed data. Re-entry now raises `RuntimeError`. | `fix: a reused transaction object could undo committed changes` |
| D14 | `CascadeSet.update` and `difference_update` kept the part applied before a failure in storage but not in the log, so queries never saw it. Also found and fixed: list `pop`, `insert`, `xs[i] = v` and `del xs[i]` with an `__index__` object changed the list and then raised before logging. | `fix: a failed set update left elements that queries never saw` |
| D4 | `compute_many`, `compute_many_stream` or `submit` called inside a query recorded no dependency, so the parent never invalidated. The first two now record each requested query; `submit` inside a query raises `RuntimeError`. | `fix: queries run through compute_many inside a query were not tracked` |
| D9 | Functions registered under one `module:qualname` shared a cache (`make(1)()` and `make(2)()` both returned 1), and a handle to a replaced function kept running old code into it. A different function now replaces the old one, whose handles raise `StaleHandleError`; `name=` on `engine.query` and `engine.input` gives factories and lambdas distinct identities. | `fix: two functions registered under one name shared cached results` |
| D8 | A builtin shadowed in an enclosing function was still rewritten (an outer `sum = lambda it: "shadowed"` was ignored). The rewriter now declines when a relevant name is a closure variable. | `fix: a builtin shadowed in an enclosing function was still rewritten` |
| D11 | `clear_disk_cache()` wiped the disk log of live named collections, so `[1, 2, 3]`, cleared, then `append(4)`, restored as `[4]`. It now deletes cached results only and leaves collection logs alone. | `fix: clearing the disk cache deleted named collections' data` |
| D3 | An expired TTL query was not refreshed when reached through a dependent already verified at the current revision (`parent()` returned 10 before and after expiry). Two causes: a result verified at a revision was trusted at that revision for ever, and a re-run that changed the value at an unchanged revision looked unchanged to dependents. Results now carry the expiry of the TTL results beneath them, and a query's change marker is a stamp instead of the revision. | `fix: an expired TTL result was still served through queries that depend on it` |
| D2 | A `fixed_point=` query ignored `snapshot=` and returned the head value (7 where the snapshot value is 1), and one read moved `engine.revision` from 3 to 6: every iteration ran at a new revision. The cycle is now iterated at the caller's revision, with intermediate results kept out of the shared cache. Found and fixed with it: a change to an input read only by another member of the cycle did not reach the head (`a` stayed 1 where a fresh run gives 21); a stale result read before the cycle was entered ended in `RecursionError`; a cycle that never converges looped for ever and now raises `CycleError` after `max_iterations` runs (default 1000). | `fix: a fixed-point query ignored snapshots and moved the revision` |
| D22 | Re-registering a query with changed code while `engine.revision` had not moved since its dependents were computed left those dependents serving old results (`top()` stayed 2 where a fresh run gives 11). The dependents counted as verified at that revision and were not looked at again; an input change in between hid it. The same held for an input registered again with a changed default and read through a snapshot, and for dependents cut off from the function by an evicted result. Registration with changed code now makes every cached result verify again. | `fix: results computed from a query's old code survived its re-registration` |
| D12 | When a pinned snapshot predated a collection, in-memory compaction ignored later snapshots and open transactions: a snapshot read raised the private `_HistoryCompacted` after 3000 appends, and a rolled-back transaction left 2048 of its 3000 appended items in place. With no head recorded at the oldest pinned revision, compaction took the newest head as its limit. The limit is now the first head the collection reported, and never past the head an open transaction started at. A reader nothing pins gets `SnapshotExpiredError`. | `fix: collection log compaction dropped diffs that snapshots and transactions needed` |
| D17 | The open transaction leaked across concurrency boundaries. Another thread appending to a collection during the block had its change deleted on rollback, and published the block's uncommitted changes to queries. A task created in the block that called `set()` after the block had ended put the update on the dead transaction's queue, where it was dropped (`set(5)` left the input at 1); a task or inheriting thread running during the block had its updates queued into a transaction it knew nothing about. A transaction now belongs to the task, or else the thread, that entered it. Other code is outside it: its `set` applies at once, and changing a collection the transaction has changed raises `RuntimeError`. | `fix: a transaction leaked into other threads and tasks` |
| D16 | A format-version change wiped collection logs along with cached results (`[1, 2, 3]` restored as `[]`, with no error, on both backends). Collection logs now have a format number of their own, `COLLECTION_FORMAT`; a `DISK_FORMAT` change deletes cached results only. Collection data that cannot be read is refused with `PersistentCacheError` instead of deleted. | `fix: a disk format change deleted named collections' data` |
| D6 | Early bail-out treated dicts that differ only in insertion order as equal, so a dependent that iterated the dict kept the old order (`['a', 'b']` where a fresh run gives `['b', 'a']`). The same digest made `input.set` of a reordered dict a no-op, and the disk encoding sorted dict keys, so a stored dict came back in sorted order. Both digests and the disk encoding now keep insertion order; sets are unchanged. `DISK_FORMAT` went from 2 to 3: results stored under 2 are dropped on open, collections are kept. | `fix: a result that only changed a dict's order did not reach its dependents` |
| D1 | A rewritten stage that read an input the query body had already read was not seen as reading anything, so its cached per-element results were kept: `f = factor(); return sum(x * factor() for x in xs)` over `[1, 2, 3]` returned 12 after `factor.set(10)` where a fresh run gives 60. The pipeline looked for new dependencies of the query, and a repeated read adds none. Every tracked read is now counted, and a pipeline whose stages read anything is rebuilt on each run. | `fix: a rewritten comprehension kept old results after an input it read changed` |
| D5 | Collection reads that bypassed tracking went stale inside queries: `s \| x` and the other set operators and methods, set and dict `==`, list `<`, `xs + [...]`, `xs * n`, `repr`, `set.copy()`. `sorted(s \| {99})` stayed `[1, 99]` after `s.add(2)`. The builtins' implementations ran on the subclass's storage without any method of the collection being called, so no dependency was recorded and a snapshot read saw live contents. Each of these is now a tracked read on the collection. Found and fixed with it: `d.keys() == {'a'}` was always `False` and set operations on dict views raised; `copy.copy(xs)` returned a second collection object sharing the first one's log. What CPython reads without calling the subclass (`set(s)` outside a rewritten body, `frozenset(s)`, `plain.union(s)`, `fs \| s`) cannot be intercepted and is a documented limit (G21). | `fix: set operators, comparisons and repr of a collection went stale inside queries` |
| D7 | Rewritten reducers differed from the builtins. Float `sum([0.1] * 10)` gave `0.9999999999999999` against `1.0`, and drifted further with each removal. `sorted(..., reverse=True)` reversed equal elements (`[(1,'b'), (1,'a')]` against `[(1,'a'), (1,'b')]`), and any `sorted` did after an insert that was not at the end. `any` and `all` evaluated every element (`any(1 / z for z in [1, 0])` raised `ZeroDivisionError`). `min` and `max` returned the wrong one of several equal elements (`max([1, True])` gave `True`), and with NaN present did not follow the builtin's scan (`min([2.0, nan, 1.0])` gave `2.0` against `1.0`). `len(map(...))` returned a count instead of raising `TypeError`, over plain lists too. Also found and fixed: of two elements that raise, the one whose change arrived first was reported, not the first in the collection; a `set` or `dict` built from equal values of different types kept the wrong one; the function expression of `map`/`filter` was evaluated after the source. Each reducer now either maintains a result that is exact by construction or applies the builtin to the stored values, and anything that raises is settled by plain evaluation (G22). | `fix: rewritten sum, sorted, min, max, any, all and len differed from the builtins` |
| D23 | A stored result of an `async def` query could not be loaded by a new engine: the call raised `TypeError: entry_from_runtime() missing ... 'computed_at_time'` instead of returning the value. The async loading path now passes the stored computation time, so a loaded result also expires with its TTL as on the sync path. | `fix: an async query's stored result could not be loaded by a new engine` |
| D18 | If the disk writer thread hit an error other than `PersistentCacheError` (for example sqlite "database is locked"), the thread died and `flush_disk()` and `shutdown()` blocked for ever; a `PersistentCacheError` (map full) was dropped without a word. The writer now survives every error and goes on with the next batch, and the next `flush_disk()` or `shutdown()` raises `PersistentCacheError` with the original error as its cause. Also fixed: the `disk_store` counter and trace were recorded for writes that failed. | `fix: a disk write error stopped the writer and hung flush_disk() and shutdown()` |
| D10 | Two engines on one `cache_dir` that both changed the same named collection overwrote each other's revision numbers (after each appended once, a restart restored only the second one's item), and a result computed from one engine's contents could verify against the other's. The first engine to change a named collection now holds an OS-level lock on it until shutdown or process exit; a change from any other engine, or from an engine whose copy is out of date, raises `PersistentCacheError` and changes nothing. Sharing a `cache_dir` for cached results and reading collections from several engines still works. | `fix: two engines changing one named collection overwrote each other's changes` |
| D20 | A query result that could not be encoded raised a bare `TypeError` from the codec, even with no `cache_dir`, naming the unsupported type but not the query; an exception with unencodable `args` was replaced by that `TypeError`. A result now raises `UnencodableValueError` (a `TypeError`) naming the query, the path to the bad part and its type, and is not cached; an exception that cannot be encoded reaches the caller as itself, uncached. Input values and `fixed_point=` guesses get the same error. | `fix: an unencodable result raised an error naming no query and hid the query's own exception` |
| D19 | A direct caller that joined a `submit`-owned run of the same query received the owner's `QueryCancelled`, though only `submit` cancels. A caller whose run ends in another call's `QueryCancelled` now checks its own cancellation and, if it is not cancelled, runs the query itself at its own revision. | `fix: a direct call that joined a submitted run was cancelled with it` |
| D21 | A sync waiter on the event-loop thread, waiting for an async run of the same query on that loop, deadlocked; reachable through the public API when a sync query that depends on an async query was called on the loop thread while that async query ran there. Also: a sync query body calling an async query on the loop thread got a coroutine back and cached the resulting `TypeError`, and a stale async dependency recomputed there cached `asyncio.run`'s `RuntimeError`. In-flight runs now record their owner (thread or task); a synchronous wait for a task on the caller's own thread is never started, and synchronous code on a loop thread raises `EventLoopBlockedError` where it would have to run or wait for an async query. | `fix: a sync call on the event loop's thread hung waiting for an async query` |
| D24 | Two threads that entered one dependency cycle through different queries waited for each other for ever: each ran one query and asked for the other. With `fixed_point=` neither got a value; without it neither got a `CycleError`. In-flight runs now record their owner, blocked threads and awaiting tasks record the run they wait for, and a caller whose wait would close a loop runs the query on its own stack instead, where the cycle is handled as on one thread. A caller whose joined run ended in a `CycleError` (or the internal re-entry signal of a verification) runs the query itself. Found and fixed with it: once threads had read one fixed-point cycle at different revisions, the stored results could depend on each other in a loop, and verifying them recursed until `RecursionError`, which was then cached as a query's result; a verification walk that reaches a query it is already bringing up to date now counts it as changed. | `fix: two threads entering one cycle through different queries waited for each other for ever` |
| D27 | A commit that failed while applying its input updates was applied in part. With `xs.append(2)` and `x.set(object())` in one block, the exit raised `TypeError`, `list(xs)` was `[1, 2]`, and a query still read `len(xs) == 1`: the collection change was kept (for a named collection, also on disk) but its new head was never published. A commit now hashes every input value before it writes or applies anything; if one fails, the collections are rolled back and the original error propagates. | `fix: a transaction commit that failed on an input value kept its collection changes` |
| D25 | After the cached result of a fixed-point cycle member was dropped (LRU eviction, `prune`, `sweep_unaccessed`) and an input changed, a query that depended on that member raised `CycleError` where the same call with the member still cached returned 50. The member was recomputed on its own, entered the cycle from the wrong end and reached itself before the head. A member whose run reaches itself through a fixed-point head that has a result (in memory or on disk), or through the head it was last stored with, now starts again by bringing that head up to date. Found and fixed with it: the same `CycleError` for a member never stored in a session whose head was loaded from disk. | `fix: a query depending on a dropped fixed-point cycle member raised CycleError` |
| D26 | Accumulator effects pushed in the abandoned runs of a fixed-point cycle reached every caller above the cycle head: with `fixed_point=0`, a head that pushed once per run and needed four runs handed its caller, and `effects=`, four copies, while the head's own cached result held one. The pushes of a run that is abandoned are now taken back from every frame above the head and from the root's `effects=`; only the converged run's effects are seen, also on cache hits and from disk (G12). | `fix: effects pushed in abandoned fixed-point runs reached the callers above the head` |
| D31 | Accumulator effects pushed inside the element function of a rewritten comprehension were not replayed for elements the pipeline already held: with `sum(note(x) for x in xs)`, after `xs.append(4)` the re-run collected `[4]` where Python collects `[1, 2, 3, 4]`; with `any` the first run pushed for every element where Python stops at the first truthy one. The pipeline now notices at run time when anything it runs pushes or replays an effect, takes those pushes back and evaluates the expression plainly, on that run and every later one (G12). | `fix: effects pushed inside a rewritten comprehension were lost for elements that did not change` |
| D30 | An assignment expression inside a rewritten comprehension was lost. The element expression was moved into a helper function, where `:=` bound a local of that helper: `last = None; out = [(last := x) for x in xs]; return last` returned `None` where Python returns the last element. A comprehension with `:=` anywhere in its element or `if` clauses, nested comprehensions included, is now left as written (G22). | `fix: an assignment expression inside a rewritten comprehension was lost` |
| D32 | A pipeline nested in a comprehension that is not rewritten (two `for` clauses, or, after the D30 fix, a `:=`) had its helper placed before the statement, outside that comprehension, so it could not see the comprehension's loop variable: `[sum(y * x for y in ys) for x in xs for _ in (0,)]` raised `NameError`, and with a variable `x = 100` in the query returned `[3000, 3000]` where Python gives `[30, 60]`. Found while fixing D30, whose fix sends more comprehensions down this path; present at the audit commit. Such a pipeline is now left as written (G22). | `fix: an assignment expression inside a rewritten comprehension was lost` |
| D29 | A `CascadeSet` was visited in two different orders. A rewritten comprehension or reducer saw insertion order (`[x for x in s]` over `CascadeSet([30, 1, 20])` gave `[30, 1, 20]`); a `for` loop, a comprehension in a helper function, a query with `incremental=False` and `list(s)` saw the builtin set's order (`[1, 20, 30]`), so the same body gave two results depending on whether it was rewritten. A `CascadeSet` now iterates in insertion order everywhere: it keeps that order beside its storage, its `__iter__` and `repr` follow it, and snapshot reads keep the order they had (G21). | `fix: a CascadeSet iterated in another order when the query was not rewritten` |
| D28 | C code that changed the storage of a list, set or dict subclass directly bypassed the collection: `heapq.heappush(xs, 1)` on a `CascadeList` logged no diff, left `len(xs)` inside a query at its old value and the list's element ids out of step with its contents. It cannot be intercepted. The collection now records its size at every change it makes; the next use of it (a read, a mutation, a rewritten pipeline) that finds another size undoes the change and raises `CollectionBypassedError`. Changes that keep the size (`heapq.heapify`, `heapreplace`, `heappushpop`, by-hand `list.sort`, `list.reverse`, item assignment) are not noticed; that limit is documented and pinned (G21). | `fix: C code such as heapq.heappush changed a collection without a trace` |
| D33 | The body of a query ran twice at one revision, on two threads, with no cycle anywhere, and the trace showed `dedup_cycle` (G5). Thread B ran `total()`, which needs `part()`, and waited for thread A's run of `part()`; A finished `part()` and asked for `total()`. B had its answer but was still recorded as waiting for A, because a waiter removed its record itself, after it had taken the engine's lock again. A took the record for two threads waiting for each other (D24) and ran `total()` on its own stack: 300 of 300 trials with nothing patched. Both runs returned the right value; with `fixed_point=`, both threads iterated the fixed point. A run that is released now drops the records of the callers that waited for it. | `fix: a query ran twice, and a cancelled waiter failed the run it waited for` |
| D34 | Cancelling one task that waited for a run of an async query failed the run's owner and its other waiters. With three tasks asking for one query, one running it and two waiting, and one waiter cancelled (`task.cancel()`, or `asyncio.wait_for` running out), the task that ran it got `concurrent.futures.InvalidStateError` where 42 was due and the other waiter got `CancelledError`. A thread that calls an async query from sync code runs it in a task of its own: a sync query `outer()` that calls async `slow()` raised `InvalidStateError` when a task elsewhere had waited for that run and been cancelled, and the error was cached, so later calls raised it again without running anything. The callers that join a run share one future, and a task that stopped waiting for it cancelled it for all of them. A task now waits on a future of its own, which the shared one settles; one that has stopped waiting, or whose loop is closed, is left alone. | `fix: a query ran twice, and a cancelled waiter failed the run it waited for` |

Smaller items, fixed: the first read of a never-set input bumped
`engine.revision`. It now keeps the default at the current revision without
moving it (G3).

## Limits that restrict who can use the library

The library's aims, taken from the README: memoise ordinary Python functions
with automatic dependency tracking; make ordinary comprehensions incremental;
persist results across runs with no configuration; run in parallel and async.
The limits below worked against those aims for common uses. Each was
checked by running it. Value types were lifted: the standard value types are
encoded natively, other classes through `register_type`, and an engine
without `cache_dir` keeps results it cannot encode (G1, G18).

| Limit | Who it stops | Why it conflicts with the aim |
|---|---|---|
| **Query identity.** A query is its `module:qualname` unless `name=` is passed. Factories and lambdas must pass `name=`; without it each registration replaces the last and the earlier handle raises `StaleHandleError`. After a reload, callers must pick up the new handle. (Methods can now be queries, with `self` as an argument: G1.) | Libraries that generate queries (one extra argument). | No longer a silent wrong answer inside a process. Across runs, closures of one body under one name can still share persisted results (G11). |
| **Persistence across code edits needs a declared version.** Code invalidation sees only the query's own bytecode. `version=` per query and `cache_version=` per engine now force invalidation (G11), but an edit to a helper, a global or a default that no version declares still leaves the old result in place. (Results are written at a normal exit; a killed process still loses what was queued.) | Anyone using `cache_dir` as a build cache who forgets to bump a version after editing a helper. | "Zero-config persistence" implies it works without the user knowing about it; it now works with one declared number. |

Limits that narrow use without stopping it:

- **Thread fan-out inside a query goes through `compute_many`.**
  `compute_many` and `compute_many_stream` track dependencies there;
  `submit` raises. `asyncio.gather` over async queries also tracks
  dependencies.
- **One writer per named collection.** Engines and processes may share a
  `cache_dir`, but only one at a time may change a given named collection
  (G26); with sqlite one can find the database locked by another (reported
  by `flush_disk()`, G17). Parallel CI jobs and
  `pytest-xdist` workers need separate directories.
- **Only `submit` cancels.** An editor-style tool that calls queries directly
  or through `compute_many` cannot abandon stale work.
- **A dependency cycle has one way in.** Only the query with `fixed_point=`
  can be reached again, so a cycle has to be entered through it to get its
  value; a caller entering through another member may get `CycleError`
  (G7). Once the head has a result, any member can be called.
- **Arguments must be hashable** (G1), so a list or dict is passed as a
  tuple or a frozen dataclass.
- **Dict order counts.** A result or input value whose dict order varies
  without meaning defeats early bail-out and reuse across runs (G2).
- **Returned values are shared with the cache.**
- **Alternating snapshot and head reads of one query recompute each time**,
  because one result is kept per query and arguments.

Limits that fit the aims as they are: digest-based equality, same-value `set`
as a no-op, snapshot expiry after release, the error-caching rules,
`memoize=False`, eviction, and the cache directory being trusted input.

## Formal model

`docs/formal/CascadeCore.tla` models two inputs, a mode switch and two
queries, one above the other, with two concurrent callers, writers and
evictions. A write changes an input and nothing else; the callers bring the
cached rows up to date by lazy re-verification, one critical section of the
store lock per step (look up, verify, mark green or claim and run, store,
release), each at the newest revision or at an older one, as through a
snapshot. TLC checks every interleaving: eight invariants for two callers
and two writes (26 million states), and minimal re-execution on top of them
for one caller and five writes. CI runs the second, which takes seconds, on
every push; the first takes minutes and is run on demand, locally or through
`.github/workflows/model-check.yml`. `docs/formal/README.md` maps the
actions to the code and the invariants to the guarantees.

- Covered: G1 and G4 (a call returns what a fresh run at its revision
  would), the `head_clean` fast path and dirty propagation, change stamps
  and the early bail-out (G2), in-flight deduplication (G5), eviction
  (G13), loading a persisted record (G15, G16).
- The invariants were tested: thirteen bugs seeded into the model one at a
  time each violate one of them. One is the race
  `test_snapshot_verification_race.py` guards, a dependent verified against
  the stored row instead of the entry its nested call returned; TLC needs
  both callers and 23 steps to reach it.
- Not modelled: cancellation of running work, errors, TTL, re-registered
  code, fixed points, effects, transactions, collections, compaction and
  history retention. Change stamps are reused once nobody holds them, where
  the code counts up; the outcome of a walk over input dependencies is fixed
  in the step that looks the row up.
- None of D1–D34 is reachable in the model: each is in a feature it leaves
  out (for D33 the record of who waits for whom, for D34 tasks and their
  cancellation). The state machine (test gap 3) is what covers the others;
  it has no threads or tasks, so neither of these two.

## Test gaps

Ordered by the risk they leave open.

1. **Every defect has a regression test, and none is open.**
   `tests/test_known_defects.py` has a test for each defect (D1-D34), each
   asserting the correct behaviour; a defect found from now on gets one
   under `xfail(strict=True)` until it is fixed.
2. **Durability is tested for normal exits only.** `test_durability_at_exit.py`
   exits child processes without `shutdown()` and with `os._exit`.
   `test_disk_writer_errors.py` injects write failures into a live engine.
   `test_collection_write_lock.py` puts two engines, and two processes, on
   one `cache_dir`, but only around named collections; nothing runs two
   processes writing results to one directory under load.
3. **The Hypothesis state machine is sequential.** It keeps a plain-Python
   model (input values, a list, a clock, the revision) and compares every
   read with a plain function of it: inputs, a dynamic dependency,
   snapshots, a TTL query and a dependent, re-registered code, a fixed
   point, a collection under transactions that commit or roll back,
   compaction while snapshots are held, and a new engine on the same
   directory, in memory and on both backends, 100 examples of 40 steps
   each. Thirteen faults seeded into the engine one at a time (faults of
   the kind of D2, D3, D12 and D22, a rollback that undoes nothing, a
   persisted result loaded without comparing its dependencies, and so on)
   each fail it. It has no threads, errors, eviction, async queries,
   accumulator effects or `compute_many`, one collection (a list), and one
   writer.
4. **Rewrite equivalence is tested per reducer, not per program.**
   `test_rewrite_differential.py` compares each rewritten reducer with the
   builtin over ints, floats, strings, tuples and mixtures. It does not
   generate query bodies, so interactions between several pipelines in one
   body, nested comprehensions and closures over changing values rest on
   example tests.
5. **No cross-process or cross-version determinism test for the codecs.**
   `test_codec_properties.py` checks round trips and determinism within one
   process with Hypothesis.
6. **Backend parity is partial.** Five tests in `test_disk_cache.py` skip
   sqlite; `test_incremental_persistence.py`, `test_code_versioning.py` and
   `test_error_caching_persistence.py` run mdbx only; `test_transaction_collections.py`
   runs sqlite only. The sqlite vacuum delete paths are excluded from coverage.
7. **Async is thinly tested** and its internals are excluded from coverage:
   no async tests for effects, bail-out counts or eviction, and snapshots,
   TTL and fixed points have one or two each.
8. **Collection concurrency has one deterministic test.** No multi-writer or
   writer-against-pipeline stress, and none on free-threaded builds. One
   more since: a writer whose mutation makes a compaction fall due while a
   pipeline reads the collection (`test_collection_compaction_pins.py`).
9. **Transactions inside the block are untested** for queries reading a
   mutated or newly created collection, `submit` and two engines. Threads and
   async tasks are covered by `test_transaction_ownership.py`.
10. **One in five mutants survives, and nothing gates on it.**
    `scripts/mutation.py` runs mutmut (11,282 mutants on covered lines) and
    reports the survival rate per module with the diffs of the survivors.
    The first full run (2026-11-24) left 2,340 alive, 20.8%: from 41% in
    `_serde` and 27% in `_evaluator` down to 9% in `_collections`. Tests
    written for what it showed, and a second run over those two modules,
    brought that to 2,281, 20.2%. So the 95% line coverage figure
    overstates what the assertions would catch, most in the async paths,
    in the bookkeeping of a result reloaded from disk, and in two paths
    that tests reached only when threads happened to meet. Fixed tests
    force those two since, and writing them found D33 and D34.
    `docs/verification-status.md` has the tables, how the runs were made
    and what in them is unconfirmed. A full run is hours of test runs, so
    it is done on demand, on a machine with many cores (on Windows through
    `scripts/mutation_windows.py`) or through
    `.github/workflows/mutation.yml`.
