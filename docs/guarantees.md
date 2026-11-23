# Cascade guarantees

This page states what Cascade promises, where each promise stops, and which
tests enforce it. It is the reference the tests, the formal model and the
README are checked against.

The second half is an audit dated 2026-11-23 (commit `03c7ed7`): defects where
the code does not keep a promise, and guarantees with no test behind them.
Every open defect has a failing regression test in
`tests/test_known_defects.py`; defects fixed since the audit are listed under
"Fixed since the audit".

**Status key**

| Status | Meaning |
|---|---|
| Holds | No counterexample known; tests enforce it |
| Holds with limits | True inside the stated limits; the limits are by design |
| Broken | A reproduced counterexample exists; see the defect ID |

## Summary

| # | Guarantee | Status | Test strength |
|---|---|---|---|
| G1 | A query returns what a fresh run at that revision would return | Broken (D1–D3, D5–D7) | Strong for pure integer graphs |
| G2 | Unchanged dependencies mean the body does not run; equal results stop propagation | Holds with limits | Strong for run counts |
| G3 | Setting an input to its current value is a no-op | Holds with limits | Strong |
| G4 | A snapshot read sees inputs as of the snapshot's revision | Broken for fixed-point queries (D2) | Strong otherwise |
| G5 | Concurrent requests for one query at one revision run the body once | Holds with limits | Strong |
| G6 | A submitted query whose inputs change raises `QueryCancelled` and stores nothing | Holds with limits (D19) | Medium |
| G7 | Cycles raise `CycleError`; `fixed_point=` iterates to a stable value | Broken with snapshots (D2) | Weak for fixed point |
| G8 | Cached errors are re-raised without re-running; control-flow errors are never cached | Holds with limits (D20) | Strong |
| G9 | A TTL query recomputes once its TTL has elapsed | Broken through dependents (D3) | Weak |
| G10 | `memoize=False` queries track dependencies but keep no value | Holds | Medium |
| G11 | Re-registering a query with changed code drops its cached results | Holds with limits | Positive cases only |
| G12 | Accumulator effects are replayed on cache hits, in a fixed order | Holds | Good |
| G13 | Eviction, `prune` and `sweep_unaccessed` change cost, never results | Holds | Strong for eviction and prune |
| G14 | Async queries behave like sync queries | Holds with limits (D21) | Weak |
| G15 | A new engine on the same `cache_dir` reuses results whose inputs match | Holds with limits | Strong on the sync path |
| G16 | A stale persisted result is never served | Holds with limits (G11's blind spots) | Medium |
| G17 | A finished result is on disk once the process exits normally | Holds with limits | Good for normal exits |
| G18 | Values round-trip through the disk codec with their types | Holds with limits | Good by example |
| G19 | Equal values of the same type encode to the same bytes | Holds | Weak |
| G20 | A corrupt or old-format cache entry is recomputed, not served | Holds for query results; broken for collections (D16) | Medium |
| G21 | Collections behave like `list`, `set` and `dict` | Holds with limits (D5) | Strong for mutators |
| G22 | A rewritten query returns what the plain Python would | Broken (D1, D7) | Weak on edge cases |
| G23 | One mutation costs one diff of work | Holds with limits | Adequate |
| G24 | A snapshot read sees collection contents as of the snapshot | Broken (D12) | Strong for one snapshot |
| G25 | A transaction applies completely or not at all | Broken (D12, D17) | Strong for inputs only |
| G26 | A named collection is restored with the same contents after a restart | Broken (D10, D16) | Good on the happy path |

## Core evaluation

### G1. From-scratch equivalence

Calling a query returns the value its function would return if run fresh
against the inputs and collections at the caller's revision.

How: each cached result records its dependencies and the revision at which
each was last seen to change. A result is reused only after every recorded
dependency is confirmed unchanged at the caller's revision.

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
  objects (a module reloaded unchanged), keeps results and handles. Methods
  cannot be queries.
- **Values must be encodable.** Results and input values are hashed through the
  canonical encoder even without `cache_dir`; an unsupported type raises
  `TypeError`. G18 lists the supported types.
- **Arguments are positional and hashable.** Keyword arguments raise
  `TypeError`. Arguments that compare equal are one cache key, so `q(1)`,
  `q(True)` and `q(1.0)` share a result.

Enforced by: `test_dirty_propagation.py::test_random_operations_match_plain_recomputation`
(seeded differential test against an independent model, 16 parametrisations),
`test_stateful_engine_invariants.py` (Hypothesis state machine),
`test_dirty_propagation.py::test_concurrent_writers_and_readers_only_see_real_states`,
`test_fan_out_inside_queries.py` (dependencies, snapshots, effects, errors and
cycles through `compute_many` and `compute_many_stream` inside a query;
`submit` refused), `test_query_identity.py` (replaced functions, stale
handles, `name=`).

### G2. Minimal re-execution and early bail-out

If no dependency changed, the body does not run. If a body re-runs and
produces an equal result, its dependents do not re-run.

"Equal" means equal content digest, not `==`. The digest is type-sensitive
(`1`, `1.0` and `True` differ) and ignores dict and set ordering (see D6).

Enforced by: `test_dirty_propagation.py::test_backdated_middle_result_stops_recompute_but_stays_correct`,
`::test_unrelated_input_change_needs_no_walk_or_recompute`,
`test_engine.py::test_smart_recalculation_and_selective_updates`,
`test_scale_behavior.py::test_giant_graph_selective_invalidation_counts`,
`test_iterative_verification.py::test_verification_stops_at_first_changed_dependency`.

### G3. Same-value `set` is a no-op

`input.set(v)` where `v` has the same digest as the current value leaves the
revision alone, invalidates nothing and cancels nothing. Inside a transaction
the same rule applies per input after last-write-wins.

Limits: "same" is digest equality, so `set(1)` then `set(1.0)` is a change.
The first read of an input that was never set stores its default and bumps
the revision.

Enforced by: `test_history_retention.py::test_same_value_set_is_a_full_noop`,
`::test_transaction_with_only_unchanged_values_does_not_bump`,
`::test_transaction_bumps_once_and_skips_unchanged_inputs`,
`::test_transaction_that_returns_to_the_original_value_is_a_noop`,
`test_dirty_propagation.py::test_same_value_set_keeps_everything_clean`.

### G4. Snapshot isolation for inputs

`query(snapshot=s)` sees every input as of `s.revision`, whatever is written
afterwards. Input history is kept while any snapshot or running call needs it.
Reading through a released snapshot raises `SnapshotExpiredError` once the
history has been trimmed.

Limits: a `Snapshot` built by hand from a revision number pins nothing. An old
snapshot read replaces the head's cached row, which is correct but costs a
recompute on the next head read.

Enforced by: `test_engine.py::test_snapshot_isolation_mvcc`,
`test_history_retention.py::test_held_snapshot_keeps_history_until_released`,
`::test_hand_built_snapshot_below_the_floor_raises`,
`::test_concurrent_snapshots_and_writers_never_expire_live_reads`,
`test_dirty_propagation.py::test_pinned_snapshot_and_head_reads_interleave_correctly`,
`test_snapshot_verification_race.py::test_old_snapshot_read_ignores_rows_replaced_during_verification`,
`test_engine_concurrency.py::test_snapshot_reads_stable_during_concurrent_writes`.

### G5. Concurrent demand runs the body once

When several threads request the same query with equal arguments at the same
revision, one runs the body and the others wait for its result, effects and
exception.

The body can run more than once when callers are at different revisions, when
the query is `memoize=False`, when the first run raised an error that is not
cached, or when the cached row was evicted in between.

Enforced by: `test_engine_concurrency.py::test_query_dedup_concurrent_requests`,
`::test_query_dedup_heavy_contention_stress`,
`::test_query_dedup_failure_propagates_and_recovers`,
`::test_in_flight_dedup_does_not_cross_snapshot_boundaries`,
`test_dependency_parallelism.py::test_compute_many_layered_dependencies_run_once_per_key`,
`::test_result_stored_between_cache_miss_and_claim_is_reused`.

### G6. Cancellation

A query started with `engine.submit` raises `QueryCancelled` if an input
changes value before it finishes. Its result is discarded and never cached.
Dependencies that already finished keep their results.

Limits: only `submit` cancels. Direct calls, `compute_many` and
`compute_many_stream` always run to completion. A same-value `set` cancels
nothing.

Enforced by: `test_engine_concurrency.py::test_safe_task_cancellation_after_input_mutation`,
`::test_cancellation_race_stress`, `test_async_coverage.py::test_async_query_cancelled`,
`test_scale_behavior.py::test_concurrency_stress_submit_compute_many_and_writes`.

### G7. Cycles and fixed points

A query that reaches itself raises `CycleError` naming the path. With
`fixed_point=v`, the re-entrant call returns the current guess and the cycle
re-runs until the result stops changing. There is no iteration cap.

Enforced by: `test_engine.py::test_cycle_detection`,
`::test_long_chain_cycle_detection_behavior`, `test_fixed_point.py::test_fixed_point_basic`,
`::test_fixed_point_no_default_raises`, `test_async_coverage.py::test_async_cycle`.

### G8. Error caching

With the default `cache_exceptions=True`, an `Exception` raised by a query is
cached and re-raised on later calls without re-running the body. A tuple
restricts caching to those types. `QueryCancelled`, `CycleError`,
`PersistentCacheError` and `SnapshotExpiredError` are never cached. A caller
that catches a dependency's error still depends on it.

Limits: the same exception object is re-raised each time, so its traceback
grows. `BaseException` subclasses outside `Exception` are not cached.

Enforced by: `test_error_caching.py::test_error_is_cached`,
`::test_error_cache_invalidation`, `::test_selective_exception_caching`,
`::test_caught_cached_error_does_not_escape_on_reverification`,
`::test_caught_uncached_error_still_tracks_dependencies`,
`::test_uncached_error_two_levels_down_reaches_the_handler`.

### G9. TTL

A `ttl=` query called directly after its TTL has elapsed re-runs. If the new
result is equal, dependents do not re-run.

Enforced by: `test_ttl.py::test_ttl_invalidation_basic`,
`::test_ttl_invalidation_async`,
`test_dirty_propagation.py::test_ttl_dependency_keeps_dependents_on_the_slow_path`.

### G10. `memoize=False`

The query keeps its digest, dependencies and effects but no value. Every call
that needs the value re-runs the body; verification alone does not. Nothing is
written to disk for it.

Enforced by: `test_memoize_false.py::test_memoize_false`,
`::test_memoize_false_multiple_calls_same_revision`.

### G11. Code-aware invalidation

Registering a query or input whose bytecode hash differs from the one cached
drops its results, in memory and on disk, and marks dependents for
re-verification. This happens at registration, not while the old function is
still registered. Handles to the replaced function go stale and raise
`StaleHandleError` when used (see G1, identity).

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

Enforced by: `test_code_versioning.py::test_memory_cache_invalidation_on_code_change`,
`::test_disk_cache_invalidation_on_code_change`,
`::test_input_cache_invalidation_on_code_change`,
`test_dirty_propagation.py::test_reregistering_a_query_dirties_its_dependents`,
`test_query_identity.py::test_reload_with_changed_code_makes_old_handles_stale`,
`::test_reload_with_unchanged_code_keeps_results_and_handles`,
`::test_replacing_function_does_not_load_the_old_ones_results_from_disk`.

### G12. Accumulator effects

Effects pushed during a query are stored with its result and replayed on every
cache hit, to every waiting caller, in execution order. A query that fails
leaks no partial effects. `compute_many(effects=)` merges per-call effects in
call order.

Enforced by: `test_engine.py::test_side_effect_replay_on_cache_hits`,
`::test_nested_side_effect_replay_propagates_to_parent_effects`,
`::test_failed_query_does_not_leak_partial_accumulator_effects`,
`test_engine_compute_many.py::test_compute_many_can_collect_accumulator_effects`,
`test_engine_concurrency.py::test_query_dedup_replays_effects_to_all_concurrent_callers`.

### G13. Eviction, prune and sweep

Dropping a cached result (LRU at `max_entries`, `prune`, `sweep_unaccessed`)
never changes what a later call returns; it only makes that call recompute.

Enforced by: `test_dirty_propagation.py::test_lru_eviction_never_serves_stale_results`,
`::test_dropping_a_middle_memo_dirties_its_dependents`,
`test_scale_behavior.py::test_prune_stress_keeps_consistent_reachable_subgraph`,
`test_iterative_verification.py::test_deep_chain_with_evicted_middle_memo`.

### G14. Async parity

`async def` queries follow G1–G13. Sync queries may call async queries and the
reverse.

Limits: the async verification path is recursive where the sync path is
iterative, so very deep stale chains may hit the recursion limit. Almost all
async internals are excluded from coverage with `# pragma: no cover`.

Enforced by: `test_async.py::test_async_query_basic`, `::test_mixed_graph_sync_async`,
`::test_dedup_yields_loop`, `test_async_dependency_tracking.py`,
`test_history_retention.py::test_no_pins_leak_from_async_calls_and_cancellation`.

### Thread safety

Safe from any thread: calling queries, reading inputs, `input.set`,
transactions, registering queries and inputs, `prune`, `sweep_unaccessed`,
traces and stats. All of these take the store lock.

Not safe: mutating one collection from another thread while a transaction that
touched it is open (D17), and two engines writing the same named collection
(D10).

## Persistence

### G15. Reuse across runs

A new `Engine` on the same `cache_dir` returns a stored result without running
the body when every recorded dependency matches: an input by the digest of its
current value, a dependency query by the digest of its up-to-date result. An
input never set in the new session is matched by the digest of its default.

Limits: a query whose arguments cannot be encoded is computed but not
persisted. Query identity on disk is `module:qualname` unless `name=` is
given, so two scripts run as `__main__` with same-named queries share entries.

Enforced by: `test_disk_cache.py::test_cross_session_hit_skips_all_query_bodies`,
`::test_file_edit_invalidates_only_its_chain`,
`::test_early_cutoff_survives_across_sessions`,
`::test_deep_dependency_chain_cross_session`,
`test_persistence_permutations.py::test_permutation_incremental_mdbx`.

### G16. No stale persisted result

A persisted result is not served if an input value differs, a dependency's
result differs, the set of dependencies changed, its TTL elapsed, or the
function's bytecode hash changed. G11's blind spots apply here too.

Enforced by: `test_code_versioning.py::test_disk_cache_invalidation_on_code_change`,
`test_disk_cache.py::test_dynamic_graph_topology_cross_session`,
`::test_in_session_invalidation_still_works_after_hydration`.

### G17. Durability

Results are queued when their query finishes and written in batches by a
background thread. They are on disk once `flush_disk()` or `shutdown()`
returns, and also when the interpreter exits normally without either: an
`atexit` hook writes whatever is still queued. "Normally" covers falling off
the end of the script, `sys.exit()` and an unhandled exception.

Limits: a process that is killed (`os._exit`, SIGKILL, a crash in native
code) runs no exit hook and loses the results still queued; the cache stays
usable. Neither backend fsyncs each commit, so a power loss can lose recent
commits even after a flush. The exit hook gives up if the writer thread has
died, so the exit never hangs (`flush_disk()` itself still does, see D18).

Enforced by: `test_known_defects.py::test_d15_results_survive_a_normal_exit_without_shutdown`,
`test_durability_at_exit.py::test_normal_exit_without_shutdown_keeps_every_result`
(both backends; plain exit, `sys.exit`, unhandled exception, engine dropped),
`::test_exit_does_not_hang_when_the_writer_thread_is_dead`,
`::test_hard_kill_is_not_covered_but_leaves_a_usable_cache`.

### G18. Round-trip fidelity

Supported: `None`, `bool`, `int` of any size, `float`, `str`, `bytes`, `list`,
`tuple`, `set`, `frozenset`, `dict`, `datetime`, `date`, `time`, `timedelta`,
`timezone`, dataclasses, `NamedTuple`s and exceptions. These decode to an equal
value of the same type.

Known losses: `bytearray` becomes `bytes`; subclasses of containers become the
base type; dict order comes back sorted; `zoneinfo` zones become fixed offsets;
exceptions keep only type and `args`. Dataclasses with `init=False` fields and
classes defined inside functions cannot be decoded, so their results recompute
every session.

Unsupported, raising `TypeError`: `Enum`, `Decimal`, `Path`, `UUID`, `complex`,
`range`, `deque`.

Enforced by: `test_disk_cache.py::test_round_trip`,
`::test_round_trip_preserves_container_types`,
`::test_bytearray_collapses_to_bytes_and_nan_round_trips`,
`test_disk_format2.py::test_round_trip_preserves_value_and_type`,
`::test_container_subclasses_encode_as_their_base_type`.

### G19. Deterministic encoding

Two equal values of the same type encode to the same bytes regardless of dict
or set insertion order.

Enforced by: `test_disk_cache.py::test_encode_is_deterministic_for_dict_and_set_ordering`,
`test_disk_format2.py::test_encoding_is_independent_of_insertion_order`.

### G20. Corruption and format changes

An undecodable query entry, a missing blob or a digest mismatch is treated as a
miss and recomputed. A cache written by a different on-disk format version is
wiped on open. Opening a directory with the other backend sees an empty cache,
because each backend uses its own files.

Enforced by: `test_disk_cache.py::test_corrupt_blob_falls_back_to_recompute`,
`::test_format_bump_wipes_stale_cache`,
`test_disk_format2.py::test_corrupt_inline_value_falls_back_to_recompute`,
`::test_foreign_extension_payload_is_rejected`.

### Trust boundary

A cache directory is trusted input. Loading one can import any importable
module named in it and construct any dataclass, `NamedTuple` or exception class
with arguments it chooses. Result values are not authenticated. Only open cache
directories you wrote.

## Collections, rewriting and transactions

### G21. Builtin equivalence

`CascadeList`, `CascadeSet` and `CascadeDict` subclass the builtins. Every
mutating method changes storage and logs a diff. Inside a query these reads are
tracked: iteration, `len`, `in`, indexing, `reversed`, list `==`, `index`,
`count`, `copy`, dict `get` and the `keys`/`values`/`items` views.

A mutator that raises changes nothing: storage and log stay in agreement, so
queries see exactly what the collection holds. This makes
`CascadeSet.update` and `difference_update` atomic, where the builtin keeps
the elements handled before the failing one.

Limits: dict views are wrappers, not real views, so set operations on them do
not work. List slices return plain lists. Collections cannot be pickled.

Enforced by: the example tests in `test_incremental_collections.py`
(`::test_failed_list_mutator_changes_nothing`, `::test_failed_set_mutator_changes_nothing`
and `::test_failed_dict_mutator_changes_nothing` cover failing calls of every
mutator) and its Hypothesis tests `::test_list_log_replay_matches_contents`,
`::test_dict_log_replay_matches_contents`, `::test_set_log_replay_matches_contents`.

### G22. Rewrite equivalence

Intended: a query rewritten into an incremental pipeline returns exactly what
the same Python would return without rewriting, after any sequence of
mutations.

Rewritten: single-`for` comprehensions, `map`, `filter`, `reversed`, and the
reducers `sum`, `len`, `min`, `max`, `any`, `all`, `sorted`, `list`, `set`,
`dict` and `str.join`.

Not rewritten, falling back to plain Python: comprehensions with more than one
`for`, `enumerate`, `zip`, `min`/`max` with `key=` or `default=`, `sum` with a
start value, lambdas, async queries, and any query for which one of those
builtin names is rebound: in the query, in a function enclosing it, or at
module level.

By design, iterating a `CascadeSet` yields insertion order.

Enforced by: `test_incremental_pipeline.py::test_list_reducers_match_recompute_through_mutations`,
`::test_property_list_pipeline_equals_recompute` (Hypothesis),
`::test_property_dict_pipeline_equals_recompute` (Hypothesis),
`::test_min_max_handle_removal_of_current_extreme`, `::test_empty_collection_reducers`,
`test_ast_rewrite.py::test_shadowed_builtin_disables_rewriting`,
`::test_builtin_shadowed_in_an_enclosing_function_disables_rewriting`,
`::test_enumerate_and_zip_sources_untouched`.

### G23. Incrementality

After an append, insert, update or remove, a rewritten query runs its mapping
function once for the changed element.

A full rebuild happens after `sort()` or `reverse()`, when a captured closure
value changes, when a stage reads an input or query, and when the same query is
called with alternating arguments.

Enforced by: `test_incremental_pipeline.py::test_single_append_runs_map_and_filter_once`,
`::test_lagging_consumer_requests_only_newer_diffs`,
`::test_closure_value_change_rebuilds_state`.

### G24. Snapshot semantics for collections

A query run with `snapshot=s` sees each collection's contents as of
`s.revision`. A collection created after the snapshot reads as empty. Diff logs
are trimmed only below what live snapshots and pipelines need.

Enforced by: `test_collection_snapshot_direct_reads.py`,
`test_collection_snapshot_before_creation.py`,
`test_history_retention.py::test_collection_snapshot_reads_survive_compaction`,
`::test_collection_log_stays_bounded_under_churn`.

### G25. Transactions

Inside `with engine.transaction():`

- input updates are queued and applied as one revision at exit;
- the last value set for each input wins, inputs ending at their current value
  are skipped, and a block that changes nothing leaves the revision alone;
- collection changes are visible on the collection object at once and are
  undone if the block raises;
- queries run inside the block see the engine as it was before the block;
- named collections write to disk once per collection at commit, and not at
  all if the block raises;
- opening a second transaction in the same context raises `RuntimeError`, even
  for a different engine;
- a transaction object is single-use: entering it a second time raises
  `RuntimeError` and changes nothing.

Limits: if the disk write for a later collection fails at commit, earlier
collections are already on disk.

Enforced by: `test_engine.py::test_engine_transaction`, `test_transaction_collections.py`
(including `::test_transaction_object_cannot_be_entered_twice`),
`test_history_retention.py::test_collection_mutations_in_one_transaction_store_one_version`,
`test_incremental_collections.py::test_mutations_inside_transaction_batch_invalidation`.

### G26. Named collection persistence

A named collection on an engine with `cache_dir` writes each mutation to disk
before applying it in memory. A new engine restores the same contents, and
compaction preserves them. A value that cannot be encoded raises
`PersistentCacheError` at the mutating call.

`clear_disk_cache()` deletes cached query results only. Collection logs are
data that nothing can recompute, so they stay, and a restart after a clear
restores exactly what each collection held at shutdown. To empty a
collection, call its `clear()`.

Limits: writes are committed but not fsynced, so they survive a process kill
but not necessarily a power loss.

Enforced by: `test_incremental_persistence.py::test_named_collections_restore_across_engines`,
`::test_restored_list_keeps_uid_continuity`, `::test_compaction_squashes_log_tail`,
`::test_unserializable_value_raises_persistent_cache_error`,
`::test_clear_disk_cache_keeps_collections`,
`::test_clear_disk_cache_drops_results_but_not_collections`,
`test_transaction_collections.py::test_named_collection_writes_to_disk_only_on_commit`.

---

# Audit, 2026-11-23

## Defects

"Reproduced" means re-run independently for this audit on Python 3.12.9, and
pinned by an expected-failure test in `tests/test_known_defects.py`.
"Reported" means found and probed during the audit but not re-run a second
time; it now applies only to the two sub-items of D7 marked that way.

### Wrong results

| ID | Defect | Evidence |
|---|---|---|
| D1 | A rewritten stage that reads an input the query has already read is not detected as impure. `f = factor(); return sum(x * factor() for x in xs)` over `[1, 2, 3]` still returns 12 after `factor.set(10)`; correct is 60. | Reproduced |
| D2 | A `fixed_point=` query ignores `snapshot=` and returns the head value (7 where the snapshot value is 1). One read also moved `engine.revision` from 3 to 6. | Reproduced |
| D3 | An expired TTL query is not refreshed when reached through a dependent that is already verified at the current revision. `parent()` returned 10 before and after expiry. | Reproduced |
| D5 | Collection reads that are not tracked go stale inside queries: `set(s)`, `s \| x`, set and dict `==`, `s.issubset`, `xs + [...]`, `repr`. `sorted(s \| {99})` stayed `[1, 99]` after `s.add(2)`. | Reproduced |
| D6 | Early bail-out treats dicts that differ only in insertion order as equal, so a dependent that iterates the dict keeps the old order (`['a', 'b']` where a fresh run gives `['b', 'a']`). | Reproduced |
| D7 | Rewritten reducers differ from the builtins. Float `sum([0.1] * 10)` gives `0.9999999999999999` against `1.0`. `sorted(..., reverse=True)` is not stable (`[(1,'b'), (1,'a')]` against `[(1,'a'), (1,'b')]`). `any` and `all` do not short-circuit (`any(1 / z for z in [1, 0])` raises `ZeroDivisionError`). Reported: `min`/`max` tie choice, and `len(map(...))` returning a count instead of raising. | Reproduced |
| D10 | Two engines on one `cache_dir` that both mutate the same named collection overwrite each other's revision numbers, and one can hydrate the other's query result. | Reproduced on both backends: after each engine appended once, a restart restored only the second engine's item. The query-result half was not re-run. |

### Data loss and corruption

| ID | Defect | Evidence |
|---|---|---|
| D12 | When a pinned snapshot predates a collection, compaction ignores later snapshots and open transactions. A snapshot read raises the private `_HistoryCompacted` after 3000 appends, and a rolled-back transaction leaves 2048 of its 3000 appended items in place. | Reproduced |
| D16 | A format-version change wipes collection logs along with cached results. Collection logs are source data and cannot be recomputed. | Reproduced on both backends: `[1, 2, 3]` restored as `[]`, with no error |
| D17 | The open transaction leaks across concurrency boundaries. Another thread mutating the same collection during the block has its change deleted on rollback while queries keep the uncommitted value. A task created in the block that calls `set()` after the block exits has the update dropped. | Reproduced: the other thread's append was gone after rollback, and the task's `set(5)` left the input at 1. The "queries keep the uncommitted value" part was not re-run. |

### Hangs and misleading errors

| ID | Defect | Evidence |
|---|---|---|
| D18 | If the disk writer thread hits an error other than `PersistentCacheError` (for example sqlite "database is locked"), the thread dies and `flush_disk()` and `shutdown()` block forever. | Reproduced on both backends by making the batch write raise `sqlite3.OperationalError` |
| D19 | A direct caller that joins a `submit`-owned run of the same query receives the owner's `QueryCancelled`. | Reproduced |
| D20 | A query result that cannot be encoded raises a bare `TypeError` even with no `cache_dir`. Its message names the unsupported type but not the query. An exception with unencodable `args` is replaced by that `TypeError`. | Reproduced |
| D21 | A sync waiter on the event-loop thread, waiting for an async run of the same query on that loop, deadlocks. Also reachable through the public API: a sync query that depends on an async query, called on the loop thread while that async query is running there, blocks the loop while re-verifying. | Reproduced, through the private entry point and through the public path |

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

Smaller items: the first read of a never-set input bumps `engine.revision`;
the `disk_store` counter and trace are recorded even when the write failed
because the map was full.

Found while fixing D9, not yet numbered or pinned by a test: re-registering a
query with changed code while `engine.revision` has not moved since its
dependents were computed leaves those dependents serving their old results
(`top()` stayed 2 where a fresh run gives 11). The new result gets the same
`changed_at` revision as the one it replaces, so re-verification sees no
change. An input change in between hides it, which is why
`test_reregistering_a_query_dirties_its_dependents` passes.

## Limits that restrict who can use the library

The library's aims, taken from the README: memoise ordinary Python functions
with automatic dependency tracking; make ordinary comprehensions incremental;
persist results across runs with no configuration; run in parallel and async.
Four limits work against those aims for common uses. Each was checked by
running it.

| Limit | Who it stops | Why it conflicts with the aim |
|---|---|---|
| **Value types.** Results and input values must be primitives, builtin containers, `datetime` types, dataclasses, `NamedTuple`s or exceptions, even with no `cache_dir`. `Enum`, `Path`, `Decimal`, `UUID`, plain class instances, `ast` nodes and numpy arrays all raise `TypeError`. | Anyone whose functions return domain objects: compilers and analysers (syntax trees), data work (arrays, frames), most application code (`Enum`, `Path`). | "Ordinary Python functions" return these. An in-memory engine has no need to encode a value; it only needs to tell whether it changed. |
| **Query identity.** A query is its `module:qualname` unless `name=` is passed. Factories and lambdas must pass `name=`; without it each registration replaces the last and the earlier handle raises `StaleHandleError`. Methods cannot be queries. After a reload, callers must pick up the new handle. | Libraries that generate queries (one extra argument), class-based designs. | No longer a silent wrong answer inside a process. Across runs, closures of one body under one name can still share persisted results (G11). |
| **Persistence cannot be trusted across code edits.** Code invalidation sees only the query's own bytecode, and there is no manual version or salt to force invalidation. (Results are now written at a normal exit; a killed process still loses what was queued.) | Anyone using `cache_dir` as a build cache: edit a helper and the old result is served. | "Zero-config persistence" implies it works without the user knowing about it. |
| **Rewriting is on by default but not exact.** Rewritten reducers differ from the builtins (D7), stages can go stale (D1), and ordinary operations such as `set(s)` or `s \| x` are untracked (D5). | Anyone using collections for numeric work, ordering-sensitive output, or set algebra. | The promise is "standard Python, rewritten"; a rewrite that changes a result has to decline instead. |

Limits that narrow use without stopping it:

- **Thread fan-out inside a query goes through `compute_many`.**
  `compute_many` and `compute_many_stream` track dependencies there;
  `submit` raises. `asyncio.gather` over async queries also tracks
  dependencies.
- **One process per `cache_dir`.** Two processes on one directory can hang the
  writer (D18) or mix collection state (D10). Parallel CI jobs and
  `pytest-xdist` workers need separate directories.
- **Only `submit` cancels.** An editor-style tool that calls queries directly
  or through `compute_many` cannot abandon stale work.
- **Positional, hashable arguments only**, with equal arguments sharing a key.
- **Returned values are shared with the cache.**
- **Alternating snapshot and head reads of one query recompute each time**,
  because one result is kept per query and arguments.

Limits that fit the aims as they are: digest-based equality, same-value `set`
as a no-op, snapshot expiry after release, the error-caching rules,
`memoize=False`, eviction, and the cache directory being trusted input.

## Formal model

`docs/formal/CascadeCore.tla` models two inputs, a mode switch and one query,
with a single sequential actor. TLC checks six invariants in CI.

- Writes update the query result in the same atomic step, so lazy
  re-verification, the mechanism behind G1 and G2, is not modelled.
- `ReadLive` is defined but not part of `Next`; `ReadLiveReturnsChoose` is
  defined but not listed in `CascadeCore.cfg`.
- Not modelled: threads, in-flight deduplication, cancellation of running work,
  transactions, collections, compaction, errors, TTL, eviction.
- None of D1–D21 is reachable in the model.

## Test gaps

Ordered by the risk they leave open.

1. **Regression tests for the open defects exist but are expected
   failures.** `tests/test_known_defects.py` has one test per defect (D1-D21),
   each asserting the correct behaviour; those for open defects run under
   `xfail(strict=True)`. A fix removes the marker. The two "Reported"
   sub-items of D7 have no test.
2. **Durability is tested for normal exits only.** `test_durability_at_exit.py`
   exits child processes without `shutdown()` and with `os._exit`. The only
   tests that inject a write failure into a live engine or put two engines on
   one `cache_dir` are the expected failures for D18 and D10. Nothing runs
   two processes on one `cache_dir`.
3. **The Hypothesis state machine is narrow.** It covers inputs, one dynamic
   query and snapshots. It copies input history and the revision from the
   engine rather than modelling them, runs 35 examples of 25 steps, and has no
   transactions, collections, errors, TTL, eviction, persistence or threads.
4. **Rewrite equivalence is tested on small integers only.** No ties, floats,
   stability, short-circuiting, `min`/`max` or `sorted(key=, reverse=)` in the
   property tests.
5. **No Hypothesis test for the disk codec** and no cross-process or
   cross-version determinism test.
6. **Backend parity is partial.** Five tests in `test_disk_cache.py` skip
   sqlite; `test_incremental_persistence.py`, `test_code_versioning.py` and
   `test_error_caching_persistence.py` run mdbx only; `test_transaction_collections.py`
   runs sqlite only. The sqlite vacuum delete paths are excluded from coverage.
7. **Async is thinly tested** and its internals are excluded from coverage:
   no async tests for snapshots, effects, bail-out counts, fixed points or
   eviction.
8. **Collection concurrency has one deterministic test.** No multi-writer or
   writer-against-pipeline stress, and none on free-threaded builds.
9. **Transactions inside the block are untested** for queries reading a
   mutated or newly created collection, `submit`, threads, async tasks and two
   engines.
10. **Two tests do not test what they claim.**
    `test_disk_cache.py::test_format_bump_sqlite` registers a different query
    in its second session, so it would recompute regardless.
    `test_async_coverage.py::test_sync_cycle_fixed_point` is an empty `pass`.
11. **Mutation testing is configured but never run in CI**, so the 95% line
    coverage figure says nothing about whether the assertions would catch a
    bug.
