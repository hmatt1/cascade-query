# Requirement Traceability Matrix

This matrix maps each requested capability to implementation evidence and test proof.

Status meanings:
- `PASS`: capability implemented and tested with direct evidence.
- `PARTIAL`: capability exists but behavior or proof was incomplete.
- `FAIL`: capability missing.

## Matrix

| Requirement | Status | Implementation evidence | Test evidence |
|---|---|---|---|
| Smart recalculation | PASS | Query memoization + dependency verification in `_compute_or_get_memo`, `_try_mark_green`, `_dependency_changed_at`; stale paths recompute only when deps changed. | `test_smart_recalculation_and_selective_updates` |
| Dependency tracking | PASS | Dynamic dependency capture in `_record_dependency`; memo dependency graph stored in `MemoEntry.deps` and `_dependents`; graph introspection via `inspect_graph()`. | `test_smart_recalculation_and_selective_updates`, `test_eviction_and_prune` |
| Selective pull updates | PASS | Demand-driven calls in `_query_call`; parent remains green via `cache_green` when dep `changed_at` unchanged; backdating in `_recompute`. | `test_smart_recalculation_and_selective_updates` |
| Persistence | PASS | Durable save/load with SQLite+pickle in `save()` and `load()`, preserving inputs, memos, dependents, traces, revision, access order. | `test_persistence_save_and_load`, `test_persistence_round_trip_robustness`, `test_misc_api_edges_and_default_input_paths` (empty DB path) |
| Work stealing | PASS | `_WorkStealingExecutor` local pop + cross-queue steal (`popleft`) scheduling, used by `compute_many()`. | `test_work_stealing_compute_many`, `test_multi_cpu_parallel_overlap_proof`, `test_compute_many_propagates_worker_exceptions` |
| Query dedup | PASS | In-flight future table `_in_flight`; non-owner requests block on owner future and replay cached effects. | `test_query_dedup_concurrent_requests`, `test_query_dedup_heavy_contention_stress` |
| Red-green bailout (backdating) | PASS | Value hash comparison in `_recompute`; unchanged value keeps previous `changed_at` and emits `backdate`; parent green-check prevents recompute. | `test_smart_recalculation_and_selective_updates` |
| MVCC snapshot isolation | PASS | Snapshot revision read via `_input_version_at`; query evaluation accepts explicit snapshot and verifies deps at snapshot revision. | `test_snapshot_isolation_mvcc`, `test_snapshot_reads_stable_during_concurrent_writes` |
| Cancellation | PASS | Epoch-based cancellation (`_cancel_epoch`) checked at query/input boundaries via `_check_cancelled`; writes bump epoch in `_set_input`. | `test_safe_task_cancellation_after_input_mutation`, `test_cancellation_race_stress` |
| Side-effect replay | PASS | Effects recorded in frames (`_push_effect`) and replayed on cache hits (`_replay_effects`) for root and transitive parent memo frames. | `test_side_effect_replay_on_cache_hits`, `test_nested_side_effect_replay_propagates_to_parent_effects` |
| Dynamic graph expansion | PASS | Query body may branch/build dependencies dynamically at runtime; deps captured from actual execution path. | `test_dynamic_graph_expansion` |
| Eviction/pruning | PASS | LRU eviction in `_evict_if_needed_locked`; reachability pruning in `prune()` and edge cleanup in `_drop_memo_locked`. | `test_eviction_and_prune`, `test_misc_api_edges_and_default_input_paths` |
| Cycle detection | PASS | Runtime stack cycle check in `_query_call` raises `CycleError` with full chain rendering. | `test_cycle_detection`, `test_long_chain_cycle_detection_behavior` |
| Tracing | PASS | Structured trace events in `_trace_event`; events emitted for input, recompute, cache states, dedup wait, eviction, backdate. | `test_tracing_captures_events`, plus stress tests asserting event presence (`dedup_wait`) |

## Fixes introduced in this audit

1. **Side-effect replay transitive propagation hardening (previously PARTIAL)**
   - Problem: replayed child effects were not copied onto the currently recomputing parent frame on cache-hit child calls.
   - Change: `_replay_effects` now mirrors `_push_effect` behavior by extending all active frames, not only ancestor subset.
   - Proof: `test_nested_side_effect_replay_propagates_to_parent_effects`.

2. **Stress and robustness proof additions**
   - Added high-contention dedup, cancellation race loops, concurrent snapshot-read/write stability, long-chain cycle diagnostics, and multi-load persistence round-trip tests.

3. **Coverage gating**
   - Added pytest-cov in CI and fail-under gate.
   - Added line+branch threshold enforcement command documented below.

## Coverage thresholds enforced

- Minimum **line (statement)** coverage: `95%`
- Minimum **branch** coverage: `90%`

Enforced by:
1. `pytest --cov ... --cov-fail-under=95` (line/statements gate in CI test step)
2. Post-test script parsing `coverage.json` and failing when branch `< 90`
