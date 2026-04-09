# Mutation triage (high-assurance iteration)

This document captures mutation-testing outcomes from this iteration and
classifies remaining notable survivors by risk and follow-up action.

## Commands used

All runs used `python3.14t` with `PYTHON_GIL=0` and the `mutmut` CLI.

Focused run helper:

```bash
PYTHON_GIL=0 ./scripts/mutation_fast.sh <mutant> [<mutant> ...]
```

Representative focused target set:

```bash
PYTHON_GIL=0 ./scripts/mutation_fast.sh \
  "cascade.engine.xǁEngineǁsubmit__mutmut_1" \
  "cascade.engine.xǁEngineǁsubmit__mutmut_2" \
  "cascade.engine.xǁEngineǁsubmit__mutmut_4" \
  "cascade.engine.xǁEngineǁsubmit__mutmut_7" \
  "cascade.engine.xǁ_EngineInternalsǁinput_version_at__mutmut_1" \
  "cascade.engine.xǁEngineǁ_input_version_at__mutmut_1" \
  "cascade._evaluator.xǁEvaluatorǁpush_effect__mutmut_4" \
  "cascade._scheduler.xǁWorkStealingExecutorǁ__init____mutmut_6" \
  "cascade._scheduler.xǁWorkStealingExecutorǁ__init____mutmut_13"
```

## Newly killed high-value mutants

- `Engine.submit` snapshot/identity paths:
  - `submit__mutmut_1`, `submit__mutmut_2`, `submit__mutmut_4`, `submit__mutmut_7`
- Internal probe wiring:
  - `_EngineInternals.input_version_at__mutmut_1`
  - `Engine._input_version_at__mutmut_1`
- Accumulator error contract:
  - `Evaluator.push_effect__mutmut_4`
- Scheduler bootstrap invariant:
  - `WorkStealingExecutor.__init____mutmut_6`
  - `WorkStealingExecutor.__init____mutmut_13`

## Survivors observed and triage

### Equivalent / intentionally low-signal API-surface mutants

- `Engine.submit__mutmut_19` / `submit__mutmut_20` (`max_workers=1` -> `None` or
  `2` for internally owned executor)
  - **Why survives:** behavior remains equivalent for single submitted task.
  - **Risk:** low for correctness, moderate for resource-usage policy.
  - **Action:** keep as acceptable survivor unless we want to freeze exact
    thread-pool sizing contract as part of public API.

- `Engine.submit__mutmut_21`, `submit__mutmut_22`, `submit__mutmut_24`,
  `submit__mutmut_26` (keyword argument variants for `owned.shutdown(...)`)
  - **Why survives:** current flow waits on `future.result()` before completion;
    alternate shutdown kwargs do not change observed correctness.
  - **Risk:** low correctness risk; mostly lifecycle/cleanup semantics.
  - **Action:** treat as low-priority unless adding strict lifecycle telemetry.

- `Evaluator.push_effect__mutmut_3` / `push_effect__mutmut_5` (error message text)
  - **Why survives:** only message text differs; exception type still correct.
  - **Risk:** low runtime risk, moderate UX/diagnostic consistency.
  - **Action:** if message stability is desired, add exact-string assertion
    similar to the one introduced in this iteration.

### Needs broader design or dedicated benchmark harness

- `GraphStore.touch_memo_locked__mutmut_3` / `touch_memo_locked__mutmut_4`
  (`next_access_id += 1` changed to `-1` or `+2`)
  - **Why survives:** many tests validate LRU outcomes qualitatively, not exact
    monotonic step-size of access IDs.
  - **Risk:** medium for eviction aging precision under sustained churn.
  - **Action:** add tighter deterministic LRU-age tests with direct internal
    assertions (partially started in this iteration via monotonicity invariant).

### Not-yet-focused in this iteration

Several survivors in `_synthetic_graph`, `_persistence`, and `prune` remain
outside this focused loop. They are valid follow-up candidates once
correctness-critical submit/snapshot/cancellation and scheduler bootstrap paths
are hardened.

## Next mutation-hardening candidates (recommended order)

1. `GraphStore.touch_memo_locked` arithmetic mutants (LRU aging precision).
2. `GraphStore.prune` survivors (graph reachability edge cases).
3. `_persistence.save_payload` survivors (durability and artifact integrity).
