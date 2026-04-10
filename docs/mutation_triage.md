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
  "cascade._evaluator.xǁEvaluatorǁpush_effect__mutmut_4" \
  "cascade._scheduler.xǁWorkStealingExecutorǁ__init____mutmut_6" \
  "cascade._scheduler.xǁWorkStealingExecutorǁ__init____mutmut_13"
```

## This iteration's focused targets and outcomes

### Targets

- `GraphStore.touch_memo_locked` arithmetic/LRU-aging mutants
- `GraphStore.prune` reachability and traversal-safety mutants
- `_persistence.save_payload` durability/integrity contract mutants

### Newly killed high-value mutants this round

- `GraphStore.touch_memo_locked`:
  - `touch_memo_locked__mutmut_3` (`next_access_id += 1` -> `-= 1`)
  - `touch_memo_locked__mutmut_4` (`next_access_id += 1` -> `+= 2`)
- `GraphStore.prune`:
  - `prune__mutmut_3`, `prune__mutmut_6`, `prune__mutmut_7`, `prune__mutmut_9`
  - `prune__mutmut_11`, `prune__mutmut_12`, `prune__mutmut_13`, `prune__mutmut_14`
  - `prune__mutmut_15`, `prune__mutmut_16`, `prune__mutmut_17`
  - `prune__mutmut_10` (killed after adding internal query-only reachability oracle)
- `_persistence.save_payload`:
  - `x_save_payload__mutmut_3` (`protocol=HIGHEST_PROTOCOL` -> `None`)
  - `x_save_payload__mutmut_5` (dropped explicit protocol argument)
  - `x_save_payload__mutmut_10`, `x_save_payload__mutmut_13`, `x_save_payload__mutmut_19`
    (SQL text/normalization variants)

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

### Remaining survivors / timeouts from this focused pass

- `GraphStore.prune__mutmut_5` (`node = queue.popleft()` -> `node = None`)
  - **Status:** timeout (not survived) on focused runs.
  - **Why:** this mutant can spin indefinitely in some memo-graph shapes because
    queue elements are never consumed.
  - **Risk:** medium for pathological non-termination if traversal logic regresses.
  - **Action:** keep timeout-guarded prune tests and consider adding an explicit
    production-side defensive iteration cap or watchdog for prune traversal.

### Low-risk equivalent/contract-level survivors

- None observed in the three prioritized target groups after this round's test
  additions, aside from the `prune__mutmut_5` timeout class above.

## Next mutation-hardening candidates (recommended order)

1. `GraphStore.prune__mutmut_5` timeout class: decide whether to encode a
   fail-fast traversal guard in implementation.
2. `_persistence.load_payload` mutants (deserialization/shape validation).
3. Remaining broader `_synthetic_graph` survivors once prune timeout policy is
   settled.
