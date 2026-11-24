# Verification: where it stands

Status on 2026-11-24, after one round of work on four items: mutation
testing, a wider Hypothesis state machine, a closer TLA+ model, and deleting
the TLC output that had been committed. This file says what each one
established, what is unconfirmed, and what to do next. It is a snapshot;
`docs/guarantees.md` stays the reference for what the engine promises.

**No defect in the engine was found by any of the three checks.** What the
work did find is where the tests are thin, and one test that leaked state
between runs.

| Item | State |
|---|---|
| TLC output under `docs/formal/` | Deleted (255 files) and ignored. Done. |
| State machine | Rewritten with the new rules, on three backends. Done. |
| TLA+ model | Rewritten; both configurations pass. The on-demand workflow has never run on GitHub. |
| Mutation testing | Tooling and a first baseline exist. The baseline is from one pass with known weak spots, and the runner that produced it is not in the repository. |

## State machine

`tests/test_stateful_engine_invariants.py` keeps a plain-Python model and
compares every read of the engine with a plain function of it. It covers
inputs, a dynamic dependency, snapshots, TTL, re-registered code, a fixed
point (also through a snapshot), a collection under transactions that commit
or roll back, compaction while snapshots are held, and reopening from disk.
It runs in memory, on mdbx and on sqlite, 100 examples of 40 steps each, in
about 12 seconds.

- The engine passes on all three.
- Thirteen faults seeded into the engine one at a time each make it fail, so
  a pass is evidence and not silence.
- It is sequential: no threads, errors, eviction, async queries, effects or
  `compute_many`, one collection, one writer (test gap 3 in
  `docs/guarantees.md`).
- Its examples are random. One mutant was killed by it in two runs and
  survived a third (see below).

## TLA+ model

`docs/formal/CascadeCore.tla` models lazy re-verification with two concurrent
callers, writers and evictions; `docs/formal/README.md` maps actions to code
and invariants to guarantees.

- `CascadeCore.cfg`: 26,385,989 distinct states, no violation (1.5 minutes
  with 32 workers).
- `CascadeMinimal.cfg`: 3,936,090 states, no violation, 12 seconds. CI runs
  this one on every push.
- Thirteen bugs seeded into the model each violate an invariant.
- Checked only with the `tla2tools.jar` committed at the root of the
  repository. `.github/workflows/model-check.yml` and the changed
  `tla_check` job in `workflow.yml` have not run on GitHub yet. The publish
  job depends on `tla_check`, so the first push shows whether it passes
  there.

## Mutation testing

### Running it

```bash
python scripts/mutation.py run                       # all of src/cascade
python scripts/mutation.py run "cascade._state.*"    # one module
python scripts/mutation.py collect
python scripts/mutation.py summary
```

`run` wraps `mutmut run` (two fixes it needs for this suite are in the
script). mutmut needs `fork`, so `run` works on Linux, macOS and WSL, not on
Windows. `.github/workflows/mutation.yml` does the same in eight shards when
started by hand; it has never been started. Neither `run` nor the workflow
has completed a run anywhere yet.

### First baseline

11,282 mutants on covered lines, each run against the tests that reach its
function (1,916 tests; `tests/test_performance.py` and the tests marked
`slow` are left out).

| Module | Mutants | Killed | Survived | Timeout | No tests | Other | Survival rate |
|---|---:|---:|---:|---:|---:|---:|---:|
| `cascade._serde` | 382 | 225 | 157 | 0 | 0 | 0 | 41.1% |
| `cascade._graph_export` | 193 | 128 | 65 | 0 | 0 | 0 | 33.7% |
| `cascade._evaluator` | 3316 | 2411 | 888 | 16 | 0 | 1 | 26.8% |
| `cascade._incremental` | 1321 | 1013 | 308 | 0 | 0 | 0 | 23.3% |
| `cascade._disk_cache` | 892 | 713 | 179 | 0 | 0 | 0 | 20.1% |
| `cascade._store` | 1080 | 859 | 211 | 9 | 1 | 0 | 19.6% |
| `cascade.engine` | 1070 | 861 | 208 | 0 | 1 | 0 | 19.5% |
| `cascade._scheduler` | 54 | 36 | 9 | 9 | 0 | 0 | 16.7% |
| `cascade._keys` | 411 | 356 | 55 | 0 | 0 | 0 | 13.4% |
| `cascade._ast_rewrite` | 653 | 567 | 85 | 0 | 0 | 1 | 13.0% |
| `cascade._synthetic_graph` | 55 | 49 | 6 | 0 | 0 | 0 | 10.9% |
| `cascade._collections` | 1218 | 1105 | 112 | 1 | 0 | 0 | 9.2% |
| `cascade._canonical` | 556 | 506 | 49 | 0 | 0 | 1 | 8.8% |
| `cascade._types` | 46 | 43 | 3 | 0 | 0 | 0 | 6.5% |
| `cascade._state` | 35 | 33 | 2 | 0 | 0 | 0 | 5.7% |
| **All** | 11282 | 8905 | 2337 | 35 | 2 | 3 | 20.7% |

Survival rate = survived / (killed + survived + timeout). "Other" is three
mutants that crash the interpreter every time (endless recursion).

### How it was produced

On Windows 11 with Python 3.12 and mutmut 3.6.0, in 310 minutes on 28
processes. `mutmut run` does not start on Windows, so a separate runner did
the loop: mutmut's own mutants, instrumentation and choice of tests, with
one process per mutant instead of a fork. One difference follows from that:
the test modules are imported with the mutant active, so a mutant in a
function called at import time can stop pytest at collection. The runner
counts that as a kill only if the same tests pass with no mutant.

**That runner is not in the repository.** An untracked copy is in
`scratch/mutation-windows/` of the working copy the run was made in. The
result files (`results.json`, the diff of every survivor, `summary.md`) are
in `mutation-results/` there, which is ignored.

### What is unconfirmed

- **A survivor is a finding as it stands.** The suite passed with the
  mutant active.
- **Kills come from one pass.** For about 11 of the 310 minutes two mutants
  of `CascadeList.__iter__` had grown to 182 GB each and starved the other
  processes. One mutant recorded as killed in that spell survives
  (`_CollectionCore.check_storage`, mutant 2; corrected in the table). 260
  other kills recorded in that spell have not been repeated; most are in
  `cascade._collections`, so its 9.2% is the least certain figure.
- 1,675 of the 8,944 results that were not "survived" have been repeated
  with a runner that caps each process at 8 GB: one changed (the mutant
  above), and nine that the first pass could not classify turned out
  killed. A repeat of all of them was stopped after 25 minutes.
- **Some kills depend on chance.** `_CollectionCore.__init__`, mutant 92, is
  killed only by the Hypothesis state machine, and not in every run.
- The examples test used to load whatever cache the previous run had left
  in the working directory. `tests/test_examples.py` now runs each example
  in an empty directory. The first pass ran before that change.

### What the survivors show

Of the survivors, sorted roughly by the kind of change:

| Kind of change | Survivors |
|---|---:|
| An argument replaced by `None`, or dropped | about 780 |
| Only a string literal changed (a message, a key, a name) | about 520 |
| A condition, operator or flag changed (`>` for `>=`, `and` for `or`, `+` for `-`, `True` for `False`) | about 425 |
| A number changed | about 200 |
| An assignment replaced by `None` | about 180 |
| Other | about 220 |

- **Most of `_serde` is wording.** 90 of the 101 survivors in
  `_to_jsonable` change only a string.
- **The async paths are tested less than the sync ones.**
  `compute_or_get_memo_async` has 79 survivors, `compute_or_get_memo` 39.
- **Reloading from disk is where survivors touch correctness.**
  `_try_hydrate_from_disk` and its async twin have 122 between them, and
  `_verify_disk_deps` 42. Changes that no test notices:
  - the expiry of a TTL result reloaded from disk computed as
    `computed_at_time - ttl` instead of `+ ttl`;
  - the guard against reloading a key from inside its own reload
    (`hydrating | {key}` changed to `hydrating & {key}`);
  - `record.get("is_error", False)` changed to default to `True`.
- **TTL boundaries are not pinned.** In `try_mark_green`, `> ttl` can
  become `>= ttl`, and a dependency's expiry `<` can become `<=`.
- **Tracing is barely asserted.** About 60 survivors are arguments of
  `trace_event` calls.

## Next steps

In order of value for the time they take.

1. **Repeat the 260 kills from the low-memory spell** with the capped
   runner. About ten minutes. It settles the `_collections` figure.
2. **Decide where the Windows runner lives.** Either add it to the
   repository behind `scripts/mutation.py run`, so that a run can be
   repeated on the machine that has the cores, or drop it and rely on Linux
   and the workflow. Until then the baseline cannot be reproduced from the
   repository.
3. **Start `.github/workflows/mutation.yml` once**, and
   `model-check.yml` once, to learn whether they work. One shard is enough
   for the first try.
4. **Write tests for the disk-reload and TTL survivors** listed above.
   That is where the list points at behaviour and not at wording.
5. **Decide whether mutation runs fix the Hypothesis seed.** With random
   examples a few verdicts change from run to run; a fixed seed makes the
   rate comparable over time, at the price of always trying the same
   examples.
6. Leave the rest. Driving the overall rate down is not worth it: about a
   fifth of the survivors are strings, and many of the `None` arguments are
   tracing.
