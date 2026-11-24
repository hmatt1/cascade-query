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
| `cascade._collections` | 1218 | 1102 | 115 | 1 | 0 | 0 | 9.4% |
| `cascade._canonical` | 556 | 506 | 49 | 0 | 0 | 1 | 8.8% |
| `cascade._types` | 46 | 43 | 3 | 0 | 0 | 0 | 6.5% |
| `cascade._state` | 35 | 33 | 2 | 0 | 0 | 0 | 5.7% |
| **All** | 11282 | 8902 | 2340 | 35 | 2 | 3 | 20.8% |

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
- **Most kills come from one pass.** 1,935 of the 8,944 results that were
  not "survived" have been repeated with a runner that caps each process at
  8 GB. A repeat of all of them was stopped after 25 minutes; about 7,000
  results have been seen once.
- **The low-memory spell is settled.** For about 11 of the 310 minutes two
  mutants of `CascadeList.__iter__` had grown to 182 GB each and starved
  the other processes. All 282 results from that spell that were not
  "survived" have been repeated. Nine that the first pass could not
  classify turned out killed. Of the 273 kills (232 in
  `cascade._collections`), 269 held and 4 did not. The four are in
  `_CollectionCore`, and the table counts them as survivors:
  - `check_storage`, mutant 2;
  - `__init__`, mutant 32: `compact_due = None` instead of `False`. The
    flag is only ever tested for truth, so nothing can tell the two apart
    and the first kill was false;
  - `__init__`, mutant 49: the message of the error for a stored list
    opened as a dict (or the reverse) replaced by `None`. No test reads
    the message, so that kill was false too;
  - `__init__`, mutant 95: see "Some kills depend on chance" below.
- Outside that spell 1,653 results were repeated and none changed.
- **Some kills depend on chance.** In `_CollectionCore.__init__`, mutants
  92 to 95 are one change in four spellings: when a list is reopened, its
  uid counter is not moved past the uids in the changes reloaded from
  disk. Only the Hypothesis state machine notices, and not in every run.
  In the runs after the first pass, 92 was killed twice and survived once,
  93 and 94 were killed once each, and 95 survived once. The table has
  each by its latest run: 95 as survived, the other three as killed. The
  state machine is also the test that failed for mutants 97 and 99 of that
  function, for `CascadeList._new_uid` mutant 3 and for `DiffLog.since`
  mutant 5. A fixed test now fails for all of these but the last (see
  "Covered since the baseline: uids of a reopened list"); the kill of
  `DiffLog.since` mutant 5 still depends on chance.
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
- **Reloading from disk is where survivors touched correctness.**
  `_try_hydrate_from_disk` and its async twin had 122 between them, and
  `_verify_disk_deps` 42. Changes that no test noticed in the baseline
  (covered since, or shown to change nothing; see the next section):
  - the expiry of a TTL result reloaded from disk computed as
    `computed_at_time - ttl` instead of `+ ttl`;
  - the guard against reloading a key from inside its own reload
    (`hydrating | {key}` changed to `hydrating & {key}`);
  - `record.get("is_error", False)` changed to default to `True`.
- **No fixed test checked that a reopened list continues its uids past
  changes that were not compacted.** When a list is reopened,
  `_CollectionCore.__init__` moves `next_uid` past the uids in the changes
  stored since the last compaction. With that step disabled (mutants 92 to
  95) the whole suite could pass; only the state machine failed, and only
  when its random examples happened to reopen a list and add to it. Found
  by the repeat of the low-memory spell (covered since, see below).
- **No fixed test checks a read of a collection at an older revision once
  its log no longer starts at zero.** In `DiffLog.since`, mutant 5 turns
  `up_to - self.base_rev` into `up_to + self.base_rev`. After a compaction,
  or after reopening from a compacted state, a reader pinned at an older
  revision would then be handed changes made after it. Every fixed test
  passes with that change; only the state machine fails, in some runs.
- **TTL boundaries were not pinned.** In `try_mark_green`, `> ttl` could
  become `>= ttl`, and a dependency's expiry `<` could become `<=` (the
  first is covered since, the second changes nothing; see the next
  section).
- **Tracing is barely asserted.** About 60 survivors are arguments of
  `trace_event` calls.

### Covered since the baseline: disk reload and TTL

Tests were added on 2026-11-24 for the disk-reload and TTL changes above and
for the survivors in `_verify_disk_deps` that change a condition or a stored
value. Each change was applied by hand to a copy of the source, to the sync
function and to its async twin separately. The new tests fail on the copy
and pass on the real source, on mdbx and on sqlite. No mutation run was
repeated, so the table above is still the baseline.

| Change (sync and async) | Killed by |
|---|---|
| Reload: expiry `computed_at_time - ttl` | `test_ttl.py::test_result_loaded_from_disk_expires_when_the_stored_one_would` |
| Reload: `- ttl` in the comparison with the expiry of the dependencies | `::test_ttl_result_loaded_from_disk_expires_with_a_shorter_ttl_beneath_it` |
| Reload: `> ttl` to `>= ttl` | `::test_result_loaded_from_disk_expires_when_the_stored_one_would` |
| Reload: `is_error` defaults to `True` | `test_disk_cache.py::test_record_without_an_error_flag_is_loaded_as_a_value` |
| `try_mark_green`: `> ttl` to `>= ttl` | `test_ttl.py::test_ttl_result_is_still_good_at_exactly_its_ttl` |
| `try_mark_green`: `dep_expires_at is not None and` to `or` | `::test_ttl_query_stays_cached_above_a_query_without_an_expiry`, `::test_verified_dependent_keeps_the_shortest_expiry_beneath_it` |
| `_verify_disk_deps`: the checks on the shape of a dependency row (mutants 8, 13, 14) | `test_disk_cache.py::test_malformed_dependency_row_falls_back_to_recompute` |
| `_verify_disk_deps`: the stamp noted for an input or a query replaced (41, 72, 73) | `::test_loaded_result_is_verified_like_a_computed_one` |
| `_verify_disk_deps`: earliest expiry, `and` to `or`, `state[3]` (74, 79) | `test_ttl.py::test_dependent_loaded_from_disk_expires_with_the_shortest_ttl_beneath_it` |
| Reload: `changed_at=None` on the loaded result (`_try_hydrate_from_disk`, mutant 154) | `test_disk_cache.py::test_dependent_notices_a_result_loaded_again_with_another_value` |

The last row was not among the changes listed above. With it the engine
serves a stale result: a dependent held in memory misses that its dependency
was loaded again with a value another engine stored.

Four of the listed changes cannot be noticed by any test, because the engine
behaves the same with them:

- **Three `<` to `<=`**: the reloaded result's own expiry against that of
  its dependencies, a dependency's expiry in `try_mark_green`, and the same
  in `_verify_disk_deps` (mutant 80). The only case the change adds is the
  two sides being equal, and then the assignment that follows stores the
  value the variable already has.
- **`hydrating | {key}` to `hydrating & {key}`**: the guard it feeds is never
  reached. A reload starts only after its caller owns the in-flight slot of
  that key and revision. A nested call for the same key finds the slot, sees
  that it is the owner and runs the query instead of loading it, and a
  nested call cannot be at another revision. The whole suite passes with
  the guard replaced by an assertion. So does a session whose two records
  name each other as dependencies, the one state that leads a reload back
  to its own key with no running query in between: it raises `CycleError`
  with or without the guard.

Two things to know when reading these tests:

- `is_error` and the shape of a dependency row can only be wrong in a
  record edited by hand; the engine writes every field. Those tests rewrite
  a stored record, as `test_disk_format2.py` does for a corrupt value.
- An `async def` query called from plain code takes the sync path. Only a
  call awaited on a running event loop reaches `_try_hydrate_from_disk_async`
  and `try_mark_green_async`, so the async variants of these tests await on
  a loop. A test that calls an async query directly tests the sync path.

Still not covered in `_try_hydrate_from_disk`: the survivors that change a
string or a `trace_event` argument, the defaults for a record with no
`computed_at_time` or `effects` field, and the bookkeeping of the loaded
result (`verified_at`, the access counter, the eviction order).

### Covered since the baseline: uids of a reopened list

`test_incremental_persistence.py::test_item_added_to_a_reopened_list_gets_a_uid_of_its_own`
was added on 2026-11-24. It stores a list, reopens it, adds an item and
changes that item, and a third session must read the list back right. It
runs with and without a compaction before the last stored change, on mdbx
and on sqlite. With a uid handed out twice, the change to the new item lands
on an old one when the stored changes are replayed.

Each change below was applied by hand to a copy of the source. The new test
fails on the copy and passes on the real source. No mutation run was
repeated, so the table above is still the baseline.

| Change | New test |
|---|---|
| `_CollectionCore.__init__`: the uid of a reloaded change is not read (mutants 92 to 95) | fails, all four cases |
| `_CollectionCore.__init__`: `uid >= self.next_uid` to `>` (97) | fails, all four cases |
| `_CollectionCore.__init__`: `self.next_uid = uid + 1` to `uid - 1` (99) | fails, all four cases |
| `CascadeList._new_uid`: `next_uid = uid + 1` to `uid - 1` (3) | fails in the two cases with a compaction |

With the last change every other fixed test passes, so the new test is the
only one that notices it. `_CollectionCore.__init__` mutant 100 (`uid + 2`)
is not a defect and is not tested for: uids then skip a number and stay
unique.

## Next steps

In order of value for the time they take.

1. **Decide where the Windows runner lives.** Either add it to the
   repository behind `scripts/mutation.py run`, so that a run can be
   repeated on the machine that has the cores, or drop it and rely on Linux
   and the workflow. Until then the baseline cannot be reproduced from the
   repository.
2. **Start `.github/workflows/mutation.yml` once**, and
   `model-check.yml` once, to learn whether they work. One shard is enough
   for the first try.
3. **Write a fixed test for a read of a collection at an older revision
   after a compaction** (`DiffLog.since`, mutant 5). That is where the list
   still points at behaviour and not at wording. The tests for the
   disk-reload and TTL survivors and for the uids of a reopened list are
   written (see the two "Covered since the baseline" sections); a mutation
   run over `cascade._evaluator` and `cascade._collections` would confirm
   them and update the table.
4. **Decide whether mutation runs fix the Hypothesis seed.** With random
   examples a few verdicts change from run to run; a fixed seed makes the
   rate comparable over time, at the price of always trying the same
   examples.
5. Leave the rest. Driving the overall rate down is not worth it: about a
   fifth of the survivors are strings, and many of the `None` arguments are
   tracing.
