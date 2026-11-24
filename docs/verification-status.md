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
| Mutation testing | Tooling, a first baseline and a repeat over two modules exist; the runner is in the repository. Outside those two modules most kills come from one pass. |

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
script). mutmut needs `fork`, which Windows does not have: there `run` hands
over to `scripts/mutation_windows.py`, which checks the same mutants against
the same tests with one process per mutant. Every run so far was made that
way. `.github/workflows/mutation.yml` does the same with mutmut itself in
eight shards when started by hand; it has never been started, so mutmut's
own `run` has not completed a run of this suite anywhere yet.

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

That runner is now `scripts/mutation_windows.py`; its docstring says how it
differs from mutmut and how to repeat a set of verdicts. The baseline was
made with an earlier copy of it, from a copy of the project. The result
files are in `mutation-results/` of the working copy the runs were made
in, which is ignored: one folder per run with `results.json` and the diff
of every survivor, and a `summary.md` over both in which the later run
replaces the earlier one for the modules it covers.

### What is unconfirmed

- **A survivor is a finding as it stands.** The suite passed with the
  mutant active.
- **Outside `_evaluator` and `_collections` most kills come from one
  pass.** Those two modules have been run twice in full (see "Repeat over
  `_evaluator` and `_collections`"). Of the baseline as a whole, 1,935 of
  the 8,944 results that were not "survived" were repeated with a runner
  that caps each process at 8 GB; a repeat of all of them was stopped
  after 25 minutes.
- **Some verdicts depend on how threads interleave**, and timeouts move
  with the load. The repeat changed eleven verdicts that no new test
  explains; they are listed in its section.
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
  mutant 5. Fixed tests now fail for all of these (see the two "Covered
  since the baseline" sections on collections), so none of these kills
  depends on chance any more.
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
- **No fixed test checked a read of a collection at an older revision once
  its log no longer starts at zero.** In `DiffLog.since`, mutant 5 turns
  `up_to - self.base_rev` into `up_to + self.base_rev`. After a compaction,
  or after reopening from a compacted state, a query run at a snapshot is
  then handed changes made after the snapshot. Every fixed test passed with
  that change, because wherever one reads at a snapshot the log's base is
  still at zero: the tests with enough changes to compact hold a pin from
  before the collection existed. Only the state machine failed, in some
  runs (covered since, see below).
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
and pass on the real source, on mdbx and on sqlite. The repeat over
`_evaluator` confirms them (see below).

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
fails on the copy and passes on the real source. The repeat over
`_collections` confirms every row (see below).

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

### Covered since the baseline: snapshots of a compacted collection

Two tests were added to `test_collection_compaction_pins.py` on 2026-11-24,
each for a list, a set and a dict. A snapshot is taken once the log's base
is above zero, the collection is changed, and a query run at the snapshot
must return the contents as of the snapshot:

- `test_snapshot_taken_after_a_compaction_keeps_its_contents`: the base
  moved by compaction in memory;
- `test_snapshot_of_a_collection_reopened_from_a_compacted_state_keeps_its_contents`:
  the base comes from the snapshot stored on disk, on mdbx and on sqlite.

With `DiffLog.since` mutant 5 applied by hand to a copy of the source all
nine cases fail: the query at the snapshot returns the later changes too.
They pass on the real source. The other four mutants of `since`, which
ordinary tests already killed, fail them as well. In the repeat a snapshot
test is the one that fails for mutant 5.

### Repeat over `_evaluator` and `_collections`

On 2026-11-24, after the tests of the three sections above, every mutant of
the two modules was run again: 4,534 mutants against 1,967 tests, from the
checkout, with `python scripts/mutation.py run` (so with
`scripts/mutation_windows.py`), in 118 minutes on 28 processes. The mutants
are the same as in the baseline, function by function.

| Module | Killed | Survived | Timeout | Other | Survival rate | In the baseline |
|---|---:|---:|---:|---:|---:|---:|
| `cascade._evaluator` | 2469 | 828 | 18 | 1 | 25.0% | 26.8% |
| `cascade._collections` | 1102 | 116 | 0 | 0 | 9.5% | 9.4% |
| **All fifteen modules**, the other thirteen as in the baseline | 8960 | 2281 | 36 | 5 | 20.2% | 20.8% |

("Other" in the last row is three crashes and two mutants with no test.)

- **The new tests do what the checks by hand said.** 62 mutants that
  survived the baseline are now killed by one of them: 61 in `_evaluator`
  and `_CollectionCore.__init__` mutant 95. Forty of the 61 are in the async
  functions, which the new tests reach by awaiting on a running loop:
  `try_mark_green_async` went from 40 survivors to 21, and
  `_verify_disk_deps` and its async twin from 21 each to 13 each. Every
  mutant the collection tests were written for is killed by them
  (`__init__` 92 to 95, 97 and 99, `CascadeList._new_uid` 3,
  `DiffLog.since` 5). Two timeouts of the baseline are kills as well
  (`try_mark_green` 17, `DiffLog.compact_to` 11). In all, a new test is the
  first to fail for 279 mutants.
- **Six verdicts changed with how threads interleaved.** No new test
  reaches these lines; the tests that do have threads in them.
  - `compute_or_get_memo`, mutants 143, 152 and 153: killed in the baseline,
    alive now. They change the calls made when a caller joins a run that
    depends on a fixed point another thread is still iterating. Two of them
    drop a required argument of `_await_cycle_heads`, so they raise as soon
    as that line runs. In this run it did not run.
  - `_CollectionCore.emit`, mutants 48 and 49: killed in both earlier runs,
    the second time by
    `test_snapshot_readers_and_a_writer_under_an_older_pin`, alive now.
    With them a compaction that falls due inside a mutation runs at once
    and not after it.
  - `_await_cycle_heads`, mutant 5: alive in the baseline, killed now by
    `test_many_threads_entering_a_cycle_at_random_while_inputs_change`.
- **Five verdicts moved to or from a timeout** with no test to explain it:
  two kills and two survivors of the baseline timed out
  (`compute_or_get_memo_async` 75, 78 and 161, `_settle_dependencies` 79),
  and one timeout survived (`_try_hydrate_from_disk` 177).
- **Six kills have the state machine as the test that failed**
  (`_CollectionCore.roll_back_to` 4 and 7, `Evaluator._store_memo_locked` 2
  and 6, `Evaluator._default_version` 3 and 12), so they may not repeat.

So a rate from one run is good to about a dozen mutants in these two
modules, and two paths are tested only when threads happen to meet: the wait
for another thread's cycle heads in the sync path, and a compaction that
falls due while a mutation is in progress.

## Next steps

In order of value for the time they take.

1. **Start `.github/workflows/mutation.yml` once**, and
   `model-check.yml` once, to learn whether they work. One shard is enough
   for the first try.
2. **Write tests that force the two paths reached only by chance**: a
   caller that joins a run depending on a fixed point another thread is
   iterating (`compute_or_get_memo`, the call of `_await_cycle_heads`),
   and a compaction that falls due inside a mutation
   (`_CollectionCore.emit`). See "Repeat over `_evaluator` and
   `_collections`".
3. **Look at the survivors in the async paths that change a condition or
   an operator.** `compute_or_get_memo_async` still has 78 survivors
   against 42 in the sync function, and awaiting on a running loop is
   what made the difference in `try_mark_green_async`.
4. **Decide whether mutation runs fix the Hypothesis seed.** With random
   examples a few verdicts change from run to run; a fixed seed makes the
   rate comparable over time, at the price of always trying the same
   examples. It does nothing for the verdicts that depend on threads.
5. Leave the rest. Driving the overall rate down is not worth it: about a
   fifth of the survivors are strings, and many of the `None` arguments are
   tracing.
