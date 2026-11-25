# Verification: where it stands

Status on 2026-11-24, after one round of work on four items: mutation
testing, a wider Hypothesis state machine, a closer TLA+ model, and deleting
the TLC output that had been committed. This file says what each one
established, what is unconfirmed, and what to do next. It is a snapshot;
`docs/guarantees.md` stays the reference for what the engine promises.

**None of the three checks failed on a defect in the engine. Three were
found on the way, all fixed since.** Writing tests for what the mutation
runs showed found D33, a query that ran twice at one revision, and D34, a
cancelled task that failed the run it waited for and that run's other
waiters. The free-threaded job of the first CI run found D35: of several
threads asking for a dropped fixed-point member, one got `CycleError`. See
the sections named "Defect found on the way". Apart from that the work
found where the tests are thin, and one test that leaked state between
runs.

| Item | State |
|---|---|
| TLC output under `docs/formal/` | Deleted (255 files) and ignored. Done. |
| State machine | Rewritten with the new rules, on three backends. Done. |
| TLA+ model | Rewritten; both configurations pass, and both the CI job and the exhaustive on-demand workflow (`model-check.yml`, 12m44s) passed on GitHub on 2026-11-24. |
| Mutation testing | Tooling, a first baseline and a repeat over two modules exist; the runner is in the repository. Outside those two modules most kills come from one pass. Two defects were found while writing tests for its findings, D33 and D34; both are fixed. The first on-demand run of `mutation.yml` (`small` shard) caught a global `register_type` leak in `tests/test_value_types.py` when run twice in one process (fixed since). |

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
  repository, and by the changed `tla_check` job in `workflow.yml`, which
  passed on GitHub on 2026-11-24 (the push of `a48c35d`).
  `.github/workflows/model-check.yml` has not been started yet.

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
falls due while a mutation is in progress. Fixed tests force both since
(next section).

### Covered since the repeat: the two paths reached by chance

Tests were added on 2026-11-24 that put a caller on each path on purpose.
Each change named below was applied by hand to a copy of the source and
the tests run against it; on the real source they passed 300 times in a
row. No mutation run was repeated, so the tables above stand.

**A compaction that falls due inside a mutation.** In-memory compaction asks
the pipelines how far they have read, which takes the pipeline runtime's
lock; a pipeline holds that lock while it reads the collection; a mutation
holds the collection's lock. `emit` therefore only notes that a compaction
is due, and it runs once the mutation has let go. `emit` mutants 42, 48 and
49 run it at once, and a reader and a writer then wait for each other.
`test_collection_compaction_pins.py::test_compaction_due_inside_a_mutation_does_not_block_a_pipeline_reading`
stops a reader inside a pipeline stage until a writer is inside an `extend`
of 3,000 items. With each of the three mutants both threads hang, every
time; before, that took a reader and the writer meeting by chance.

**A caller that joined a run which ended on a guess.** Tests in
`test_cross_thread_cycles.py`:

- `test_thread_joining_a_run_that_ends_on_a_guess_waits_for_the_fixed_point`:
  the caller iterates none of the fixed points. It has to wait for them
  (`_await_cycle_heads`) and then read the final result; the test checks
  that it waited twice and ran nothing itself. It fails with
  `compute_or_get_memo` mutants 125, 137, 138, 150, 151, 152 and 153, and
  with `_await_cycle_heads` mutants 2, 3, 4, 6, 8, 9 and 10. Mutants 151,
  152 and 153 and those seven were alive after the repeat. The seven raise
  inside `_await_cycle_heads`, which lets such an error pass, so the caller
  still got the right value; the test notices that the caller then runs the
  member itself. Mutants 1, 5, 7, 11 and 12 of `_await_cycle_heads` still
  pass: they change the function handed over and `need_value`, which do not
  matter to a caller that joins the fixed point's run.
- `test_task_joining_a_run_that_ends_on_a_guess_waits_for_the_fixed_point`:
  the same with two tasks on one loop. mutmut made no mutants for
  `_await_cycle_heads_async` or for the lines that call it, so four changes
  were made by hand (an argument of the call dropped, the call replaced by
  `pass`, `_owns_any(None, ...)`, an argument of the inner call dropped).
  The test fails with each.
- A caller that iterates one of the fixed points itself runs the member on
  its own guess. A third test,
  `test_two_threads_iterating_one_fixed_point_each_use_their_own_guess`,
  put two threads there and failed with `compute_or_get_memo` mutants 141,
  142, 143, 144 and 147. It could do so only through defect D33, by
  holding that defect's window open, and was removed with the fix. The
  branch stays in the code. `test_task_iterating_a_fixed_point_runs_a_joined_member_on_its_own_guess`
  now gets tasks there by cancelling the owner of the fixed point's run;
  nothing gets threads there. See "What the fix leaves" below.

### Defect found on the way: D33 (fixed)

Found while working out how the random test reached the last of those
states.

- **What.** The body of a query ran twice at one revision, on two threads,
  with no cycle anywhere, and the trace showed `dedup_cycle`. G5 says one
  caller runs the body and the others wait.
- **When.** Thread B runs `total()`, which needs `part()`, and waits for
  thread A's run of `part()`. A finishes `part()` and asks for `total()`.
  B had its answer by then but was still recorded as waiting for A: a
  waiter removed its record itself, after it had taken the engine's lock
  again. A read "B runs `total()` and waits for me", took that for two
  threads waiting for each other, and ran `total()` on its own stack.
- **How often.** With nothing patched, in 300 of 300 trials on Python
  3.12.9: under the interpreter lock A goes on before B wakes.
- **Consequence.** A second run of the body, with whatever it does outside
  the engine. Both runs returned the right value in every trial and no
  wrong result was found. With `fixed_point=`, both threads iterated the
  fixed point.
- **Where it came from.** The record of who waits for whom came with the
  fix for D24.
- **The fix.** A run that is released (`_release_in_flight`) now drops the
  records of the callers that waited for it, in the critical section that
  unregisters it. `_leave_wait` remains for a waiter that stops before the
  run does.
- **Tests.**
  `tests/test_known_defects.py::test_d33_caller_the_owner_just_stopped_waiting_for_does_not_run_the_query_again`
  asserts one run and no `dedup_cycle`. It holds B where it used to remove
  its record, so the outcome does not depend on the build; it failed before
  the fix. `test_cross_thread_cycles.py::test_releasing_one_run_leaves_the_waits_for_other_runs_on_record`
  is for the other direction: with a fix that drops the records of other
  runs' waiters too, two threads in one cycle wait for each other for good.
  Four ways of breaking the fix were applied to a copy (never dropping,
  dropping the others' records, dropping every record, finding the records
  and keeping them); each fails one of the two tests.
- **After the fix.** The plain reproduction shows no second run in 300
  trials. The random test no longer runs a query a second time because of a
  record that is out of date (11 to 19 times a run before).

**What the fix leaves.** `compute_or_get_memo` and its async twin have a
branch for a caller that joined a run computed from another caller's guess
for a fixed point it iterates itself. Before the fix the random test took it
7 to 47 times a run, always through D33; after it, not once in three runs.
It is kept: two callers still iterate one fixed point at one revision when
the owner of its run gives up part way while another caller runs it on its
own stack.

- For tasks that is a cancelled owner.
  `test_cross_thread_cycles.py::test_task_iterating_a_fixed_point_runs_a_joined_member_on_its_own_guess`
  does it with three tasks and takes the async branch every time (20 of 20
  trials before it became a test). With `None` in place of the runtime on
  that line the test fails. It could only be written once D34 was fixed:
  cancelling the owner is cancelling a task that waits for a run.
- For threads it takes a wait cut short by an interrupt. No test brings one
  there, and the line is marked `# pragma: no cover`.

### Defect found on the way: D34 (fixed)

Found while checking what the D33 fix leaves reachable. It was in the
committed source, so it did not come from that fix.

- **What.** Several tasks ask for one async query: one runs it, the others
  wait for that run. One waiter is cancelled, with `task.cancel()` or by
  `asyncio.wait_for` running out. When the run finished, the task that ran
  it got `concurrent.futures.InvalidStateError` where its value was due, and
  every other waiter got `CancelledError` though nobody had cancelled it.
- **The worse case.** A thread that calls an async query from sync code runs
  it in a task of its own. With `outer()` a sync query that calls async
  `slow()`, and a task elsewhere that waited for that run of `slow()` and
  was cancelled, `outer()` raised `InvalidStateError`, and the error was
  cached: two more calls raised it again without running anything. A thread
  that only runs and waits for sync queries was not affected; that was
  tried too.
- **Why.** The callers that join a run share one `concurrent.futures`
  future. A task waited for it through `asyncio.wrap_future`, which hands a
  cancellation of the wait on to the future it wraps.
- **How often.** On every try, on Python 3.12.9. It does not depend on
  timing: cancelling the wait cancels the shared future at once.
- **The fix.** A task now waits on a future of its own, which the shared one
  settles when the run ends (`_wait_for_joined_run`). A task that has
  stopped waiting by then, or whose loop is closed, is left alone.
  `asyncio.shield` around the old wait was tried first. It cures the
  failures, but a cancelled waiter whose run later fails then leaves a
  "Future exception was never retrieved" for asyncio to log.
- **Tests.** Two in `tests/test_known_defects.py`: `test_d34_cancelling_a_waiting_task_leaves_the_owner_and_other_waiters_alone`
  (three tasks; it failed before the fix) and `test_d34_sync_query_keeps_its_value_when_a_task_waiting_for_its_run_is_cancelled`
  (the thread, with the cancelled task's loop closed before the run ends).
  Five ways of breaking the fix were applied to a copy: the old wait, the
  shielded wait, settling a future whose task has stopped waiting, not
  coping with a closed loop, and mixing up result and error. Each fails one
  of those two tests or the three-task test above, which also checks that
  nothing is left for asyncio to report.

### Defect found on the way: D35 (fixed)

Found by CI. The push of `a48c35d` on 2026-11-24 was the first run of the
suite on GitHub since this work began. Its free-threaded job (Python 3.14t)
failed
`test_fixed_point_dropped_members.py::test_threads_reading_a_dependent_of_a_dropped_member`:
one of six threads got `CycleError` where 50 was due.

- **Not from the two fixes above.** On a free-threaded 3.14.4 the test
  failed in 36 of 400 runs on the source before them and in 40 of 400
  after; on 3.12, in 0 of 400. The same job had passed it on the push
  before, by chance.
- **What.** The member of a fixed-point cycle has lost its result
  (eviction, `prune`, `sweep_unaccessed`) and an input has changed.
  Several threads ask for the member, or for a query that depends on it.
  One of them gets `CycleError` for a cycle that converges. G13 says
  dropping a result never changes what a later call returns.
- **Why.** A member's run that reaches itself through its head starts over
  by bringing the head up to date first (D25). The head's run asks for the
  member. If a second caller has taken over the member by then, the head
  waits for that run, which fails as the first caller's own did: it
  reaches itself. The head was run the way a verification runs a
  dependency, and a verification gives up on a joined run that ended in
  `CycleError`, because the query being verified then runs for real. Here
  nothing runs for real afterwards. The head stayed as it was, and the
  member ran once more, reached itself again and raised.
- **How often.** About one run in ten with six threads on the free-threaded
  build; not once in 400 runs under the interpreter lock. With three holds
  in the query bodies: every time on 3.12, with threads and with tasks.
- **The fix.** `_enter_cycle_through_head` brings the head up to date with
  `as_verification=False`. A joined run that ended in `CycleError` is
  retried there, so the head runs the member on its own stack, where the
  cycle converges.
- **Tests.** `tests/test_known_defects.py::test_d35_two_threads_asking_for_a_dropped_cycle_member_both_get_its_value`
  and `::test_d35_two_tasks_asking_for_a_dropped_cycle_member_both_get_its_value`.
  The fix passes through three places for threads and three for tasks; it
  was undone at each on a copy, and each time the matching test fails.
- **After the fix.** The test that failed in CI passed 1,500 of 1,500 runs
  on the free-threaded build, and the whole suite passes there (1,967
  passed, 10 skipped).
- **A slip on the way, and what it showed.** The first version of the fix
  named its new parameter `verifying`, and a local variable of that name
  overwrote it: no verification gave up on a joined `CycleError` any more.
  Every test passed. It was noticed because undoing the fix by hand did
  not make the new test fail. So nothing in the suite pins that a
  verification does not retry. Tried directly: with `_retry_after` always
  true, the cycle, fixed-point, defect, concurrency and engine test files
  pass. That was a gap. Two tests in `test_async_parity.py` pin the rule
  since (`test_retry_after_verification_does_not_retry_joined_cycle_error_sync`
  and its async twin): a verification joins a run of a dependency that
  ends in `CycleError`, and the dependency must not run again. With
  `_retry_after` always true, on a copy, both fail (the dependency runs
  four times instead of three); on the real source they pass.

### The first push: what CI showed

The run for `a48c35d` on 2026-11-24.

- **`tla_check` passed.**
- **Both test jobs fail the coverage gate**: 94.24% on 3.12 and 94.23% on
  3.14t against the required 95%. The push before (`59c3299`) had failed it
  too, at 94.13%, so nothing has been published since `v0.3.28`. `numpy` is
  not in the `dev` extra, so the four numpy tests are skipped in CI and
  about 30 statements of `_canonical.py` and 8 of `_serde.py` are never
  run. With numpy installed the same command gives 95.01% on Windows.
- **The 3.14t job also failed the test behind D35**, above. The 3.12 job
  passed all 1,965 tests it ran.
- **Races show on the free-threaded build, slowly.** D35 failed one run in
  ten there, so one run per push finds such a defect one push in ten. A
  free-threaded interpreter is installed on the machine this was written
  on; running the thread tests a few hundred times there takes minutes.

### The coverage gate

CI requires 95% of statements and branches together, and 90% of branches.
Measured on Windows with CI's command (the performance tests left out), in
statements and branch exits never run, of 8,469:

| | Never run | Total | Branches |
|---|---:|---:|---:|
| Without the numpy tests, as CI ran | 479 | 94.34% | 91.03% |
| With the numpy tests | 420 | 95.04% | 91.96% |
| With the tests below as well | 344 | 95.94% | 93.46% |

The gate allows 423. CI's own figure, 94.24%, is 488 never run: 9 more than
the first row, which is code for the other platform (file locking in
`_disk_cache.py` has a branch for each). So the estimate for CI is about
353 never run, 95.8%. It has not run on GitHub yet.

- **`numpy` is in the `dev` extra now**, so CI runs the four numpy tests.
  numpy 2.5.5 has wheels for both interpreters CI uses (3.12 and the
  free-threaded 3.14). It stays out of the package's own dependencies. The
  numpy tests have not run on a free-threaded build here: numpy is not
  installed for the one on this machine.
- **Twenty-three tests for behaviour nothing ran**, in the files that cover
  each area:
  - `test_argument_keys.py`: a default that cannot be part of a key, passed
    explicitly by position, keyword or keyword-only, is the call without
    it; a missing keyword-only argument is refused before the body; such a
    default before `*rest`; positional-only parameters with such defaults;
    a callable whose signature cannot be inspected takes positional
    arguments only.
  - `test_unencodable_values.py`: the path to the part that cannot be
    encoded, inside a deque, a set and a frozenset (on both backends).
  - `test_subgraph_stats_export.py`: `condense=True` in `inspect_graph`,
    `subgraph`, `export_dot` and `export_mermaid`; single calls and names
    without arguments are left alone; edges with one end are skipped; a
    dependency shared by two queries is followed once, in both directions.
  - `test_query_methods.py`: handles read from an instance compare and hash
    by instance and method.
  - `test_query_identity.py`: the same code with another default object, in
    other globals, or with a captured variable assigned after the function,
    is a different function under the same name.
  - `test_history_retention.py`: a snapshot's revision cannot be changed,
    and snapshots are equal when their revisions are.
- **They assert behaviour.** Seventeen of the lines they newly reach were
  broken one at a time on a copy; a test fails for each.
- **Still never run**, among others: the other platform's file locking, the
  fallback in the handles for an interpreter without
  `asyncio._get_running_loop`, and error paths of the disk cache.

- **Follow-up (`921329f`, `v0.3.29`):** Additional tests and unreachable-guard
  pragmas brought Windows coverage to **100%** (6,145 statements, 2,218
  branches, 2,142 tests passing). CI passed on both Python 3.12 and
  free-threaded Python 3.14t (**99.77%** branch coverage on Linux; the next
  point says what it missed) and published `v0.3.29` to PyPI.
- **What CI still missed, and why.** For `08e08f8` CI reported 99.89% on
  3.12 and 99.84% on 3.14t. The lines were not the file locking, which is
  excluded on both platforms. They were the exit hook in `_evaluator.py`:
  `_flush_all_at_exit` and the wait loop of `flush_at_exit`. Only the child
  processes of `test_durability_at_exit.py` ran them. pytest-cov 6, which
  is installed on this machine, follows child processes; pytest-cov 7,
  which CI installs, does not. Two tests now call the hook in the test
  process: one holds a write and checks that the hook waits for it and that
  the result is on disk afterwards, one stops the writer and checks that
  the flush gives up. Run alone under pytest-cov 7 they execute every one
  of those lines.
- **Two spots were covered by chance.** A run of the committed tree on this
  machine missed one branch of `_commit_green` (a result dropped while it
  is being verified, which took another thread dropping it at that moment)
  and one line of the ordered reducers (an element that starts to pass the
  filter, which only random examples produced). CI had happened to run
  both. Each has a fixed test now:
  `test_dirty_propagation.py::test_result_dropped_while_it_is_being_verified_is_returned_but_not_kept`
  and
  `test_rewrite_reducers_exact.py::test_ordered_reducers_take_in_an_element_that_starts_to_pass_the_filter`.
- **Measured five more times with those four tests.** Three runs on 3.12:
  100% each, 6,145 statements and 2,218 branches. Two runs on the
  free-threaded 3.14 with pytest-cov 7: everything but the numpy code, and
  numpy is not installed for that interpreter here. Not yet confirmed on
  GitHub.
- **The 40 lines marked as not coverable in `921329f` were read** (26
  `no cover`, 14 `no branch`). Each has a reason, or sits next to one. One
  was tested rather than believed: the trimming block of
  `_append_version_locked`, which three pins released one at a time did
  not reach. `Evaluator._note_write_failure` was marked as an unused helper
  and had no caller; it has been deleted.
- **Async coverage and 3.14t soak added (`1b2e596`).** Function-level
  `# pragma: no cover` exclusions were removed from all 16 async evaluator
  methods in `src/cascade/_evaluator.py`, and 19 event-loop-awaited async
  parity tests were added in `tests/test_async_parity.py` (accumulator
  replay/rollback/waiters, early bail-out & `memoize=False`,
  `max_entries`/`prune`/`sweep_unaccessed` with async fixed points, async
  snapshots under concurrent writes, mixed sync↔async fan-out, sync+async
  `_retry_after` verification rules, and `compute_or_get_memo_async`
  condition/operator branches). Multi-writer collection concurrency stress tests
  (`tests/test_collection_concurrency.py`) and an on-demand 3.14t soak workflow
  (`.github/workflows/concurrency-soak.yml`) were added. On 3.12 the full suite
  reaches 99.35% total coverage (99.53% branch coverage); on 3.14t with
  `PYTHON_GIL=0`, the full suite passes, 50/50 local soak rounds pass, and the
  50-round GitHub Actions soak (`45546504695`) passed in 5m1s.
- **Second repeat over `cascade._evaluator` (`1b2e596`).** Run locally with
  `python scripts/mutation.py run "cascade._evaluator.*"` on 28 processes
  (50.0 minutes) against 2,176 tests (0 untested mutants across all 11,619
  mutants):

  | Run | Mutants | Killed | Survived | Timeout | Other | Survival rate |
  |---|---:|---:|---:|---:|---:|---:|
  | Baseline | 3,316 | 2,411 | 888 | 16 | 1 | 26.8% |
  | First repeat | 3,316 | 2,469 | 828 | 18 | 1 | 25.0% |
  | **Second repeat (`1b2e596`)** | **3,463** | **2,671** | **762** | **28** | **2** | **22.0%** |

  Removing `# pragma: no cover` on the async methods added 147 newly mutated
  sites, yet total kills rose by +202 and survivors fell by -66 (-3.0 pp).
  `compute_or_get_memo_async` dropped from 78 to 49 survivors (vs 37 in sync
  `compute_or_get_memo`), `_verify_disk_deps` and `_verify_disk_deps_async`
  are at parity (13 each), and `_retry_after` (2/2), `_wait_for_joined_run`
  (8/8), and `_release_in_flight` (10/10) have 0 survivors.

## Next steps

In order of value for the time they take.

1. ~~**Re-run `.github/workflows/mutation.yml` (`shards=small`) once more.**~~
   Done (`45546501531`, passed in 11m24s with `--only small`).
2. ~~**Repeat the mutation run over `cascade._evaluator`.**~~ Done above
   (`22.0%` survival rate across 3,463 mutants).
3. **Optionally pin the last few behavioral survivors in
   `compute_or_get_memo_async`.** Most of the remaining 49 survivors are
   `trace_event` arguments, pin/unpin calls, or the `_finished_meanwhile_locked`
   race window; the main behavioral ones left are provisional-hit and
   `cache_green` effect replay (`return pending, True` / `return existing, True`)
   and `_refuse_reentry` under async shadow verification.
4. **Decide whether mutation runs fix the Hypothesis seed.** With random
   examples a few verdicts change from run to run; a fixed seed makes the
   rate comparable over time, at the price of always trying the same
   examples. It does nothing for the verdicts that depend on threads.
5. Leave the rest. Driving the overall rate down is not worth it: about a
   fifth of the survivors are strings, and many of the `None` arguments are
   tracing.
