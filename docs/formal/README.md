# Formal model: lazy re-verification with concurrent callers

`CascadeCore.tla` is a TLA+ model of how the engine brings cached results up
to date, checked with TLC. It is a model of the algorithm, not of the
Python: each action is one critical section of the store lock, and TLC
explores every order in which callers, writers and evictions can take them.

The graph is the smallest one that has every mechanism in it:

```
choose = left if mode == "left" else right     # a dynamic dependency
top    = 0 if choose == 0 else 1               # a query above a query
```

- **Writers** change one input and nothing else. A cached result is not
  touched by a write, apart from losing its `head_clean` flag.
- **Callers** (two of them) each call `choose` or `top`, at the newest
  revision (`ReadLive`) or at any older one, as through a snapshot
  (`ReadSnapshot`). A call is a sequence of steps, and anything can happen
  between two of them.
- **Evictions** drop a cached row at any moment.

## Files

- `CascadeCore.tla`: the model and its invariants.
- `CascadeCore.cfg`: two callers, snapshots, evictions, two writes. Minutes
  of computation, so it is run on demand: locally (below), or by starting
  `.github/workflows/model-check.yml` by hand. Run it after changing the
  model or the evaluator's verification path.
- `CascadeMinimal.cfg`: one caller at the newest revision, five writes,
  three values; adds the minimal re-execution property (G2). Seconds; CI
  runs it on every push.

## From actions to code

| Action | Code | What it does |
|---|---|---|
| `WriteMode`, `WriteData` | `GraphStore.set_input`, `mark_dependents_dirty_locked` | New value from the next revision on; rows that read the input turn dirty, the walk stopping at a row already dirty. A same-value write is not a step (G3). |
| `ReadLive`, `ReadSnapshot` | `GraphStore.pin_snapshot` | Fixes the revision the call reads at. |
| `CLookup`, `TLookup` | `Evaluator.compute_or_get_memo`, first locked block | A row that is current (`is_current_locked`) is a hit. Otherwise the caller keeps the row it saw and verifies it without the lock. |
| `CGreen`, `TGreen` | `try_mark_green`, `_commit_green`, `refresh_head_clean_locked` | Every recorded dependency still has its marker: the row is the answer. If it is still the stored row it is marked verified, and clean at the newest revision. |
| `CClaim`, `TClaim` | `_finished_meanwhile_locked`, `_claim_in_flight_locked` | Use a result stored for this revision in the meantime, join the caller computing one, or claim the slot. |
| `CRun`, `TStore` | `recompute`, `_finish_recompute`, `_try_hydrate_from_disk` | Run the body and store the row; an equal value keeps the old change stamp. `choose` may instead load its persisted record, if the values of its dependencies match. Replacing a row dirties its dependents. |
| `CRelease`, `TRelease`, `CWait`, `TWait` | `_release_in_flight` | Give up the slot and hand the entry to the callers that joined. |
| `EvictChoose`, `EvictTop` | `GraphStore.drop_memo_locked` | Eviction, `prune`, `sweep_unaccessed`. |

Verifying `top` and running its body both make a nested call of `choose` at
the caller's revision, which goes through the same steps. `top` then
compares the entry that call returned, not the row that is stored by then:
another caller, at another revision, may have replaced it in between.

## What TLC checks

| Invariant | Guarantee | Statement |
|---|---|---|
| `ReadsMatchFreshRun` | G1, G4 | A call returns what a fresh run on the inputs at its revision returns. |
| `RowsAreSound` | G1, G13 | A stored row holds the value for the revision it was verified at; a clean row holds the value for the newest revision. |
| `RecordedDependenciesHold` | G1 | A row's recorded dependencies are the ones a run at its verified revision reads, with their markers. |
| `StampsNameValues` | G1, G2 | Everyone holding a change stamp of `choose` agrees on the value it stands for, so comparing stamps is comparing values. |
| `CleanRowsRestOnCleanRows` | G2 | A clean row sits on clean rows, so a write beneath it reaches it (invariant 1 of dirty propagation). |
| `OneRunPerRevision` | G5 | One caller at a time runs a query at a revision; a waiting caller always has an owner to wait for. |
| `MarkersBoundedByRevision` | | Markers and verified revisions never run ahead of the newest revision. |
| `TopRunsOnlyAfterChooseMoved` | G2 | `top` runs again only if `choose` has had another value since. `CascadeMinimal.cfg` only. |
| `Monotone` | G6 | The revision and the cancellation epoch never go back. |

Loading the persisted record of `choose` is covered by the first two (G15,
G16): a stale record is never served.

`CascadeCore.cfg` is exhaustive for two callers and two writes: 26 million
states, four and a half minutes with four workers. `CascadeMinimal.cfg` has
4 million and takes seconds. Neither bounds the number of calls, evictions
or re-runs.

The invariants were tested by breaking the model: thirteen bugs seeded one
at a time (a write that does not dirty `top`, a re-run that keeps its stamp
whatever the value, a record loaded without comparing its dependencies, no
in-flight slot, `top` verified against the stored row instead of the entry
its nested call returned, and so on) each violate one of them. The last of
those needs a trace of 23 steps with both callers at different revisions.

## What the model leaves out

- **Change stamps are reused.** The code counts them up and only compares
  them for equality. The model hands out the smallest stamp nobody holds
  any more, which gives the same behaviours in a finite state space.
- **The dependency walk of `choose` is one step.** It reads input history at
  the caller's revision, which nothing can change any more, so its outcome
  is fixed when the row is looked up. Committing it is a separate step.
- **`top` has no change stamp**, because nothing reads `top`. Its early
  bail-out would not be observable.
- **Snapshots are not objects.** Any revision up to the newest can be read
  at; input history is never trimmed, so `SnapshotExpiredError` and
  history retention are not modelled.
- Not modelled at all: cancellation of running work, errors, TTL,
  re-registered code, fixed points, accumulator effects, transactions,
  collections and their compaction, `memoize=False`, a persisted record of
  `top`, and the engine restart itself (a record is loaded after an
  eviction, in the same process). The Hypothesis state machine in
  `tests/test_stateful_engine_invariants.py` covers most of these against
  the real engine.

## Running TLC

With Java 17 or later, using the `tla2tools.jar` at the root of the
repository:

```bash
cd docs/formal
java -cp ../../tla2tools.jar tlc2.TLC -workers auto -deadlock CascadeCore.tla -config CascadeMinimal.cfg
java -cp ../../tla2tools.jar tlc2.TLC -workers auto -deadlock CascadeCore.tla -config CascadeCore.cfg
```

`-deadlock` turns the deadlock check off; `OneRunPerRevision` states what
matters there. TLC writes its state queue to `states/` and, when it finds
a violation, a trace as `*_TTrace_*`. Both are in `.gitignore`; they were
committed once, by accident.
